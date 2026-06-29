(ns hooray.plan
  (:require [clojure.set :as set]
            [clojure.core.match :refer [match]]
            [hooray.error :as err])
  (:import (org.hooray.algo GenericJoin)
           (org.hooray.iterator GenericNotPrefixExtender)))

;; some helpers used in plan and query

(declare variable-order*)

(defn variable-order [[type value]]
  (-> (case type
        :triple (let [{:keys [e a v]} value]
                  (match [e a v]
                    [[:constant _] [:constant _] [:constant _]] []
                    [[:constant _] [:constant _] [:variable value-var]] [value-var]
                    [[:variable entity-var] [:constant _] [:constant _]] [entity-var]
                    [[:variable entity-var] [:constant _] [:variable value-var]]
                    (if (= entity-var value-var)
                      (err/unsupported-ex "Hooray does not (yet) support repeated variables inside one triple pattern"
                                          {:pattern value})
                      [entity-var value-var])
                    [_ [:variable _] _] (err/unsupported-ex "Currently variables in attribute position are not supported")))

        (:or :and :not) (variable-order* value)
        :predicate (let [{:keys [args]} value]
                     (->> args
                          (filter (fn [[arg-type _arg-value]] (= arg-type :variable)))
                          (map second)))
        :fn (let [[{:keys [args]} ret-var] value]
              (-> (->> args
                       (filter (fn [[arg-type _arg-value]] (= arg-type :variable)))
                       (map second))
                  (conj ret-var))))
      distinct))

(defn variable-order* [patterns]
  (->> (map variable-order patterns)
       flatten
       distinct))

(defn free-variables [pattern]
  (set (variable-order pattern)))

(defn in->variables [in]
  (-> (for [[type var] in]
        (case type
          :scale-binding [var]
          :collection-binding [var]
          :tuple-binding var
          :relation-binding var))
      flatten))

(defn- in-binding-variables [[type var]]
  (case type
    :scale-binding #{var}
    :collection-binding #{var}
    :tuple-binding (set var)
    :relation-binding (set (first var))))

;; To avoid branch leaking in or-branches we use the following strategy:
;; We fix some variable order over all `where` clauses.
;; Whenever there are no `or` patterns participating in the variable level
;; we do standard generic join across the levels where no `or` patterns
;; appear. Whenever an or-pattern appears we fork, meaning we continue
;; running generic join, but now on the number of branches the `or` pattern
;; has. We include all variables levels between the first variable level of the
;; `or` pattern and the last variable pattern.
;;
;; Example:
;; variable order [a, b, c, d, e]
;; OR b,...,d
;;
;; Then the OR branches are forked for variable levels b,c and d, even if c is not
;; participating in the OR branches.
;; What happens if multiple `or` patterns overlap in their appearing variables?
;; We make them a connected fork component and take the cartesian product of
;; the different branches. This might be not be very performant in the beginning
;; but I think it's a good first start.
;;
;; OR1: b, ..., d with branches X, Y
;; OR2: b, ......, e with branches Z, M
;;
;; fork covering b,c,d,e with four forks
;; covering XZ, XM, YZ, YM

;; We implement this algorithm in two phases.
;; 1. Transform the query into a logical plan tree. This removes query syntax and
;;    compiles leaf patterns to prefix extenders, but keeps boolean shape explicit.
;;
;; {:type :extender
;;  :variables ...
;;  :extenders [<PrefixExtender> ...]}
;;
;; {:type :and
;;  :variables ...
;;  :children [<logical-plan-item> ...]}
;;
;; {:type :or
;;  :variables ...
;;  :levels ...
;;  :branches [[<logical-plan-item> ...] ...]}
;;
;; {:type :not
;;  :variables ...
;;  :levels ...
;;  :children [<logical-plan-item> ...]}
;;
;; 2. Lower the logical plan tree into executable plan items. At this boundary
;;    all boolean nesting has been normalized, `not` has become ordinary
;;    GenericNotPrefixExtenders, and the only remaining item types are
;;    executable `:extender` and positive `:or` items.


(defn- in->items [in in-extenders]
  (mapv (fn [binding extender]
          {:type :extender
           :variables (in-binding-variables binding)
           :extenders [extender]})
        in
        in-extenders))

(defn- branch-patterns [branch]
  (if (= :and (first branch))
    (second branch)
    [branch]))

(defn- cartesian-product [colls]
  (if (empty? colls)
    [[]]
    (for [x (first colls)
          more (cartesian-product (rest colls))]
      (cons x more))))

(declare compile-plan-item)

(defn- compile-plan-item [compile-pattern var-in-join-order [type pattern :as conformed-pattern]]
  (let [var->idx (zipmap var-in-join-order (range))
        variables (free-variables conformed-pattern)]
    (case type
      (:triple :fn :predicate) {:type :extender
                                :variables variables
                                :extenders [(compile-pattern conformed-pattern)]}
      :and {:type :and
            :variables variables
            :children (mapv (partial compile-plan-item compile-pattern var-in-join-order) pattern)}
      :or {:type :or
           :variables variables
           :levels (sort (mapv var->idx variables))
           :branches (mapv (fn [branch]
                              (mapv (partial compile-plan-item compile-pattern var-in-join-order)
                                    (branch-patterns branch)))
                            pattern)}

      :not {:type :not
            :variables variables
            :levels (sort (mapv var->idx variables))
            :children (mapv (partial compile-plan-item compile-pattern var-in-join-order) pattern)})))

(defn- compile-items [{:keys [compile-pattern var-in-join-order in in-extenders where]}]
  (concat (in->items in in-extenders)
          (map (partial compile-plan-item compile-pattern var-in-join-order) where)))

(declare logical-items->alternatives)

(defn- logical-item->alternatives [{:keys [type children branches extenders levels] :as item}]
  (case type
    :extender [extenders]
    :and (logical-items->alternatives children)
    :or (vec (mapcat logical-items->alternatives branches))
    :not [(mapv (fn [alternative]
                  (GenericNotPrefixExtender. alternative (apply max levels)))
                (logical-items->alternatives children))]
    (throw (ex-info "Unknown logical plan item" {:item item}))))

(defn- logical-items->alternatives [items]
  (mapv (fn [alternative-set]
          (vec (mapcat identity alternative-set)))
        (cartesian-product (map logical-item->alternatives items))))

(defn- logical-item->executable-items [{:keys [type variables levels branches extenders children] :as item}]
  (case type
    :extender (mapv (fn [extender]
                      {:type :extender
                       :variables variables
                       :extender extender})
                    extenders)
    :and (mapcat logical-item->executable-items children)
    :or [{:type :or
          :variables variables
          :levels levels
          :branches (vec (mapcat logical-items->alternatives branches))}]
    :not (mapv (fn [extender]
                 {:type :extender
                  :variables variables
                  :extender extender})
               (first (logical-item->alternatives item)))
    (throw (ex-info "Unknown logical plan item" {:item item}))))

(defn- logical-items->executable-items [items]
  (vec (mapcat logical-item->executable-items items)))

(defn- or-item? [item]
  (= :or (:type item)))

(defn- extender-item? [item]
  (= :extender (:type item)))

(defn- merge-component [component or-item]
  (-> component
      (update :ors conj or-item)
      (update :variables set/union (:variables or-item))
      (update :levels set/union (set (:levels or-item)))))

(defn- merge-components [component other-component]
  (-> component
      (update :ors into (:ors other-component))
      (update :variables set/union (:variables other-component))
      (update :levels set/union (:levels other-component))))

(defn- connected? [component or-item]
  (seq (set/intersection (:variables component) (:variables or-item))))

(defn- add-to-components [components or-item]
  (let [[connected-components disjoint-components] ((juxt filter remove) #(connected? % or-item) components)
        merged-component (-> (reduce merge-components
                                 {:ors []
                                  :variables #{}
                                  :levels #{}}
                                 connected-components)
                             (merge-component or-item))]
    (conj (vec disjoint-components) merged-component)))

(defn- component-span [component]
  (let [levels (:levels component)]
    [(apply min levels) (apply max levels)]))

(defn- components [items]
  (->> items
       (filter or-item?)
       (reduce add-to-components [])
       (map (fn [component]
              (let [[start end] (component-span component)]
                (assoc component
                       :start start
                       :end end))))
       (sort-by :start)
       vec))

(defn- join-phase [start end extenders]
  {:type :join
   :start start
   :end end
   :levels (mapv int (range start (inc end)))
   :extenders (vec extenders)})

(defn- fork-phase [component normal-extenders]
  (let [branch-products (cartesian-product (map :branches (:ors component)))]
    {:type :fork
     :start (:start component)
     :end (:end component)
     :variables (:variables component)
     :children (mapv (fn [branch-product]
                       {:phases [(join-phase (:start component)
                                             (:end component)
                                             (concat normal-extenders
                                                     (mapcat identity branch-product)))]})
                     branch-products)}))

(defn plan-items [items levels]
  (let [normal-extenders (mapv :extender (filter extender-item? items))
        components (components items)]
    (loop [phases []
           next-level 0
           [component & remaining-components] components]
      (cond
        (>= next-level levels)
        phases

        (nil? component)
        (conj phases (join-phase next-level (dec levels) normal-extenders))

        (< next-level (:start component))
        (recur (conj phases (join-phase next-level (dec (:start component)) normal-extenders))
               (:start component)
               (cons component remaining-components))

        :else
        (recur (conj phases (fork-phase component normal-extenders))
               (inc (:end component))
               remaining-components)))))

(defn generic-plan [{:keys [var-in-join-order] :as opts}]
  (plan-items (logical-items->executable-items (compile-items opts))
              (count var-in-join-order)))

;; this looks good

(defn- execute-join [phase prefixes]
  (if (empty? (:extenders phase))
    prefixes
    (.join (GenericJoin. (:extenders phase) ^java.util.List (:levels phase)) prefixes)))

(declare execute)

(defn- execute-fork [phase prefixes]
  (->> (:children phase)
       (mapcat (fn [child]
                 (execute (:phases child) prefixes)))
       distinct
       vec))

(defn execute
  ([phases] (execute phases [[]]))
  ([phases prefixes]
   (reduce (fn [prefixes phase]
             (case (:type phase)
               :join (execute-join phase prefixes)
               :fork (execute-fork phase prefixes)))
           prefixes
           phases)))
