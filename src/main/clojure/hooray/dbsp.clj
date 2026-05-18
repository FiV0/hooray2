(ns hooray.dbsp
  "Standard DBSP incremental engine.

  Compiles a conjunctive Datalog query of standard triple patterns into a
  circuit of unary/binary operators (see `org.hooray.dbsp`), modelled on the
  Feldera `dbsp` crate. This is the `:standard` engine; the `:wcoj` engine in
  `hooray.incremental` is unaffected.

  See specs/dbsp-standard.md."
  (:require
   [clojure.set :as set]
   [clojure.spec.alpha :as s]
   [hooray.query :as query]))

;; --------------------------------------------------------------------------
;; Phase 1 — pattern descriptors
;; --------------------------------------------------------------------------

(defn- elem
  "Normalises a conformed pattern element into `{:kind :constant :value v}` or
  `{:kind :variable :var s}`."
  [[etype evalue]]
  (case etype
    :constant {:kind :constant :value evalue}
    :variable {:kind :variable :var evalue}))

(defn- elem-var
  "The variable bound by [el], or nil if it is a constant."
  [el]
  (when (= :variable (:kind el)) (:var el)))

(defn compile-pattern
  "Compiles one conformed `where` clause into a triple-pattern descriptor:

    {:index   <position in :where>
     :attr    <constant attribute>
     :e       {:kind :constant :value v} | {:kind :variable :var s}
     :v       {:kind :constant :value v} | {:kind :variable :var s}
     :vars    [vars in [e v] column order]}

  The DBSP-standard engine only supports triple patterns with a constant
  attribute."
  [index [clause-type pattern]]
  (when-not (= :triple clause-type)
    (throw (ex-info "DBSP-standard engine supports only triple patterns"
                    {:clause-type clause-type :pattern pattern})))
  (let [{:keys [e a v]} pattern
        a* (elem a)
        e* (elem e)
        v* (elem v)]
    (when-not (= :constant (:kind a*))
      (throw (ex-info "DBSP-standard engine requires a constant attribute"
                      {:pattern pattern})))
    {:index index
     :attr (:value a*)
     :e e*
     :v v*
     :vars (vec (keep elem-var [e* v*]))}))

(defn compile-patterns
  "Compiles every clause of a conformed `:where` into a vector of descriptors,
  indexed by original position."
  [conformed-where]
  (vec (map-indexed compile-pattern conformed-where)))

;; --------------------------------------------------------------------------
;; Phase 1 — deterministic left-deep join order
;; --------------------------------------------------------------------------

(defn left-deep-order
  "Orders [patterns] (a vector of descriptors) into a left-deep join sequence.

  Starts from the first pattern and repeatedly appends the remaining pattern
  that shares the most variables with the patterns chosen so far; ties — and
  fully disconnected patterns, which become Cartesian joins — are broken by
  lowest original index. Deterministic: the same input always yields the same
  order."
  [patterns]
  (when (seq patterns)
    (loop [chosen [(first patterns)]
           chosen-vars (set (:vars (first patterns)))
           remaining (vec (rest patterns))]
      (if (empty? remaining)
        chosen
        (let [best (->> remaining
                        (sort-by (fn [p]
                                   [(- (count (set/intersection
                                               chosen-vars (set (:vars p)))))
                                    (:index p)]))
                        first)]
          (recur (conj chosen best)
                 (set/union chosen-vars (set (:vars best)))
                 (vec (remove #(= (:index %) (:index best)) remaining))))))))

(defn parse
  "Conforms a raw query and returns `{:find <conformed find> :patterns
  <descriptors>}`. Throws on unsupported clauses."
  [query]
  (let [conformed (s/conform ::query/query query)]
    (when (= ::s/invalid conformed)
      (throw (ex-info "Invalid query" {:query query})))
    {:find (:find conformed)
     :patterns (compile-patterns (:where conformed))}))
