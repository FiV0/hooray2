(ns hooray.dbsp-test
  (:require
   [clojure.test :as t :refer [deftest is testing]]
   [hooray.core :as h]
   [hooray.dbsp :as dbsp]
   [hooray.fixtures :as fix])
  (:import
   (org.hooray.dbsp Tuple)))

(def ^:private opts {:type :mem :storage :hash :algo :generic})

(def ^:private schema
  [{:db/id -1
    :db/ident :name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/id -2
    :db/ident :last-name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/id -3
    :db/ident :city
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/id -4
    :db/ident :edge
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}
   {:db/id -5
    :db/ident :age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/id -6
    :db/ident :salary
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}])

(defn- fresh-node
  "A connected node with the test schema already transacted."
  []
  (doto (h/connect opts)
    (h/transact schema)))

(defn- with-standard-dbsp [f]
  (binding [h/*dbsp-version* :standard]
    (f)))

(defn- with-dbsp-schema [f]
  (h/transact fix/*node* schema)
  (f))

(t/use-fixtures :each with-standard-dbsp fix/with-node with-dbsp-schema)

(defn- patterns [query]
  (:patterns (dbsp/parse query)))

(defn- order-indices [query]
  (mapv :index (dbsp/left-deep-order (patterns query))))

;; --------------------------------------------------------------------------
;; Pattern descriptors
;; --------------------------------------------------------------------------

(deftest compile-pattern-test
  (testing "two variables"
    (let [[p] (patterns '{:find [name] :where [[?e :name name]]})]
      (is (= :triple (:kind p)))
      (is (= {:kind :constant, :value :name} (:attr p)))
      (is (= {:kind :variable :var '?e} (:entity p)))
      (is (= {:kind :variable :var 'name} (:value p)))
      (is (= '[?e name] (:vars p)))
      (is (= '[?e name] (:groundable p)))))

  (testing "constant value"
    (let [[p] (patterns '{:find [?e] :where [[?e :name "Ivan"]]})]
      (is (= {:kind :constant :value "Ivan"} (:value p)))
      (is (= '[?e] (:vars p)))
      (is (= '[?e] (:groundable p)))))

  (testing "constant entity"
    (let [[p] (patterns '{:find [name] :where [[1 :name name]]})]
      (is (= {:kind :constant :value 1} (:entity p)))
      (is (= '[name] (:vars p)))
      (is (= '[name] (:groundable p)))))

  (testing "indices follow :where position"
    (is (= [0 1 2]
           (mapv :index (patterns '{:find [name]
                                    :where [[?e :name name]
                                            [?e :age age]
                                            [?e :last-name ln]]})))))

  (testing "predicate descriptor records predicate args and free variables"
    (let [[_ p] (patterns '{:find [name]
                            :where [[?e :name name]
                                    [(re-find #"A" name)]]})]
      (is (= :predicate (:kind p)))
      (is (= 're-find (:predicate p)))
      (is (= [{:kind :constant :value "A"}
              {:kind :variable :var 'name}]
             (update-in (:args p) [0 :value] str)))
      (is (= '[name] (:vars p)))
      (is (= [] (:groundable p))))))

(deftest compile-pattern-fn-test
  (testing "fn descriptors normalize args and ground their result variables"
    (let [[_ _ unary-p binary-p]
          (patterns '{:find [half c]
                      :where [[?e :age a]
                              [?e :salary b]
                              [(quot a 2) half]
                              [(+ a b) c]]})]
      (is (= :fn (:kind unary-p)))
      (is (= 'quot (:fn unary-p)))
      (is (= [{:kind :variable :var 'a}
              {:kind :constant :value 2}]
             (:args unary-p)))
      (is (= 'half (:ret-var unary-p)))
      (is (= '[a half] (:vars unary-p)))
      (is (= '[half] (:groundable unary-p)))
      (is (= :fn (:kind binary-p)))
      (is (= '+ (:fn binary-p)))
      (is (= '[a b c] (:vars binary-p)))
      (is (= '[c] (:groundable binary-p)))))

  (testing "a self-referential fn clause cannot ground its own result variable"
    (let [[_ p] (patterns '{:find [age]
                            :where [[?e :age age]
                                    [(inc age) age]]})]
      (is (= '[age] (:vars p)))
      (is (= [] (:groundable p))))))

(deftest rejects-unsupported-clauses-test
  (testing "fn clauses with more than two arguments are rejected"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"unary and binary"
                          (dbsp/parse '{:find [x]
                                        :where [[?e :age a]
                                                [(+ a 1 2) x]]}))))
  (testing "unsupported fn names still fail query conformance"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid query"
                          (dbsp/parse '{:find [x]
                                        :where [[?e :age age]
                                                [(* age 2) x]]}))))
  (testing "repeated variables inside one triple pattern are rejected"
    (is (thrown? clojure.lang.ExceptionInfo
                 (dbsp/parse '{:find [?x] :where [[?x :edge ?x]]})))))

(deftest compile-pattern-or-test
  (testing "flat single-variable or"
    (let [[p] (patterns '{:find [?e]
                          :where [(or [?e :sex :male]
                                      [?e :sex :female])]})]
      (is (= :or (:kind p)))
      (is (= '[?e] (:vars p)))
      (is (= '[?e] (:groundable p)))
      (is (= 2 (count (:branches p))))
      (is (every? #(= :triple (:kind %)) (:branches p)))))

  (testing "flat multi-variable or — :vars in encounter order of first branch"
    (let [[p] (patterns '{:find [?p n]
                          :where [(or [?p :name n]
                                      [?p :age n])]})]
      (is (= :or (:kind p)))
      (is (= '[?p n] (:vars p)))))

  (testing "multi-variable or where branches use different encounter orders"
    ;; first branch's encounter order is [?p v]; that becomes the :or's :vars
    (let [[p] (patterns '{:find [?p v]
                          :where [(or [?p :name v]
                                      [v :age ?p])]})]
      (is (= :or (:kind p)))
      (is (= '[?p v] (:vars p)))))

  (testing "nested or is preserved (not flattened)"
    (let [[p] (patterns '{:find [?e]
                          :where [(or [?e :name "Ada"]
                                      (or [?e :name "Bob"]
                                          [?e :name "Carla"]))]})]
      (is (= :or (:kind p)))
      (is (= 2 (count (:branches p))))
      (is (= :triple (:kind (first (:branches p)))))
      (is (= :or (:kind (second (:branches p)))))
      (is (= 2 (count (:branches (second (:branches p))))))))

  (testing "deeply nested or (3 levels) compiles to matching descriptor tree"
    (let [[p] (patterns '{:find [?e]
                          :where [(or [?e :name "Ada"]
                                      (or [?e :name "Bob"]
                                          (or [?e :name "Carla"]
                                              [?e :name "Dave"])))]})]
      (is (= :or (:kind p)))
      (is (= :or (:kind (second (:branches p)))))
      (is (= :or (:kind (second (:branches (second (:branches p)))))))))

  (testing "or branch positions are 0, 1, … within their immediate or clause"
    (let [[p] (patterns '{:find [?e]
                          :where [(or [?e :sex :male]
                                      [?e :sex :female]
                                      [?e :sex :other])]})]
      (is (= [0 1 2] (mapv :index (:branches p))))))

  (testing "allows :and branch inside or"
    (let [[p] (patterns '{:find [?e]
                          :where [(or [?e :name "Ada"]
                                      (and [?e :name "Bob"]
                                           [?e :age 30]))]})]
      (is (= :or (:kind p)))
      (is (= '[?e] (:groundable p)))
      (is (= :and (:kind (second (:branches p)))))
      (is (= '[?e] (:vars (second (:branches p)))))
      (is (= '[?e] (:groundable (second (:branches p)))))
      (is (= [:triple :triple]
             (mapv :kind (:children (second (:branches p))))))))

  (testing "allows :not branch inside or"
    (let [[_outer p] (patterns '{:find [?e]
                                 :where [[?e :name n]
                                         (or [?e :name "Ada"]
                                             (not [?e :name "Bob"]))]})]
      (is (= :or (:kind p)))
      (is (= :not (:kind (second (:branches p)))))
      (is (= '[?e] (:vars (second (:branches p)))))
      ;; the :not branch grounds nothing, so neither does the :or
      (is (= [] (:groundable (second (:branches p)))))
      (is (= [] (:groundable p)))))

  (testing "nested :or inside :and is preserved"
    (let [[p] (patterns '{:find [?e]
                          :where [(or (and [?e :sex :male]
                                           (or [?e :name "Ivan"]
                                               [?e :name "Bob"]))
                                      [?e :sex :female])]})]
      (is (= :and (:kind (first (:branches p)))))
      (is (= [:triple :or]
             (mapv :kind (:children (first (:branches p)))))))))

(deftest compile-pattern-not-test
  (testing "not descriptor carries child descriptors and free variables"
    (let [[_outer p] (patterns '{:find [?e]
                                 :where [[?e :name name]
                                         (not [?e :last-name "Smith"])]})]
      (is (= :not (:kind p)))
      (is (= '[?e] (:vars p)))
      (is (= [] (:groundable p)))
      (is (= 1 (count (:children p))))
      (is (= :triple (:kind (first (:children p)))))))

  (testing "not with unbound variables is rejected by query validation"
    (is (thrown? clojure.lang.ExceptionInfo
                 (dbsp/parse '{:find [?e]
                               :where [[?e :name name]
                                       (not [?other :last-name "Smith"])]})))))

(deftest compile-pattern-groundable-test
  (testing "vars from an outer scope are not groundable by an inner and/not"
    (let [[_outer p] (patterns '{:find [?n]
                                 :where [[?e :name ?n]
                                         (or (and [?e :age 30]
                                                  (not [?e :name ?n]
                                                       [?e :age 35])))]})
          and-branch (first (:branches p))]
      (is (= :or (:kind p)))
      (is (= :and (:kind and-branch)))
      (is (= '[?e ?n] (:vars and-branch)))
      (is (= '[?e] (:groundable and-branch)))
      (is (= '[?e ?n] (:vars p)))
      (is (= '[?e] (:groundable p)))))

  (testing "an or of only not branches grounds nothing"
    (let [[_outer p] (patterns '{:find [?e]
                                 :where [[?e :name ?n]
                                         (or (not [?e :age 1]))]})]
      (is (= :or (:kind p)))
      (is (= '[?e] (:vars p)))
      (is (= [] (:groundable p))))))

;; --------------------------------------------------------------------------
;; Left-deep join order
;; --------------------------------------------------------------------------

(deftest left-deep-order-test
  (testing "patterns all sharing one variable keep query order"
    (is (= [0 1 2]
           (order-indices '{:find [name]
                            :where [[?e :name name]
                                    [?e :age age]
                                    [?e :last-name ln]]}))))

  (testing "connectivity reorders patterns ahead of disconnected ones"
    ;; p0 [?a ?b], p1 [?c ?d], p2 [?b ?c]: 0 -> 2 (shares ?b) -> 1 (shares ?c)
    (is (= [0 2 1]
           (order-indices '{:find [?a]
                            :where [[?a :foo ?b]
                                    [?c :bar ?d]
                                    [?b :baz ?c]]}))))

  (testing "fully disconnected patterns fall back to index order (Cartesian)"
    (is (= [0 1]
           (order-indices '{:find [?a]
                            :where [[?a :foo ?b]
                                    [?c :bar ?d]]}))))

  (testing "single pattern"
    (is (= [0] (order-indices '{:find [name] :where [[?e :name name]]})))))

(deftest left-deep-order-groundable-test
  (testing "an :or needing outer bindings is deferred until they are grounded"
    (is (= [1 0]
           (order-indices '{:find [?n]
                            :where [(or (and [?e :age 30]
                                             (not [?e :name ?n]
                                                  [?e :age 35])))
                                    [?e :name ?n]]}))))

  (testing "a :not is ordered inline once its variables are grounded"
    ;; the not shares 2 vars with the grounded set, the disconnected triple 0,
    ;; so the not is introduced mid-chain despite its higher index
    (is (= [0 2 1]
           (order-indices '{:find [?n]
                            :where [[?e :name ?n]
                                    [?a :age ?g]
                                    (not [?e :name ?n])]}))))

  (testing "a seeded grounded set makes otherwise unplannable patterns introducible"
    (let [[_outer or-p] (patterns '{:find [?e]
                                    :where [[?e :name ?n]
                                            (or (not [?e :age 1]))]})]
      (is (thrown? clojure.lang.ExceptionInfo
                   (dbsp/left-deep-order [or-p])))
      (is (= [1] (mapv :index (dbsp/left-deep-order [or-p] '#{?e}))))))

  (testing "a fn clause is deferred until its argument variables are grounded"
    (is (= [1 0]
           (order-indices '{:find [half]
                            :where [[(quot age 2) half]
                                    [?e :age age]]}))))

  (testing "chained fn results ground one another in dependency order"
    (is (= [2 1 0]
           (order-indices '{:find [h2]
                            :where [[(inc half) h2]
                                    [(quot age 2) half]
                                    [?e :age age]]}))))

  (testing "a fn clause with an unbound argument fails with insufficient binding"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"not bound"
                          (dbsp/plan '{:find [y]
                                       :where [[?e :name name]
                                               [(inc x) y]]}))))

  (testing "cyclically dependent fn clauses fail with insufficient binding"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"not bound"
                          (dbsp/plan '{:find [x y]
                                       :where [[?e :name name]
                                               [(inc x) y]
                                               [(inc y) x]]}))))

  (testing "a self-referential fn clause with an unbound variable fails"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"not bound"
                          (dbsp/plan '{:find [x]
                                       :where [[?e :name name]
                                               [(inc x) x]]}))))

  (testing "unbound variables fail planning with an insufficient-binding error"
    (let [ex (try (dbsp/plan '{:find [?n]
                               :where [(or (and [?e :age 30]
                                                (not [?e :name ?n]
                                                     [?e :age 35])))]})
                  nil
                  (catch clojure.lang.ExceptionInfo e e))]
      (is (some? ex))
      (is (re-find #"not bound" (ex-message ex)))
      (is (= :db.error/insufficient-binding (:db/error (ex-data ex))))))

  (testing "cyclic nested groundability requires an earlier seed"
    (let [nested-first '{:find [?x ?y]
                         :where [(or
                                  (and
                                   (or (and [?x :name "left"]
                                            (not [?y :city "blocked"])))
                                   (or (and [?y :name "right"]
                                            (not [?x :city "blocked"])))))
                                 [?x :last-name "seed"]]}
          seed-first '{:find [?x ?y]
                       :where [[?x :last-name "seed"]
                               (or
                                (and
                                 (or (and [?x :name "left"]
                                          (not [?y :city "blocked"])))
                                 (or (and [?y :name "right"]
                                          (not [?x :city "blocked"])))))]}
          ex (try
               (dbsp/plan nested-first)
               nil
               (catch clojure.lang.ExceptionInfo e e))]
      ;; Datomic likewise rejects this clause order with insufficient bindings, so
      ;; we do not reorder this cyclic nested dependency ahead of its later seed.
      (is (map? (dbsp/plan seed-first)))
      (is (some? ex))
      (is (= :db.error/insufficient-binding (:db/error (ex-data ex)))))))

;; --------------------------------------------------------------------------
;; Full join plan
;; --------------------------------------------------------------------------

(deftest plan-single-pattern-test
  (let [p (dbsp/plan '{:find [name] :where [[?e :name name]]})]
    (is (= :triple (get-in p [:where-plan :kind])))
    (is (= '[?e name] (:result-vars p)))
    (is (= [1] (:final-permute p)))
    (let [pat (:where-plan p)]
      (is (= :aev (:order pat)))
      (is (= {0 :name} (:filter pat)))
      (is (= [1 2] (:project pat)))
      (is (= '[?e name] (:out-vars pat))))))

(deftest plan-multi-pattern-chain-test
  (let [p (dbsp/plan '{:find [?a ?d]
                       :where [[?a :r ?b]
                               [?b :s ?c]
                               [?c :t ?d]]})
        chain (:where-plan p)
        [base j1 j2] (:children chain)]
    (testing "a multi-pattern query plans as a chain of joins off a base relation"
      (is (= :chain (:kind chain)))
      (is (nil? (:incoming chain)))
      (is (= 3 (count (:children chain))))
      (is (= :triple (:kind base)))
      (is (nil? (:incoming base)))
      (is (every? #(= :triple (:kind %)) [j1 j2]))
      ;; each child extends the layout produced by its predecessor
      (is (= (mapv :out-vars [base j1])
             (mapv :incoming [j1 j2]))))

    (testing "later joins re-permute the running relation so the key columns lead"
      (is (= [1 0] (:left-permute j1)))
      (is (= [2 0 1] (:left-permute j2)))
      (is (= '[?c ?b ?a ?d] (:result-vars p)))
      (is (= [2 3] (:final-permute p))))))

(deftest plan-two-pattern-join-test
  (let [p (dbsp/plan '{:find [name age]
                       :where [[?e :name name]
                               [?e :age age]]})
        chain (:where-plan p)]
    (is (= :chain (:kind chain)))
    (is (= '[?e name age] (:result-vars p)))
    (is (= [1 2] (:final-permute p)))
    (is (= 2 (count (:children chain))))
    (let [[base j] (:children chain)]
      (is (= :triple (:kind base)))
      (is (nil? (:incoming base)))
      (is (= :triple (:kind j)))
      (is (= '[?e name] (:incoming j)))
      (is (= 1 (:key-arity j)))
      (is (= '[?e] (:key-vars j)))
      (is (nil? (:left-permute j)))
      (is (= '[?e name age] (:out-vars j))))))

(deftest plan-ave-order-test
  (testing "a pattern joining on its value column is fed in :ave order"
    (let [p (dbsp/plan '{:find [?e ?p]
                         :where [[?e :name name]
                                 [?p :age name]]})
          chain (:where-plan p)
          [p0 p1] (:children chain)]
      (is (= :chain (:kind chain)))
      (is (= :triple (:kind p0)))
      (is (= :triple (:kind p1)))
      (is (= :aev (:order p0)))
      (is (= :ave (:order p1)))
      (is (= {0 :name} (:filter p0)))
      (is (= [1 2] (:project p0)))
      (is (= {0 :age} (:filter p1)))
      (is (= [1 2] (:project p1)))
      (is (= '[?e name] (:incoming p1)))
      (is (= '[name ?e ?p] (:result-vars p))))))

(deftest plan-constant-filter-test
  (testing "a constant value column becomes a Filter and is projected away"
    (let [p (dbsp/plan '{:find [?e] :where [[?e :name "Ivan"]]})
          pat (:where-plan p)]
      (is (= :triple (:kind pat)))
      (is (= {0 :name, 2 "Ivan"} (:filter pat)))
      (is (= [1] (:project pat)))
      (is (= '[?e] (:out-vars pat)))))
  (testing "a constant entity column becomes a Filter and is projected away"
    (let [p (dbsp/plan '{:find [name] :where [[1 :name name]]})
          pat (:where-plan p)]
      (is (= :triple (:kind pat)))
      (is (= {0 :name, 1 1} (:filter pat)))
      (is (= [2] (:project pat)))
      (is (= '[name] (:out-vars pat))))))

(deftest plan-or-only-test
  (testing "single :or block as the only pattern"
    (let [p (dbsp/plan '{:find [?e]
                         :where [(or [?e :sex :male]
                                     [?e :sex :female])]})
          pat (:where-plan p)]
      (is (= :union (:kind pat)))
      (is (nil? (:incoming pat)))
      (is (= '[?e] (:out-vars pat)))
      (is (= 2 (count (:branches pat))))
      (is (every? #(= '[?e] (:out-vars %)) (:branches pat)))
      (is (every? #(= :triple (:kind %)) (:branches pat)))
      (is (every? #(nil? (:incoming %)) (:branches pat)))
      (is (= '[?e] (:result-vars p))))))

(deftest plan-or-with-outer-join-test
  (testing ":or after an outer triple becomes a :union node extending the running relation"
    (let [p (dbsp/plan '{:find [name]
                         :where [[?e :name name]
                                 (or [?e :sex :male]
                                     [?e :sex :female])]})
          chain (:where-plan p)
          [base or-node] (:children chain)]
      (is (= :chain (:kind chain)))
      (is (= :triple (:kind base)))
      (is (= :union (:kind or-node)))
      (is (= '[?e name] (:incoming or-node)))
      ;; the or grounds nothing new, so the node keeps the running layout
      (is (= '[?e name] (:out-vars or-node)))
      (is (= 2 (count (:branches or-node))))
      ;; every branch joins the running relation with its triple on ?e
      (is (every? (fn [branch]
                    (and (= :triple (:kind branch))
                         (= '[?e name] (:incoming branch))
                         (= '[?e] (:key-vars branch))))
                  (:branches or-node))))))

(deftest plan-or-multi-var-test
  (testing "2-var :or grounds its extra variable through every branch"
    (let [p (dbsp/plan '{:find [n]
                         :where [[?p :tag "x"]
                                 (or [?p :name n]
                                     [?p :age n])]})
          chain (:where-plan p)
          [_base or-node] (:children chain)]
      (is (= :chain (:kind chain)))
      (is (= :union (:kind or-node)))
      ;; the running relation grounds ?p, the or adds n
      (is (= '[?p n] (:out-vars or-node)))
      (is (every? #(= '[?p n] (:out-vars %)) (:branches or-node))))))

(deftest plan-nested-or-test
  (testing "nested :or plan preserves the descriptor tree"
    (let [p (dbsp/plan '{:find [?e]
                         :where [(or [?e :name "Ada"]
                                     (or [?e :name "Bob"]
                                         [?e :name "Carla"]))]})
          pat (:where-plan p)]
      (is (= :union (:kind pat)))
      (is (= 2 (count (:branches pat))))
      (is (= :triple (:kind (first (:branches pat)))))
      (let [inner (second (:branches pat))]
        (is (= :union (:kind inner)))
        (is (= '[?e] (:out-vars inner)))
        (is (= 2 (count (:branches inner))))
        (is (every? #(= '[?e] (:out-vars %)) (:branches inner)))))))

(deftest plan-and-inside-or-test
  (testing ":and branch inside :or becomes a chain extending the running relation"
    (let [p (dbsp/plan '{:find [name]
                         :where [[?e :name name]
                                 (or [?e :last-name name]
                                     (and [?e :sex :male]
                                          [?e :name name]))]})
          [_base or-node] (:children (:where-plan p))
          and-branch (second (:branches or-node))]
      (is (= :union (:kind or-node)))
      (is (= '[?e name] (:out-vars or-node)))
      (is (= :chain (:kind and-branch)))
      (is (= '[?e name] (:incoming and-branch)))
      (is (= 2 (count (:children and-branch))))
      (is (every? #(and (= :triple (:kind %)) (some? (:incoming %)))
                  (:children and-branch)))
      (is (= '[?e name] (:out-vars and-branch)))))

  (testing ":and branch of a scope-initial :or is finally permuted when its natural join order differs"
    (let [p (dbsp/plan '{:find [?x ?y]
                         :where [(or [?x :edge ?y]
                                     (and [?y :name ?x]
                                          [?y :edge ?x]))]})
          or-node (:where-plan p)
          and-branch (second (:branches or-node))]
      (is (= :union (:kind or-node)))
      (is (= :chain (:kind and-branch)))
      (is (nil? (:incoming and-branch)))
      (is (= '[?x ?y] (:out-vars and-branch)))
      (is (= {:kind :permute :indices [1 0] :out-vars '[?x ?y]}
             (last (:children and-branch)))))))

(deftest plan-nested-or-inside-and-test
  (testing "nested :or inside an :and branch becomes a :union node in the branch chain"
    (let [p (dbsp/plan '{:find [?e]
                         :where [(or (and [?e :sex :male]
                                          (or [?e :name "Ivan"]
                                              [?e :name "Bob"]))
                                     [?e :sex :female])]})
          or-node (:where-plan p)
          and-branch (first (:branches or-node))
          nested-or (second (:children and-branch))]
      (is (= :union (:kind or-node)))
      (is (= :chain (:kind and-branch)))
      (is (= :union (:kind nested-or)))
      (is (= '[?e] (:incoming nested-or)))
      (is (= '[?e] (:out-vars and-branch)))
      (is (= '[?e] (:out-vars nested-or))))))

(deftest plan-not-test
  (testing "`not` plans as an anti-semijoin :difference node off the running relation"
    (let [p (dbsp/plan '{:find [name]
                         :where [[?e :name name]
                                 (not [?e :last-name "Smith"])]})
          chain (:where-plan p)
          [base diff] (:children chain)]
      (is (= :chain (:kind chain)))
      (is (= :triple (:kind base)))
      (is (= :difference (:kind diff)))
      (is (= '[?e name] (:incoming diff)))
      (is (= '[?e name] (:result-vars p)))
      (is (= '[?e name] (:out-vars diff)))
      (is (= '[?e] (:key-vars diff)))
      ;; the anti key already leads the running layout — no re-order needed
      (is (= '[?e name] (:keyed-vars diff)))
      (is (= :triple (:kind (:negative diff))))
      ;; the negative relation extends the running relation's key columns
      (is (= '[?e] (:incoming (:negative diff))))))

  (testing "`not` records the keyed positive shape when the anti key is not leading"
    (let [p (dbsp/plan '{:find [name city]
                         :where [[?e :name name]
                                 [name :city city]
                                 (not [?e :last-name "Smith"])]})
          diff (last (:children (:where-plan p)))]
      (is (= :difference (:kind diff)))
      (is (= '[?e] (:key-vars diff)))
      (is (= '[name ?e city] (:incoming diff)))
      (is (= '[name ?e city] (:out-vars diff)))
      (is (= '[?e name city] (:keyed-vars diff)))))

  (testing "a bare `not` branch inside `or` anti-joins the running relation"
    (let [p (dbsp/plan '{:find [?e]
                         :where [[?e :name n]
                                 (or [?e :name "Ada"]
                                     (not [?e :name "Bob"]))]})
          [_base or-node] (:children (:where-plan p))
          [pos-branch not-branch] (:branches or-node)]
      (is (= :union (:kind or-node)))
      (is (= :triple (:kind pos-branch)))
      (is (= '[?e n] (:incoming pos-branch)))
      (is (= :difference (:kind not-branch)))
      (is (= '[?e n] (:incoming not-branch)))
      (is (= '[?e] (:key-vars not-branch)))
      (is (= '[?e n] (:out-vars not-branch))))))

(deftest plan-predicate-filter-test
  (testing "a bound predicate plans as a :filter chain child after the joins"
    (let [p (dbsp/plan '{:find [name]
                         :where [[?e :name name]
                                 [?e :age age]
                                 [(< age 50)]]})
          chain (:where-plan p)
          [base join filter-node] (:children chain)]
      (is (= :chain (:kind chain)))
      (is (= 3 (count (:children chain))))
      (is (= :triple (:kind base)))
      (is (= :triple (:kind join)))
      (is (= :filter (:kind filter-node)))
      (is (= '[?e name age] (:incoming filter-node)))
      (is (= '[?e name age] (:out-vars filter-node)))
      (is (= '< (-> filter-node :predicate :predicate)))
      (is (= '[?e name age] (:result-vars p)))))

  (testing "a predicate is scheduled as soon as its variables are grounded"
    (let [p (dbsp/plan '{:find [name]
                         :where [[?e :name name]
                                 [(re-find #"A" name)]
                                 [?e :age age]]})
          [_base filter-node join] (:children (:where-plan p))]
      (is (= :filter (:kind filter-node)))
      (is (= '[?e name] (:out-vars filter-node)))
      (is (= :triple (:kind join)))
      (is (= '[?e name] (:incoming join)))))

  (testing "boolean predicate trees retain relational nodes and individual filter leaves"
    (let [p (dbsp/plan '{:find [age]
                         :where [[?e :age age]
                                 (or (and [(> age 10)]
                                          (not [(< age 30)]))
                                     [(= age 42)])]})
          [_base or-node] (:children (:where-plan p))
          [and-branch equals-filter] (:branches or-node)
          [greater-filter diff] (:children and-branch)
          less-filter (:negative diff)
          filters [greater-filter less-filter equals-filter]]
      (is (= [:union :chain :difference]
             (mapv :kind [or-node and-branch diff])))
      (is (= [[:filter :predicate '>]
              [:filter :predicate '<]
              [:filter :predicate '=]]
             (mapv (juxt :kind
                         #(-> % :predicate :kind)
                         #(-> % :predicate :predicate))
                   filters)))))

  (testing "predicate-only top-level queries are rejected as insufficient binding"
    (is (thrown? clojure.lang.ExceptionInfo
                 (dbsp/plan '{:find [age] :where [[(< age 30)]]}))))

  (testing "a variable-free predicate with no positive relation before it is rejected"
    (is (thrown? clojure.lang.ExceptionInfo
                 (dbsp/plan '{:find [age]
                              :where [[(< 1 2)]
                                      [?e :age age]]}))))

  (testing "predicates with unbound variables are rejected"
    (is (thrown? clojure.lang.ExceptionInfo
                 (dbsp/plan '{:find [name]
                              :where [[?e :name name]
                                      [(< age 50)]]})))))

(deftest plan-function-test
  (testing "a fn with a new result variable plans as a :function node appending a column"
    (let [p (dbsp/plan '{:find [half]
                         :where [[?e :age age]
                                 [(quot age 2) half]]})
          chain (:where-plan p)
          [base function-node] (:children chain)]
      (is (= :chain (:kind chain)))
      (is (= :triple (:kind base)))
      (is (= :function (:kind function-node)))
      (is (= '[?e age] (:incoming function-node)))
      (is (= '[?e age half] (:out-vars function-node)))
      (is (= 'quot (-> function-node :function :fn)))
      (is (= '[?e age half] (:result-vars p)))
      (is (= [2] (:final-permute p)))))

  (testing "a fn whose result variable is already bound plans as a :function node"
    (let [p (dbsp/plan '{:find [age]
                         :where [[?e :age age]
                                 [?e :salary half]
                                 [(quot age 2) half]]})
          [_base _join function-node] (:children (:where-plan p))]
      (is (= :function (:kind function-node)))
      (is (= '[?e age half] (:incoming function-node)))
      (is (= '[?e age half] (:out-vars function-node)))
      (is (= 'quot (-> function-node :function :fn)))))

  (testing "fn branches of an `or` are arranged to the union layout"
    (let [p (dbsp/plan '{:find [res]
                         :where [[?e :age age]
                                 (or [(inc age) res]
                                     [(dec age) res])]})
          [_base or-node] (:children (:where-plan p))]
      (is (= :union (:kind or-node)))
      (is (= '[?e age res] (:out-vars or-node)))
      (is (= [[:function '[?e age res]]
              [:function '[?e age res]]]
             (mapv (juxt :kind :out-vars) (:branches or-node))))))

  (testing "a fn inside a not body plans as a :function over the key columns"
    (let [p (dbsp/plan '{:find [name]
                         :where [[?e :name name]
                                 [?e :age age]
                                 [?e :salary sal]
                                 (not [(identity age) sal])]})
          diff (peek (:children (:where-plan p)))
          negative (:negative diff)]
      (is (= :difference (:kind diff)))
      (is (= '[age sal] (:key-vars diff)))
      (is (= :function (:kind negative)))
      (is (= '[age sal] (:incoming negative)))
      (is (= '[age sal] (:out-vars negative)))))

  (testing "function-only queries are rejected"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"without a positive relation"
                          (dbsp/plan '{:find [x] :where [[(inc 1) x]]}))))

  (testing "a constant-args fn with no positive relation before it is rejected"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"without a positive relation"
                          (dbsp/plan '{:find [x]
                                       :where [[(inc 1) x]
                                               [?e :age x]]})))))

;; --------------------------------------------------------------------------
;; Circuit assembly
;; --------------------------------------------------------------------------

(defn- assemble [query]
  (dbsp/plan->circuit (dbsp/plan query)))

(defn- circuit->tree
  "Renders [circuit]'s wiring as a nested vector `[op-name & input-trees]`
  rooted at the final (output) operator. Input sources are already named
  `input-<n>` in creation order by the circuit. It should be noted that
  streams being fed into multiple sub-trees appear multiple times, but
  this does not reflect the actual circuit DAG, where they are just produced
  once."
  [circuit]
  (let [names (vec (.operatorNames circuit))
        inputs (mapv vec (.nodeInputs circuit))]
    (letfn [(render [id]
              (into [(symbol (nth names id))] (map render (nth inputs id))))]
      (render (dec (count names))))))

(deftest assemble-single-pattern-test
  (let [{:keys [circuit leaves output]} (assemble '{:find [name]
                                                    :where [[?e :name name]]})]
    (is (= 1 (count leaves)))
    (is (some? output))
    ;; source pipeline (filter constants, project to variables), find projection
    (is (= '[project [project [filter-constants [input-0]]]]
           (circuit->tree circuit)))))

(deftest assemble-three-pattern-chain-test
  (let [{:keys [circuit leaves]} (assemble '{:find [?a ?d]
                                             :where [[?a :r ?b]
                                                     [?b :s ?c]
                                                     [?c :t ?d]]})]
    (is (= 3 (count leaves)))
    ;; each join permutes the running relation so its key columns lead, then
    ;; joins the next triple's source pipeline; final find projection on top
    (is (= '[project
             [incremental-join
              [project
               [incremental-join
                [project [project [filter-constants [input-0]]]]
                [project [filter-constants [input-1]]]]]
              [project [filter-constants [input-2]]]]]
           (circuit->tree circuit)))))

(deftest assemble-leaves-test
  (testing "plan->circuit returns one {:order … :handle …} leaf per input triple"
    (let [{:keys [leaves]} (assemble '{:find [name]
                                       :where [[?e :name name]]})]
      (is (= 1 (count leaves)))
      (is (= :aev (:order (first leaves))))
      (is (some? (:handle (first leaves))))))

  (testing "each leaf carries :order; aev/ave mixed chain"
    (let [{:keys [leaves]} (assemble '{:find [?e ?p]
                                       :where [[?e :name name]
                                               [?p :age name]]})]
      (is (= 2 (count leaves)))
      (is (every? #{:aev :ave} (map :order leaves)))))

  (testing "leaf count equals total triple count in a chain"
    (let [{:keys [leaves]} (assemble '{:find [?a ?d]
                                       :where [[?a :r ?b]
                                               [?b :s ?c]
                                               [?c :t ?d]]})]
      (is (= 3 (count leaves)))
      (is (every? :order leaves))
      ;; every leaf has its own input handle
      (is (= 3 (count (set (map :handle leaves))))))))

(deftest assemble-or-single-branch-test
  (testing "single-branch :or — no plus, just distinct after the branch"
    (let [{:keys [circuit leaves]} (assemble '{:find [?e]
                                               :where [(or [?e :name "Ada"])]})]
      (is (= 1 (count leaves)))
      (is (= '[project [distinct [project [filter-constants [input-0]]]]]
             (circuit->tree circuit))))))

(deftest assemble-or-k-branch-test
  (testing "k-branch :or — branches plus-folded left-to-right, one distinct on top"
    (let [{:keys [circuit leaves]} (assemble '{:find [?e]
                                               :where [(or [?e :sex :male]
                                                           [?e :sex :female]
                                                           [?e :sex :other])]})]
      (is (= 3 (count leaves)))
      (is (= '[project
               [distinct
                [plus
                 [plus
                  [project [filter-constants [input-0]]]
                  [project [filter-constants [input-1]]]]
                 [project [filter-constants [input-2]]]]]]
             (circuit->tree circuit))))))

(deftest assemble-or-with-outer-join-test
  (testing ":or after an outer triple joins the running stream into every branch"
    (let [{:keys [circuit leaves]} (assemble '{:find [name]
                                               :where [[?e :name name]
                                                       (or [?e :sex :male]
                                                           [?e :sex :female])]})]
      (is (= 3 (count leaves)))
      ;; the running relation (input 0's pipeline) fans out into both branches
      (is (= '[project
               [distinct
                [plus
                 [incremental-join
                  [project [filter-constants [input-0]]]
                  [project [filter-constants [input-1]]]]
                 [incremental-join
                  [project [filter-constants [input-0]]]
                  [project [filter-constants [input-2]]]]]]]
             (circuit->tree circuit))))))

(deftest assemble-nested-or-test
  (testing "nested :or yields one plus fold + one distinct per :or node"
    (let [{:keys [circuit leaves]} (assemble
                                    '{:find [?e]
                                      :where [(or [?e :name "Ada"]
                                                  (or [?e :name "Bob"]
                                                      [?e :name "Carla"]))]})]
      (is (= 3 (count leaves)))
      ;; the inner :or is a fully assembled union whose distinct output is
      ;; just another branch stream of the outer :or
      (is (= '[project
               [distinct
                [plus
                 [project [filter-constants [input-0]]]
                 [distinct
                  [plus
                   [project [filter-constants [input-1]]]
                   [project [filter-constants [input-2]]]]]]]]
             (circuit->tree circuit))))))

(deftest assemble-and-inside-or-test
  (testing ":and inside :or contributes its branch as a join chain"
    (let [{:keys [circuit leaves]} (assemble
                                    '{:find [?e]
                                      :where [(or [?e :sex :female]
                                                  (and [?e :sex :male]
                                                       [?e :name "Ivan"]))]})]
      (is (= 3 (count leaves)))
      ;; the :and branch joins its two source pipelines; the join output is
      ;; unioned with the plain triple branch
      (is (= '[project
               [distinct
                [plus
                 [project [filter-constants [input-0]]]
                 [incremental-join
                  [project [filter-constants [input-1]]]
                  [project [filter-constants [input-2]]]]]]]
             (circuit->tree circuit))))))

(deftest assemble-not-test
  (testing "not assembles as body joined onto the key seed, distinct negative keys, semijoin, and difference"
    (let [{:keys [circuit leaves]} (assemble
                                    '{:find [name]
                                      :where [[?e :name name]
                                              (not [?e :last-name "Smith"])]})]
      (is (= 2 (count leaves)))
      ;; A = input 0's pipeline; it fans out into the minus, the semijoin,
      ;; and (projected to the key columns) the seed of the negative body
      (is (= '[project
               [difference
                [project [filter-constants [input-0]]]
                [incremental-join
                 [project [filter-constants [input-0]]]
                 [distinct
                  [incremental-join
                   [project [project [filter-constants [input-0]]]]
                   [project [filter-constants [input-1]]]]]]]]
             (circuit->tree circuit))))))

(deftest assemble-predicate-filter-test
  (testing "a bound predicate assembles as a stateless filter over the running stream"
    (let [{:keys [circuit leaves]} (assemble '{:find [name]
                                               :where [[?e :name name]
                                                       [(re-find #"A" name)]]})]
      (is (= 1 (count leaves)))
      (is (= '[project [filter-predicate [project [filter-constants [input-0]]]]]
             (circuit->tree circuit))))))

(deftest assemble-function-map-test
  (testing "a new-result fn assembles as a map-function over the running stream, adding no leaves"
    (let [{:keys [circuit leaves]} (assemble '{:find [half]
                                               :where [[?e :age age]
                                                       [(quot age 2) half]]})]
      (is (= 1 (count leaves)))
      (is (= '[project [map-function [project [filter-constants [input-0]]]]]
             (circuit->tree circuit))))))

(deftest assemble-function-filter-test
  (testing "a bound-result fn assembles as a filter-function over the running stream"
    (let [{:keys [circuit leaves]} (assemble '{:find [age]
                                               :where [[?e :age age]
                                                       [?e :salary half]
                                                       [(quot age 2) half]]})]
      (is (= 2 (count leaves)))
      (is (= '[project
               [filter-function
                [incremental-join
                 [project [filter-constants [input-0]]]
                 [project [filter-constants [input-1]]]]]]
             (circuit->tree circuit))))))

;; --------------------------------------------------------------------------
;; Per-pattern delta construction
;; --------------------------------------------------------------------------

(deftest db->index-deltas-add-test
  (is (= {:aev {[:name 1 "Ivan"] 1}
          :ave {[:name "Ivan" 1] 1}}
         (dbsp/db->index-deltas {:eav {} :schema {}}
                                [{:db/id 1 :name "Ivan"}]))))

(deftest db->index-deltas-cardinality-one-replacement-test
  (testing "an add over an existing cardinality-one value retracts the old one"
    (is (= {:aev {[:name 1 "Ivan"] -1, [:name 1 "Ivanov"] 1}
            :ave {[:name "Ivan" 1] -1, [:name "Ivanov" 1] 1}}
           (dbsp/db->index-deltas {:eav {1 {:name #{"Ivan"}}} :schema {}}
                                  [{:db/id 1 :name "Ivanov"}]))))

  (testing "a tx with explicit retract plus replacement only retracts the old value once"
    (is (= {:aev {[:name 1 "Ivan"] -1, [:name 1 "Ivanov"] 1}
            :ave {[:name "Ivan" 1] -1, [:name "Ivanov" 1] 1}}
           (dbsp/db->index-deltas {:eav {1 {:name #{"Ivan"}}} :schema {}}
                                  [[:db/retract 1 :name "Ivan"]
                                   [:db/add 1 :name "Ivanov"]]))))

  (testing "a tx with duplicate retract only results in one retraction"
    (is (= {:aev {[:name 1 "Ivan"] -1} :ave {[:name "Ivan" 1] -1}}
           (dbsp/db->index-deltas {:eav {1 {:name #{"Ivan"}}} :schema {}}
                                  [[:db/retract 1 :name "Ivan"]
                                   [:db/retract 1 :name "Ivan"]]))))

  (testing "false existing values are still retracted during replacement"
    (is (= {:aev {[:enabled 1 false] -1, [:enabled 1 true] 1}
            :ave {[:enabled false 1] -1, [:enabled true 1] 1}}
           (dbsp/db->index-deltas {:eav {1 {:enabled #{false}}} :schema {}}
                                  [[:db/add 1 :enabled true]])))))

(deftest db->index-deltas-cardinality-many-duplicate-test
  (testing "adding an existing cardinality-many value is a no-op"
    (is (= {}
           (dbsp/db->index-deltas {:eav {1 {:edge #{2}}}
                                   :schema {:edge {:db/cardinality :db.cardinality/many}}}
                                  [[:db/add 1 :edge 2]])))))

(deftest db->index-deltas-retract-test
  (testing "retracting a present fact"
    (is (= {:aev {[:name 1 "Ivan"] -1}
            :ave {[:name "Ivan" 1] -1}}
           (dbsp/db->index-deltas {:eav {1 {:name #{"Ivan"}}} :schema {}}
                                  [[:db/retract 1 :name "Ivan"]]))))
  (testing "retracting an absent fact yields no delta"
    (is (= {}
           (dbsp/db->index-deltas {:eav {} :schema {}}
                                  [[:db/retract 1 :name "Ivan"]])))))

(deftest index-delta-zset-test
  (testing ":aev order keeps [a e v]"
    (let [zs (dbsp/index-delta-zset {:aev {[:name 1 "Ivan"] 1}} :aev)]
      (is (= 1 (.getSize zs)))
      (is (= 1 (.getValue (.weight zs (Tuple/of (object-array [:name 1 "Ivan"]))))))))

  (testing ":ave order keeps [a v e]"
    (let [zs (dbsp/index-delta-zset {:ave {[:name "Ivan" 1] 1}} :ave)]
      (is (= 1 (.getValue (.weight zs (Tuple/of (object-array [:name "Ivan" 1]))))))))

  (testing "zero-weight facts are dropped"
    (is (.isEmpty (dbsp/index-delta-zset {:aev {[:name 1 "Ivan"] 0}} :aev)))))

;; --------------------------------------------------------------------------
;; compile-query + compute-delta!
;; --------------------------------------------------------------------------

(deftest compute-delta-single-pattern-update-test
  (let [node fix/*node*]
    (h/transact node [{:db/id 1 :name "Ivan"}])
    (let [db-before (h/db node)
          iq (dbsp/compile-query db-before '{:find [name] :where [[1 :name name]]})
          delta (dbsp/compute-delta! iq db-before [{:db/id 1 :name "Ivanov"}])]
      (is (= #{[["Ivan"] -1] [["Ivanov"] 1]} (set delta))))))

(deftest compute-delta-two-pattern-join-test
  (let [node fix/*node*]
    (h/transact node [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}])
    (let [db-before (h/db node)
          iq (dbsp/compile-query db-before '{:find [name last-name]
                                             :where [[e :name name]
                                                     [e :last-name last-name]]})
          delta (dbsp/compute-delta! iq db-before
                                     [{:db/id :petr :name "Petr" :last-name "Petrov"}])]
      ;; the new entity joins; the primed entity produces no delta
      (is (= #{[["Petr" "Petrov"] 1]} (set delta))))))

(deftest compute-delta-queue-test
  (let [node fix/*node*]
    (h/transact node [{:db/id 1 :name "Ivan"}])
    (let [db-before (h/db node)
          iq (dbsp/compile-query db-before '{:find [name] :where [[1 :name name]]})]
      (dbsp/compute-delta! iq db-before [{:db/id 1 :name "Ivanov"}])
      (is (= #{[["Ivan"] -1] [["Ivanov"] 1]} (set (dbsp/pop-result! iq))))
      (is (nil? (dbsp/pop-result! iq))))))

;; --------------------------------------------------------------------------
;; End-to-end via q-inc / transact / consume-delta!
;; --------------------------------------------------------------------------

(deftest e2e-single-pattern-test
  (let [node fix/*node*
        iq (h/q-inc node '{:find [name] :where [[1 :name name]]})]
    (h/transact node [{:db/id 1 :name "Ivan"}])
    (is (= #{[["Ivan"] 1]} (set (h/consume-delta! iq))))))

(deftest e2e-update-test
  (let [node fix/*node*]
    (h/transact node [{:db/id 1 :name "Ivan"}])
    (let [iq (h/q-inc node '{:find [name] :where [[1 :name name]]})]
      (h/transact node [{:db/id 1 :name "Ivanov"}])
      (is (= #{[["Ivan"] -1] [["Ivanov"] 1]} (set (h/consume-delta! iq)))))))

(deftest e2e-two-pattern-join-test
  (let [node fix/*node*]
    (h/transact node [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}])
    (let [iq (h/q-inc node '{:find [name last-name]
                                    :where [[e :name name]
                                            [e :last-name last-name]]})]
      (h/transact node [{:db/id :petr :name "Petr" :last-name "Petrov"}])
      (is (= #{[["Petr" "Petrov"] 1]} (set (h/consume-delta! iq)))))))

(deftest e2e-self-join-test
  (let [node fix/*node*
        iq (h/q-inc node '{:find [?a ?b]
                                  :where [[?a :name n]
                                          [?b :name n]]})]
    (h/transact node [{:db/id 1 :name "Ivan"} {:db/id 2 :name "Ivan"}])
    ;; every ordered pair of entities sharing the name, including reflexive
    (is (= #{[[1 1] 1] [[1 2] 1] [[2 1] 1] [[2 2] 1]}
           (set (h/consume-delta! iq))))))

(deftest e2e-three-pattern-chain-test
  (let [node fix/*node*
        iq (h/q-inc node '{:find [?a ?d]
                                  :where [[?a :edge ?b]
                                          [?b :edge ?c]
                                          [?c :edge ?d]]})]
    ;; path n1 -> n2 -> n3 -> n4
    (h/transact node [{:db/id :n1 :edge :n2}
                      {:db/id :n2 :edge :n3}
                      {:db/id :n3 :edge :n4}])
    (is (= #{[[:n1 :n4] 1]} (set (h/consume-delta! iq))))))

(deftest e2e-triangle-test
  (let [node fix/*node*
        iq (h/q-inc node '{:find [?a ?b ?c]
                                  :where [[?a :edge ?b]
                                          [?b :edge ?c]
                                          [?c :edge ?a]]})]
    ;; directed 3-cycle n1 -> n2 -> n3 -> n1
    (h/transact node [{:db/id :n1 :edge :n2}
                      {:db/id :n2 :edge :n3}
                      {:db/id :n3 :edge :n1}])
    (is (= #{[[:n1 :n2 :n3] 1] [[:n2 :n3 :n1] 1] [[:n3 :n1 :n2] 1]}
           (set (h/consume-delta! iq))))))

(deftest e2e-cartesian-test
  (let [node fix/*node*
        iq (h/q-inc node '{:find [n c]
                                  :where [[e1 :name n]
                                          [e2 :city c]]})]
    (h/transact node [{:db/id 1 :name "Ivan"}
                      {:db/id 2 :name "Bob"}
                      {:db/id 3 :city "NYC"}
                      {:db/id 4 :city "Berlin"}])
    (is (= #{[["Ivan" "NYC"] 1]
             [["Ivan" "Berlin"] 1]
             [["Bob" "NYC"] 1]
             [["Bob" "Berlin"] 1]}
           (set (h/consume-delta! iq))))))

(deftest e2e-predicate-filter-test
  (testing "range predicate filters add and retract deltas"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [name]
                             :where [[?e :name name]
                                     [?e :age age]
                                     [(< age 50)]]})]
      (h/transact node [{:db/id :ivan :name "Ivan" :age 30}
                        {:db/id :dominic :name "Dominic" :age 50}])
      (is (= [[["Ivan"] 1]] (h/consume-delta! iq)))
      (h/transact node [[:db/retract :ivan :age 30]])
      (is (= [[["Ivan"] -1]] (h/consume-delta! iq)))))

  (testing "binary predicate filters joined rows"
    (let [node (fresh-node)
          iq (h/q-inc node '{:find [name1 name2]
                             :where [[?e1 :name name1]
                                     [?e1 :age age1]
                                     [?e2 :name name2]
                                     [?e2 :age age2]
                                     [(<= age1 age2)]]})]
      (h/transact node [{:db/id :ivan :name "Ivan" :age 30}
                        {:db/id :bob :name "Bob" :age 40}])
      (is (= #{[["Ivan" "Ivan"] 1]
               [["Ivan" "Bob"] 1]
               [["Bob" "Bob"] 1]}
             (set (h/consume-delta! iq)))))))

(deftest e2e-predicate-only-or-test
  (testing "overlapping predicate-only or branches filter outer-bound values with set semantics"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [age]
                             :where [[?e :age age]
                                     (or [(< age 30)]
                                         [(< age 40)])]})]
      (h/transact node [{:db/id :young :age 20}
                        {:db/id :middle :age 35}
                        {:db/id :older :age 45}])
      (is (= #{[[20] 1] [[35] 1]}
             (set (h/consume-delta! iq)))))))

(deftest e2e-predicate-not-test
  (testing "not over a bound predicate anti-joins rows matching the predicate"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [name]
                             :where [[?e :name name]
                                     [?e :age age]
                                     (not [(< age 30)])]})]
      (h/transact node [{:db/id :young :name "Young" :age 20}
                        {:db/id :middle :name "Middle" :age 35}
                        {:db/id :older :name "Older" :age 50}])
      (is (= #{[["Middle"] 1] [["Older"] 1]}
             (set (h/consume-delta! iq)))))))

(deftest e2e-function-map-test
  (testing "functions compose with predicates and emit only projected result changes"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [name half]
                             :where [[?e :name name]
                                     [?e :age age]
                                     [(quot age 2) half]
                                     [(+ age half) total]
                                     [(< total 50)]]})]
      (h/transact node [{:db/id :ivan :name "Ivan" :age 30}
                        {:db/id :bob :name "Bob" :age 40}])
      (is (= #{[["Ivan" 15] 1]} (set (h/consume-delta! iq))))
      ;; The intermediate total changes, but the projected half does not.
      (h/transact node [{:db/id :ivan :age 31}])
      (is (nil? (h/consume-delta! iq)))
      (h/transact node [{:db/id :ivan :age 32}])
      (is (= #{[["Ivan" 15] -1] [["Ivan" 16] 1]}
             (set (h/consume-delta! iq)))))))

(deftest e2e-function-rebinding-filters-test
  (testing "a fn whose result variable is already bound keeps only matching rows"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [name]
                             :where [[?e :name name]
                                     [?e :age age]
                                     [?e :salary sal]
                                     [(identity age) sal]]})]
      (h/transact node [{:db/id :eq :name "Eq" :age 30 :salary 30}
                        {:db/id :neq :name "Neq" :age 30 :salary 40}])
      (is (= #{[["Eq"] 1]} (set (h/consume-delta! iq))))
      (h/transact node [{:db/id :neq :salary 30}])
      (is (= #{[["Neq"] 1]} (set (h/consume-delta! iq)))))))

(deftest e2e-function-nil-false-values-test
  (testing "nil and false results are values, not row-removal signals"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [name nil-value false-value]
                             :where [[?e :name name]
                                     [(identity nil) nil-value]
                                     [(identity false) false-value]]})]
      (h/transact node [{:db/id :ivan :name "Ivan"}])
      (is (= #{[["Ivan" nil false] 1]}
             (set (h/consume-delta! iq)))))))

(deftest e2e-function-or-test
  (testing "fn branches of an or emit both computed values"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [res]
                             :where [[?e :age age]
                                     (or [(inc age) res]
                                         [(dec age) res])]})]
      (h/transact node [{:db/id :ivan :age 30}])
      (is (= #{[[29] 1] [[31] 1]} (set (h/consume-delta! iq)))))))

(deftest e2e-function-not-test
  (testing "not over a bound fn clause anti-joins matching rows"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [name]
                             :where [[?e :name name]
                                     [?e :age age]
                                     [?e :salary sal]
                                     (not [(identity age) sal])]})]
      (h/transact node [{:db/id :eq :name "Eq" :age 30 :salary 30}
                        {:db/id :neq :name "Neq" :age 30 :salary 40}])
      (is (= #{[["Neq"] 1]} (set (h/consume-delta! iq))))
      ;; making the salaries match flips Neq out of the result
      (h/transact node [{:db/id :neq :salary 30}])
      (is (= #{[["Neq"] -1]} (set (h/consume-delta! iq)))))))

(deftest e2e-cardinality-many-duplicate-add-is-noop-test
  (let [node fix/*node*]
    (h/transact node [[:db/add 1 :edge 2]])
    (let [iq (h/q-inc node '{:find [?to]
                             :where [[1 :edge ?to]]})]
      (h/transact node [[:db/add 1 :edge 2]])
      (is (nil? (h/consume-delta! iq)))
      (h/transact node [[:db/retract 1 :edge 2]])
      (is (= #{[[2] -1]} (set (h/consume-delta! iq)))))))

(deftest e2e-unregister-standard-query-test
  (let [node fix/*node*
        iq (h/q-inc node '{:find [name]
                           :where [[?e :name name]]})]
    (h/transact node [{:db/id 1 :name "Ivan"}])
    (is (= #{[["Ivan"] 1]} (set (h/consume-delta! iq))))
    (h/unregister-inc-q node iq)
    (h/transact node [{:db/id 2 :name "Petr"}])
    (is (nil? (h/consume-delta! iq)))))

(deftest e2e-rejects-unsupported-query-options-test
  (let [node fix/*node*]
    (is (thrown? clojure.lang.ExceptionInfo
                 (h/q-inc node '{:find [name]
                                 :in [name]
                                 :where [[?e :name name]]})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (h/q-inc node '{:find [name]
                                 :in []
                                 :where [[?e :name name]]})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (h/q-inc node '{:find [name]
                                 :keys [name]
                                 :where [[?e :name name]]})))))

;; --------------------------------------------------------------------------
;; End-to-end: flat :or
;; --------------------------------------------------------------------------

(deftest e2e-or-single-branch-equivalent-to-bare-test
  (testing "(or B) produces the same delta as the bare triple B"
    (let [bare-node fix/*node*
          or-node (fresh-node)
          bare-iq (h/q-inc bare-node '{:find [?e]
                                              :where [[?e :name "Ivan"]]})
          or-iq (h/q-inc or-node '{:find [?e]
                                          :where [(or [?e :name "Ivan"])]})]
      (h/transact bare-node [{:db/id 1 :name "Ivan"}])
      (h/transact or-node   [{:db/id 1 :name "Ivan"}])
      (is (= (set (h/consume-delta! bare-iq))
             (set (h/consume-delta! or-iq)))))))

(deftest e2e-or-disjoint-union-test
  (testing "two-branch :or returns the union of its branches"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [?e]
                                    :where [(or [?e :name "Ada"]
                                                [?e :name "Bob"])]})]
      (h/transact node [{:db/id :ada  :name "Ada"}
                        {:db/id :bob  :name "Bob"}
                        {:db/id :carla :name "Carla"}])
      (is (= #{[[:ada] 1] [[:bob] 1]}
             (set (h/consume-delta! iq)))))))

(deftest e2e-or-overlap-distinct-test
  (testing "overlapping branches collapse via DistinctOp"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [?e]
                                    :where [(or [?e :name "X"]
                                                [?e :last-name "X"])]})]
      ;; entity 1 satisfies both branches at once — DistinctOp keeps weight 1
      (h/transact node [{:db/id 1 :name "X" :last-name "X"}])
      (is (= #{[[1] 1]} (set (h/consume-delta! iq))))

      ;; retract one of the two matching facts: entity still in the set, no delta
      (h/transact node [[:db/retract 1 :name "X"]])
      (is (nil? (h/consume-delta! iq)))

      ;; retract the remaining matching fact: entity leaves the set, emit -1
      (h/transact node [[:db/retract 1 :last-name "X"]])
      (is (= #{[[1] -1]} (set (h/consume-delta! iq)))))))

(deftest e2e-or-with-outer-join-add-test
  (testing ":or joined with an outer triple — adds"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [name]
                                    :where [[?e :name name]
                                            (or [?e :last-name "Lovelace"]
                                                [?e :last-name "Turing"])]})]
      (h/transact node [{:db/id :ada  :name "Ada"   :last-name "Lovelace"}
                        {:db/id :alan :name "Alan"  :last-name "Turing"}
                        {:db/id :bob  :name "Bob"   :last-name "Smith"}])
      (is (= #{[["Ada"] 1] [["Alan"] 1]}
             (set (h/consume-delta! iq)))))))

(deftest e2e-or-with-outer-join-retract-test
  (testing ":or joined with an outer triple — retract drops a matching row"
    (let [node fix/*node*]
      (h/transact node [{:db/id :ada :name "Ada" :last-name "Lovelace"}])
      (let [iq (h/q-inc node '{:find [name]
                                      :where [[?e :name name]
                                              (or [?e :last-name "Lovelace"]
                                                  [?e :last-name "Turing"])]})]
        (h/transact node [[:db/retract :ada :last-name "Lovelace"]])
        (is (= #{[["Ada"] -1]} (set (h/consume-delta! iq))))))))

(deftest e2e-or-two-var-in-chain-test
  (testing "2-var :or joined into a chain"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [name]
                                    :where [[?e :city "NYC"]
                                            (or [?e :name name]
                                                [?e :last-name name])]})]
      (h/transact node [{:db/id :ada  :name "Ada"  :last-name "Lovelace" :city "NYC"}
                        {:db/id :bob  :name "Bob"  :last-name "Smith"    :city "London"}])
      (is (= #{[["Ada"] 1] [["Lovelace"] 1]}
             (set (h/consume-delta! iq)))))))

(deftest e2e-or-only-query-test
  (testing "an :or block as the only :where pattern"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [?e]
                                    :where [(or [?e :name "Ada"]
                                                [?e :name "Bob"]
                                                [?e :name "Carla"])]})]
      (h/transact node [{:db/id 1 :name "Ada"}
                        {:db/id 2 :name "Bob"}
                        {:db/id 3 :name "Dave"}])
      (is (= #{[[1] 1] [[2] 1]} (set (h/consume-delta! iq)))))))

(deftest e2e-or-branch-can-use-and-test
  (testing ":standard supports existing query grammar where :or branches contain :and"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [name]
                             :where [[e :name name]
                                     (or [e :last-name "Ivanova"]
                                         (and [e :last-name "Ivanov"]
                                              [e :name "Ivan"]))]})]
      (h/transact node [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}
                        {:db/id :petr :name "Petr" :last-name "Ivanov"}
                        {:db/id :ivana :name "Ivana" :last-name "Ivanova"}])
      (is (= #{[["Ivan"] 1] [["Ivana"] 1]}
             (set (h/consume-delta! iq)))))))

(deftest e2e-or-and-branch-final-permute-test
  (testing ":and branch results are emitted in the parent :or column order"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [?x ?y]
                             :where [(or [?x :edge ?y]
                                         (and [?y :name ?x]
                                              [?y :edge ?x]))]})]
      (h/transact node [{:db/id "node" :name "mirror" :edge "mirror"}])
      (is (= #{[["node" "mirror"] 1]
               [["mirror" "node"] 1]}
             (set (h/consume-delta! iq)))))))

(deftest e2e-not-antijoin-add-negative-test
  (testing "adding a negative fact retracts matching positive rows +
            retracting a negative fact re-adds matching positive rows"
    (let [iq (h/q-inc fix/*node* '{:find [name]
                                   :where [[e :name name]
                                           (not [e :last-name "Smith"])]})]
      (h/transact fix/*node* [{:db/id 1 :name "Alice"}
                              {:db/id 2 :name "Bob" :last-name "Smith"}])
      (is (= [[["Alice"] 1]]
             (h/consume-delta! iq)))
      (h/transact fix/*node* [{:db/id 1 :last-name "Smith"}])
      (is (= [[["Alice"] -1]]
             (h/consume-delta! iq)))
      (h/transact fix/*node* [[:db/retract 1 :last-name "Smith"]])
      (is (= [[["Alice"] 1]]
             (h/consume-delta! iq))))))

(deftest e2e-not-antijoin-duplicate-right-keys-test
  (testing "duplicate right-side keys do not multiply the retraction"
    (let [iq (h/q-inc fix/*node* '{:find [name]
                                   :where [[e :name name]
                                           (not (or [e :last-name "Blocked"]
                                                    [e :city "Blocked"]))]})]
      (h/transact fix/*node* [{:db/id 1 :name "Alice" :last-name "Blocked" :city "Blocked"}])
      (is (nil? (h/consume-delta! iq)))
      (h/transact fix/*node* [[:db/retract 1 :last-name "Blocked"]])
      (is (nil? (h/consume-delta! iq)))
      (h/transact fix/*node* [[:db/retract 1 :city "Blocked"]])
      (is (= [[["Alice"] 1]]
             (h/consume-delta! iq))))))

(deftest e2e-not-antijoin-inside-and-under-or-test
  (testing "not composes inside an and branch under or"
    (let [iq (h/q-inc fix/*node* '{:find [name]
                                   :where [(or (and [e :name name]
                                                    [e :last-name "Fallback"])
                                               (and [e :name name]
                                                    (not [e :last-name "Smith"])))]})]
      (h/transact fix/*node* [{:db/id 1 :name "Alice"}
                              {:db/id 2 :name "Bob" :last-name "Smith"}
                              {:db/id 3 :name "Fallback" :last-name "Fallback"}])
      (is (= [[["Alice"] 1] [["Fallback"] 1]]
             (h/consume-delta! iq))))))

(deftest e2e-not-antijoin-inside-or-with-variable-from-outer-scope
  (let [iq (h/q-inc fix/*node* '{:find [?n]
                                 :where [[?e :name ?n]
                                         (or (and [?e :age 30]
                                                  (not [?e :name ?n]
                                                       [?e :age 35])))]})]
    (h/transact fix/*node* [{:db/id 1 :name "Ivan" :age 30}
                            {:db/id 2 :name "Bob" :age 35}])
    (is (= [[["Ivan"] 1]]
           (h/consume-delta! iq)))))

(deftest e2e-not-antijoin-inside-or-retraction-test
  (testing "retracting the branch's positive trigger emits a retraction"
    (let [iq (h/q-inc fix/*node* '{:find [?n]
                                   :where [[?e :name ?n]
                                           (or (and [?e :age 30]
                                                    (not [?e :name ?n]
                                                         [?e :age 35])))]})]
      (h/transact fix/*node* [{:db/id 1 :name "Ivan" :age 30}
                              {:db/id 2 :name "Bob" :age 35}])
      (is (= [[["Ivan"] 1]]
             (h/consume-delta! iq)))
      (h/transact fix/*node* [[:db/retract 1 :age 30]])
      (is (= [[["Ivan"] -1]]
             (h/consume-delta! iq))))))

(deftest e2e-not-antijoin-with-only-not-clauses
  (let [iq (h/q-inc fix/*node* '{:find [?n]
                                 :where  [[?e :name ?n] (or (not [?e :age 1]) (not [?e :age 2]))]})]
    (h/transact fix/*node* [{:db/id 1 :name "Ivan" :age 30}
                            {:db/id 2 :name "Bob" :age 35}])
    (is (= #{[["Ivan"] 1] [["Bob"] 1]}
           (set (h/consume-delta! iq))))))

(deftest e2e-not-double-negation-test
  (testing "(not (or (not B))) ≡ B — the not body anti-joins the outer key relation"
    (let [iq (h/q-inc fix/*node* '{:find [?n]
                                   :where [[?e :name ?n]
                                           (not (or (not [?e :age 1])))]})]
      (h/transact fix/*node* [{:db/id 1 :name "Ivan" :age 1}
                              {:db/id 2 :name "Bob" :age 2}
                              {:db/id 3 :name "Eve"}])
      (is (= [[["Ivan"] 1]] (h/consume-delta! iq)))
      ;; Bob's age flips to 1: he now satisfies the double negation
      (h/transact fix/*node* [{:db/id 2 :age 1}])
      (is (= [[["Bob"] 1]] (h/consume-delta! iq)))
      ;; retracting Ivan's age removes him again
      (h/transact fix/*node* [[:db/retract 1 :age 1]])
      (is (= [[["Ivan"] -1]] (h/consume-delta! iq))))))

(deftest e2e-not-conjunction-via-double-negation-test
  (testing "(not (or (not B1) (not B2))) ≡ B1 ∧ B2 over a cardinality-many attribute"
    (let [iq (h/q-inc fix/*node* '{:find [?n]
                                   :where [[?e :name ?n]
                                           (not (or (not [?e :edge 10])
                                                    (not [?e :edge 20])))]})]
      ;; only entities carrying BOTH edges qualify
      (h/transact fix/*node* [{:db/id 1 :name "Ivan" :edge 10}])
      (is (nil? (h/consume-delta! iq)))
      (h/transact fix/*node* [{:db/id 1 :edge 20}])
      (is (= [[["Ivan"] 1]] (h/consume-delta! iq)))
      (h/transact fix/*node* [[:db/retract 1 :edge 10]])
      (is (= [[["Ivan"] -1]] (h/consume-delta! iq))))))

(deftest e2e-or-only-not-branches-retraction-test
  (testing "cross-branch distinct state: a row leaves only when no branch supports it"
    (let [iq (h/q-inc fix/*node* '{:find [?n]
                                   :where [[?e :name ?n]
                                           (or (not [?e :edge 10])
                                               (not [?e :edge 20]))]})]
      (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])
      (is (= [[["Ivan"] 1]] (h/consume-delta! iq)))
      ;; edge 10 kills branch 1; branch 2 still supports the row
      (h/transact fix/*node* [{:db/id 1 :edge 10}])
      (is (nil? (h/consume-delta! iq)))
      ;; edge 20 kills branch 2 as well
      (h/transact fix/*node* [{:db/id 1 :edge 20}])
      (is (= [[["Ivan"] -1]] (h/consume-delta! iq)))
      ;; retracting edge 10 revives branch 1
      (h/transact fix/*node* [[:db/retract 1 :edge 10]])
      (is (= [[["Ivan"] 1]] (h/consume-delta! iq))))))

(deftest e2e-not-mid-chain-test
  (testing "a not is applied mid-chain before a later join"
    (let [iq (h/q-inc fix/*node* '{:find [?n ?c]
                                   :where [[?e :name ?n]
                                           (not [?e :last-name "Smith"])
                                           [?e :city ?c]]})]
      (h/transact fix/*node* [{:db/id 1 :name "Ivan" :city "NYC"}
                              {:db/id 2 :name "Bob" :last-name "Smith" :city "London"}])
      (is (= [[["Ivan" "NYC"] 1]]
             (h/consume-delta! iq))))))

(deftest e2e-or-mixed-positive-and-not-branch-test
  (testing "an or with a positive branch and a bare not branch"
    (let [iq (h/q-inc fix/*node* '{:find [?n]
                                   :where [[?e :name ?n]
                                           (or [?e :last-name "Keep"]
                                               (not [?e :last-name "Smith"]))]})]
      (h/transact fix/*node* [{:db/id 1 :name "Ivan"}
                              {:db/id 2 :name "Bob" :last-name "Smith"}])
      (is (= [[["Ivan"] 1]] (h/consume-delta! iq)))
      ;; Bob's last-name flips Smith -> Keep: both branches turn true at once,
      ;; the distinct still emits exactly +1
      (h/transact fix/*node* [{:db/id 2 :last-name "Keep"}])
      (is (= [[["Bob"] 1]] (h/consume-delta! iq))))))

;; --------------------------------------------------------------------------
;; End-to-end: nested :or
;; --------------------------------------------------------------------------

(defn- query-deltas
  "Registers [query] on [node], applies [transactions] one at a time,
  returns the per-transaction deltas as sets."
  [node query transactions]
  (let [iq (h/q-inc node query)]
    (mapv (fn [tx]
            (h/transact node tx)
            (set (h/consume-delta! iq)))
          transactions)))

(deftest e2e-nested-or-equals-flat-test
  (testing "nested (or A (or B C)) produces the same deltas as flat (or A B C)"
    (let [txs [[{:db/id 1 :name "Ada"}
                {:db/id 2 :name "Bob"}
                {:db/id 3 :name "Carla"}
                {:db/id 4 :name "Dave"}]
               [[:db/retract 1 :name "Ada"]
                [:db/retract 4 :name "Dave"]]]
          nested '{:find [?e]
                   :where [(or [?e :name "Ada"]
                               (or [?e :name "Bob"]
                                   [?e :name "Carla"]))]}
          flat   '{:find [?e]
                   :where [(or [?e :name "Ada"]
                               [?e :name "Bob"]
                               [?e :name "Carla"])]}]
      (is (= (query-deltas fix/*node* flat txs)
             (query-deltas (fresh-node) nested txs))))))

(deftest e2e-nested-or-with-outer-join-test
  (testing "nested :or joined with an outer triple"
    (let [node fix/*node*
          iq (h/q-inc node '{:find [name]
                                    :where [[?e :name name]
                                            (or [?e :last-name "Lovelace"]
                                                (or [?e :last-name "Turing"]
                                                    [?e :last-name "Hopper"]))]})]
      (h/transact node [{:db/id :ada    :name "Ada"    :last-name "Lovelace"}
                        {:db/id :alan   :name "Alan"   :last-name "Turing"}
                        {:db/id :grace  :name "Grace"  :last-name "Hopper"}
                        {:db/id :bob    :name "Bob"    :last-name "Smith"}])
      (is (= #{[["Ada"] 1] [["Alan"] 1] [["Grace"] 1]}
             (set (h/consume-delta! iq)))))))

(deftest e2e-deeply-nested-or-test
  (testing "3-level-deep nesting (or A (or B (or C D))) matches the flat form"
    (let [txs [[{:db/id 1 :name "Ada"}
                {:db/id 2 :name "Bob"}
                {:db/id 3 :name "Carla"}
                {:db/id 4 :name "Dave"}
                {:db/id 5 :name "Eve"}]]
          deep '{:find [?e]
                 :where [(or [?e :name "Ada"]
                             (or [?e :name "Bob"]
                                 (or [?e :name "Carla"]
                                     [?e :name "Dave"])))]}
          flat '{:find [?e]
                 :where [(or [?e :name "Ada"]
                             [?e :name "Bob"]
                             [?e :name "Carla"]
                             [?e :name "Dave"])]}]
      (is (= (query-deltas fix/*node* flat txs)
             (query-deltas (fresh-node) deep txs))))))

;; --------------------------------------------------------------------------
;; Cross-engine equivalence: :wcoj vs :standard produce the same deltas
;; --------------------------------------------------------------------------

(defn- run-engine
  "Registers [query] under [version], applies each transaction in [delta-txs],
  and returns the per-transaction deltas as sets."
  [version initial-tx query delta-txs]
  (let [node (fresh-node)]
    (when (seq initial-tx)
      (h/transact node initial-tx))
    (let [iq (binding [h/*dbsp-version* version] (h/q-inc node query))]
      (mapv (fn [tx]
              (h/transact node tx)
              (set (h/consume-delta! iq)))
            delta-txs))))

(defn- cross-engine= [initial-tx query delta-txs]
  (= (run-engine :wcoj initial-tx query delta-txs)
     (run-engine :standard initial-tx query delta-txs)))

(deftest cross-engine-update-test
  (is (cross-engine= [{:db/id 1 :name "Ivan"}]
                     '{:find [name] :where [[1 :name name]]}
                     [[{:db/id 1 :name "Ivanov"}]])))

(deftest cross-engine-two-pattern-join-test
  (is (cross-engine= [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}]
                     '{:find [name last-name]
                       :where [[e :name name]
                               [e :last-name last-name]]}
                     [[{:db/id :petr :name "Petr" :last-name "Petrov"}]
                      [{:db/id :sam :name "Sam" :last-name "Smith"}]])))

(deftest cross-engine-self-join-test
  (is (cross-engine= []
                     '{:find [?a ?b]
                       :where [[?a :name n]
                               [?b :name n]]}
                     [[{:db/id 1 :name "Ivan"} {:db/id 2 :name "Ivan"}]
                      [{:db/id 3 :name "Ivan"}]])))

(deftest cross-engine-multi-step-test
  (is (cross-engine= []
                     '{:find [name last-name]
                       :where [[e :name name]
                               [e :last-name last-name]]}
                     [[{:db/id :a :name "A" :last-name "AA"}]
                      [{:db/id :b :name "B" :last-name "BB"}]
                      [[:db/retractEntity :a]]])))
