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
    :db/cardinality :db.cardinality/many}])

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
      (is (= '[?e name] (:vars p)))))

  (testing "constant value"
    (let [[p] (patterns '{:find [?e] :where [[?e :name "Ivan"]]})]
      (is (= {:kind :constant :value "Ivan"} (:value p)))
      (is (= '[?e] (:vars p)))))

  (testing "constant entity"
    (let [[p] (patterns '{:find [name] :where [[1 :name name]]})]
      (is (= {:kind :constant :value 1} (:entity p)))
      (is (= '[name] (:vars p)))))

  (testing "indices follow :where position"
    (is (= [0 1 2]
           (mapv :index (patterns '{:find [name]
                                    :where [[?e :name name]
                                            [?e :age age]
                                            [?e :last-name ln]]}))))))

(deftest rejects-unsupported-clauses-test
  (testing "predicate clauses are rejected"
    (is (thrown? clojure.lang.ExceptionInfo
                 (dbsp/parse '{:find [?x] :where [[?x :age ?y] [(= ?y 1)]]}))))
  (testing "function clauses are rejected"
    (is (thrown? clojure.lang.ExceptionInfo
                 (dbsp/parse '{:find [?s]
                               :where [[?x :name ?n]
                                       [(str ?n) ?s]]}))))
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
      (is (= :and (:kind (second (:branches p)))))
      (is (= '[?e] (:vars (second (:branches p)))))
      (is (= [:triple :triple]
             (mapv :kind (:children (second (:branches p))))))))

  (testing "allows :not branch inside or"
    (let [[_outer p] (patterns '{:find [?e]
                                 :where [[?e :name n]
                                         (or [?e :name "Ada"]
                                             (not [?e :name "Bob"]))]})]
      (is (= :or (:kind p)))
      (is (= :not (:kind (second (:branches p)))))
      (is (= '[?e] (:vars (second (:branches p)))))))

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
      (is (= 1 (count (:children p))))
      (is (= :triple (:kind (first (:children p)))))))

  (testing "not with unbound variables is rejected by query validation"
    (is (thrown? clojure.lang.ExceptionInfo
                 (dbsp/parse '{:find [?e]
                               :where [[?e :name name]
                                       (not [?other :last-name "Smith"])]})))))

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

(deftest plan-multi-pattern-relation-tree-test
  (testing "a multi-pattern query plans as a join over pattern relations"
    (let [p (dbsp/plan '{:find [?a ?d]
                         :where [[?a :r ?b]
                                 [?b :s ?c]
                                 [?c :t ?d]]})
          join (:where-plan p)]
      (is (= :join (:kind join)))
      (is (= 3 (count (:inputs join))))
      (is (every? #(= :triple (:kind %)) (:inputs join))))))

(deftest plan-two-pattern-join-test
  (let [p (dbsp/plan '{:find [name age]
                       :where [[?e :name name]
                               [?e :age age]]})
        join (:where-plan p)]
    (is (= :join (:kind join)))
    (is (= '[?e name age] (:result-vars p)))
    (is (= [1 2] (:final-permute p)))
    (is (= 2 (count (:inputs join))))
    (is (= 1 (count (:steps join))))
    (let [j (first (:steps join))]
      (is (= 1 (:key-arity j)))
      (is (= '[?e] (:key-vars j)))
      (is (nil? (:left-permute j)))
      (is (= '[?e name age] (:out-vars j))))))

(deftest plan-ave-order-test
  (testing "a pattern joining on its value column is fed in :ave order"
    (let [p (dbsp/plan '{:find [?e ?p]
                         :where [[?e :name name]
                                 [?p :age name]]})
          join (:where-plan p)
          [rel0 rel1] (:inputs join)
          p0 rel0
          p1 rel1]
      (is (= :join (:kind join)))
      (is (= :triple (:kind rel0)))
      (is (= :triple (:kind rel1)))
      (is (= :ave (:order p0)))
      (is (= :ave (:order p1)))
      (is (= {0 :name} (:filter p0)))
      (is (= [1 2] (:project p0)))
      (is (= {0 :age} (:filter p1)))
      (is (= [1 2] (:project p1)))
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

(deftest plan-chain-intermediate-permute-test
  (testing "the third join needs the intermediate result re-permuted"
    (let [p (dbsp/plan '{:find [?a ?d]
                         :where [[?a :r ?b]
                                 [?b :s ?c]
                                 [?c :t ?d]]})
          join (:where-plan p)]
      (is (= :join (:kind join)))
      (is (= 3 (count (:inputs join))))
      (is (= 2 (count (:steps join))))
      (is (nil? (:left-permute (nth (:steps join) 0))))
      (is (= [2 0 1] (:left-permute (nth (:steps join) 1))))
      (is (= '[?c ?b ?a ?d] (:result-vars p)))
      (is (= [2 3] (:final-permute p))))))

(deftest plan-triangle-test
  (testing "a cyclic query joins the closing pattern on a two-variable key"
    (let [p (dbsp/plan '{:find [?a]
                         :where [[?a :r ?b]
                                 [?b :s ?c]
                                 [?c :t ?a]]})
          join (:where-plan p)
          closing (last (:steps join))]
      (is (= :join (:kind join)))
      (is (= 2 (:key-arity closing)))
      (is (= 2 (count (set (:key-vars closing))))))))

(deftest plan-or-only-test
  (testing "single :or block as the only pattern"
    (let [p (dbsp/plan '{:find [?e]
                         :where [(or [?e :sex :male]
                                     [?e :sex :female])]})
          pat (:where-plan p)]
      (is (= :union (:kind pat)))
      (is (= '[?e] (:out-vars pat)))
      (is (= 2 (count (:branches pat))))
      (is (every? #(= '[?e] (:out-vars %)) (:branches pat)))
      (is (every? #(= :triple (:kind %)) (:branches pat)))
      (is (= '[?e] (:result-vars p))))))

(deftest plan-or-with-outer-join-test
  (testing ":or joined with an outer triple — outer chain forces target order"
    (let [p (dbsp/plan '{:find [name]
                         :where [[?e :name name]
                                 (or [?e :sex :male]
                                     [?e :sex :female])]})
          join (:where-plan p)
          [outer or-pat] (:inputs join)]
      (is (= :join (:kind join)))
      (is (= :triple (:kind outer)))
      (is (= :union (:kind or-pat)))
      (is (= '[?e] (:out-vars or-pat)))
      (is (every? #(= '[?e] (:out-vars %)) (:branches or-pat)))
      (is (= 1 (count (:steps join)))))))

(deftest plan-or-multi-var-test
  (testing "2-var :or joined with an outer pattern that re-orders branch columns"
    (let [p (dbsp/plan '{:find [n]
                         :where [[?p :tag "x"]
                                 (or [?p :name n]
                                     [?p :age n])]})
          join (:where-plan p)
          [_outer or-pat] (:inputs join)]
      (is (= :join (:kind join)))
      (is (= :union (:kind or-pat)))
      ;; the outer chain leads with ?p, so each branch is planned with target [?p n]
      (is (= '[?p n] (:out-vars or-pat)))
      (is (every? #(= '[?p n] (:out-vars %)) (:branches or-pat))))))

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
  (testing ":and branch inside :or lowers to an existing :join relation"
    (let [p (dbsp/plan '{:find [name]
                         :where [[?e :name name]
                                 (or [?e :last-name name]
                                     (and [?e :sex :male]
                                          [?e :name name]))]})
          join (:where-plan p)
          [_outer or-pat] (:inputs join)
          and-branch (second (:branches or-pat))]
      (is (= :union (:kind or-pat)))
      (is (= :join (:kind and-branch)))
      (is (= '[?e name] (:out-vars and-branch)))
      (is (= '[?e name] (:out-vars or-pat)))
      (is (= '[?e name] (:right-vars (first (:steps join)))))))

  (testing ":and branch is finally permuted when its natural join order differs"
    (let [p (dbsp/plan '{:find [?x ?y]
                         :where [(or [?x :edge ?y]
                                     (and [?y :name ?x]
                                          [?y :edge ?x]))]})
          or-pat (:where-plan p)
          and-branch (second (:branches or-pat))]
      (is (= :join (:kind and-branch)))
      (is (= '[?x ?y] (:out-vars and-branch)))
      (is (= [1 0] (:final-permute and-branch))))))

(deftest plan-nested-or-inside-and-test
  (testing "nested :or inside an :and branch keeps branch output aligned"
    (let [p (dbsp/plan '{:find [?e]
                         :where [(or (and [?e :sex :male]
                                          (or [?e :name "Ivan"]
                                              [?e :name "Bob"]))
                                     [?e :sex :female])]})
          or-pat (:where-plan p)
          and-branch (first (:branches or-pat))
          nested-or (second (:inputs and-branch))]
      (is (= :union (:kind or-pat)))
      (is (= :join (:kind and-branch)))
      (is (= :union (:kind nested-or)))
      (is (= '[?e] (:out-vars and-branch)))
      (is (= '[?e] (:out-vars nested-or))))))

(deftest plan-not-test
  (testing "not plans as anti-semijoin difference after the positive relation"
    (let [p (dbsp/plan '{:find [name]
                         :where [[?e :name name]
                                 (not [?e :last-name "Smith"])]})
          diff (:where-plan p)]
      (is (= :difference (:kind diff)))
      (is (= '[?e name] (:result-vars p)))
      (is (= '[?e name] (:out-vars diff)))
      (is (= '[?e] (:key-vars diff)))
      (is (= :triple (:kind (:positive diff))))
      (is (= :triple (:kind (:negative diff))))))

  (testing "not keeps the positive side as-is when the anti key is already leading"
    (let [p (dbsp/plan '{:find [name ?e]
                         :where [[?e :name name]
                                 (not [?e :last-name "Smith"])]})
          diff (:where-plan p)]
      (is (= :difference (:kind diff)))
      (is (= '[?e] (:key-vars diff)))
      (is (not (contains? diff :positive-permute)))
      (is (= '[?e name] (:out-vars diff)))))

  (testing "not records the keyed positive shape when the anti key is not leading"
    (let [p (dbsp/plan '{:find [name city]
                         :where [[?e :name name]
                                 [name :city city]
                                 (not [?e :last-name "Smith"])]})
          diff (:where-plan p)]
      (is (= :difference (:kind diff)))
      (is (= '[?e] (:key-vars diff)))
      (is (= '[name ?e city] (:out-vars diff)))
      (is (= '[?e name city] (:keyed-vars diff)))
      (is (not (contains? diff :positive-permute)))))

  (testing "bare not branch inside or parses but is rejected by planning"
    (is (thrown? clojure.lang.ExceptionInfo
                 (dbsp/plan '{:find [?e]
                              :where [[?e :name n]
                                      (or [?e :name "Ada"]
                                          (not [?e :name "Bob"]))]})))))

(deftest plan-or-deterministic-test
  (let [q '{:find [?e]
            :where [[?e :name name]
                    (or [?e :sex :male]
                        [?e :sex :female])]}]
    (is (= (dbsp/plan q) (dbsp/plan q)))))

(deftest plan-deterministic-test
  (let [q '{:find [?a ?d]
            :where [[?a :r ?b] [?b :s ?c] [?c :t ?d]]}]
    (is (= (dbsp/plan q) (dbsp/plan q)))))

;; --------------------------------------------------------------------------
;; Circuit assembly
;; --------------------------------------------------------------------------

(defn- assemble [query]
  (dbsp/plan->circuit (dbsp/plan query)))

(deftest assemble-single-pattern-test
  (let [{:keys [circuit inputs output]} (assemble '{:find [name]
                                                    :where [[?e :name name]]})]
    (is (= 1 (count inputs)))
    (is (some? output))
    ;; input -> filter(attribute constant) -> map(project) -> map(final projection)
    (is (= ["input" "filter-constants" "permute" "permute"]
           (vec (.operatorNames circuit))))))

(deftest assemble-constant-pattern-test
  (let [{:keys [circuit]} (assemble '{:find [?e] :where [[?e :name "Ivan"]]})]
    ;; input -> filter -> map(project) -> map(final)
    (is (= ["input" "filter-constants" "permute" "permute"]
           (vec (.operatorNames circuit))))))

(deftest assemble-three-pattern-chain-test
  (let [{:keys [circuit inputs]} (assemble '{:find [?a ?d]
                                             :where [[?a :r ?b]
                                                     [?b :s ?c]
                                                     [?c :t ?d]]})]
    (is (= 3 (count inputs)))
    ;; 3x (input, filter, permute); join1 (no intermediate map); join2
    ;; preceded by a permute; final permute.
    (is (= ["input" "filter-constants" "permute"
            "input" "filter-constants" "permute"
            "input" "filter-constants" "permute"
            "incremental-join" "permute" "incremental-join" "permute"]
           (vec (.operatorNames circuit))))
    (is (= 13 (.getNodeCount circuit)))))

(deftest assemble-leaves-test
  (testing "plan->circuit returns a :leaves vector parallel to :inputs"
    (let [{:keys [inputs leaves]} (assemble '{:find [name]
                                              :where [[?e :name name]]})]
      (is (= 1 (count leaves)))
      (is (= (count inputs) (count leaves)))
      (is (= :aev (:order (first leaves))))))

  (testing "each leaf carries :order; aev/ave mixed chain"
    (let [{:keys [inputs leaves]} (assemble '{:find [?e ?p]
                                              :where [[?e :name name]
                                                      [?p :age name]]})]
      (is (= 2 (count inputs)))
      (is (= 2 (count leaves)))
      (is (every? #{:aev :ave} (map :order leaves)))))

  (testing "leaf count equals total triple count in a chain"
    (let [{:keys [inputs leaves]} (assemble '{:find [?a ?d]
                                              :where [[?a :r ?b]
                                                      [?b :s ?c]
                                                      [?c :t ?d]]})]
      (is (= 3 (count inputs)))
      (is (= 3 (count leaves)))
      (is (every? :order leaves)))))

(deftest assemble-or-single-branch-test
  (testing "single-branch :or — no plus, just distinct after the projection"
    (let [{:keys [circuit inputs leaves]} (assemble '{:find [?e]
                                                      :where [(or [?e :name "Ada"])]})]
      (is (= 1 (count inputs)))
      (is (= 1 (count leaves)))
      ;; branch: input -> filter -> permute; then distinct on the union; then final permute
      (is (= ["input" "filter-constants" "permute" "distinct" "permute"]
             (vec (.operatorNames circuit)))))))

(deftest assemble-or-two-branch-test
  (testing "two-branch :or — one plus, one distinct"
    (let [{:keys [circuit inputs leaves]} (assemble '{:find [?e]
                                                      :where [(or [?e :sex :male]
                                                                  [?e :sex :female])]})]
      (is (= 2 (count inputs)))
      (is (= 2 (count leaves)))
      (is (= ["input" "filter-constants" "permute"
              "input" "filter-constants" "permute"
              "plus" "distinct" "permute"]
             (vec (.operatorNames circuit)))))))

(deftest assemble-or-k-branch-test
  (testing "k-branch :or has (k - 1) plus operators and one distinct"
    (let [{:keys [circuit inputs]} (assemble '{:find [?e]
                                               :where [(or [?e :sex :male]
                                                           [?e :sex :female]
                                                           [?e :sex :other])]})
          ops (vec (.operatorNames circuit))]
      (is (= 3 (count inputs)))
      (is (= 2 (count (filter #(= "plus" %) ops))))
      (is (= 1 (count (filter #(= "distinct" %) ops)))))))

(deftest assemble-or-with-outer-join-test
  (testing ":or joined with an outer triple wires through incremental-join"
    (let [{:keys [circuit inputs]} (assemble '{:find [name]
                                               :where [[?e :name name]
                                                       (or [?e :sex :male]
                                                           [?e :sex :female])]})
          ops (vec (.operatorNames circuit))]
      (is (= 3 (count inputs)))
      (is (= 1 (count (filter #(= "incremental-join" %) ops))))
      (is (= 1 (count (filter #(= "plus" %) ops))))
      (is (= 1 (count (filter #(= "distinct" %) ops)))))))

(deftest assemble-nested-or-test
  (testing "nested :or yields one plus + one distinct per :or node"
    (let [{:keys [circuit inputs leaves]} (assemble
                                           '{:find [?e]
                                             :where [(or [?e :name "Ada"]
                                                         (or [?e :name "Bob"]
                                                             [?e :name "Carla"]))]})
          ops (vec (.operatorNames circuit))]
      (is (= 3 (count inputs)))
      (is (= 3 (count leaves)))
      ;; two :or nodes -> two plus, two distinct
      (is (= 2 (count (filter #(= "plus" %) ops))))
      (is (= 2 (count (filter #(= "distinct" %) ops)))))))

(deftest assemble-and-inside-or-test
  (testing ":and inside :or contributes its leaf triples and uses existing join"
    (let [{:keys [circuit inputs leaves]} (assemble
                                           '{:find [?e]
                                             :where [(or [?e :sex :female]
                                                         (and [?e :sex :male]
                                                              [?e :name "Ivan"]))]})
          ops (vec (.operatorNames circuit))]
      (is (= 3 (count inputs)))
      (is (= 3 (count leaves)))
      (is (= 1 (count (filter #(= "incremental-join" %) ops))))
      (is (= 1 (count (filter #(= "plus" %) ops))))
      (is (= 1 (count (filter #(= "distinct" %) ops)))))))

(deftest assemble-not-test
  (testing "not assembles as distinct negative keys, semijoin, and difference"
    (let [{:keys [circuit inputs leaves]} (assemble
                                           '{:find [name]
                                             :where [[?e :name name]
                                                     (not [?e :last-name "Smith"])]})
          ops (vec (.operatorNames circuit))]
      (is (= 2 (count inputs)))
      (is (= 2 (count leaves)))
      (is (= 1 (count (filter #(= "distinct" %) ops))))
      (is (= 1 (count (filter #(= "incremental-join" %) ops))))
      (is (= 1 (count (filter #(= "difference" %) ops)))))))

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

(deftest e2e-cardinality-many-duplicate-add-is-noop-test
  (let [node fix/*node*]
    (h/transact node [[:db/add 1 :edge 2]])
    (let [iq (h/q-inc node '{:find [?to] :where [[1 :edge ?to]]})]
      (h/transact node [[:db/add 1 :edge 2]])
      (is (nil? (h/consume-delta! iq)))
      (h/transact node [[:db/retract 1 :edge 2]])
      (is (= #{[[2] -1]} (set (h/consume-delta! iq)))))))

(deftest e2e-unregister-standard-query-test
  (let [node fix/*node*
        iq (h/q-inc node '{:find [name] :where [[?e :name name]]})]
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
