(ns hooray.dbsp-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [hooray.core :as h]
   [hooray.dbsp :as dbsp])
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
  (testing "repeated variables inside one triple pattern are rejected"
    (is (thrown? clojure.lang.ExceptionInfo
                 (dbsp/parse '{:find [?x] :where [[?x :edge ?x]]})))))

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
    (is (= 1 (count (:patterns p))))
    (is (= [] (:joins p)))
    (is (= '[?e name] (:result-vars p)))
    (is (= [1] (:final-permute p)))
    (let [pat (first (:patterns p))]
      (is (= :aev (:order pat)))
      (is (= {0 :name} (:filter pat)))
      (is (= [1 2] (:project pat)))
      (is (= '[?e name] (:out-vars pat))))))

(deftest plan-two-pattern-join-test
  (let [p (dbsp/plan '{:find [name age]
                       :where [[?e :name name]
                               [?e :age age]]})]
    (is (= '[?e name age] (:result-vars p)))
    (is (= [1 2] (:final-permute p)))
    (is (= 1 (count (:joins p))))
    (let [j (first (:joins p))]
      (is (= 1 (:key-arity j)))
      (is (= '[?e] (:key-vars j)))
      (is (nil? (:left-permute j)))
      (is (= '[?e name age] (:out-vars j))))))

(deftest plan-ave-order-test
  (testing "a pattern joining on its value column is fed in :ave order"
    (let [p (dbsp/plan '{:find [?e ?p]
                         :where [[?e :name name]
                                 [?p :age name]]})]
      (is (= :ave (:order (nth (:patterns p) 0))))
      (is (= :ave (:order (nth (:patterns p) 1))))
      (is (= {0 :name} (:filter (nth (:patterns p) 0))))
      (is (= [1 2] (:project (nth (:patterns p) 0))))
      (is (= {0 :age} (:filter (nth (:patterns p) 1))))
      (is (= [1 2] (:project (nth (:patterns p) 1))))
      (is (= '[name ?e ?p] (:result-vars p))))))

(deftest plan-constant-filter-test
  (testing "a constant value column becomes a Filter and is projected away"
    (let [p (dbsp/plan '{:find [?e] :where [[?e :name "Ivan"]]})
          pat (first (:patterns p))]
      (is (= {0 :name, 2 "Ivan"} (:filter pat)))
      (is (= [1] (:project pat)))
      (is (= '[?e] (:out-vars pat)))))
  (testing "a constant entity column becomes a Filter and is projected away"
    (let [p (dbsp/plan '{:find [name] :where [[1 :name name]]})
          pat (first (:patterns p))]
      (is (= {0 :name, 1 1} (:filter pat)))
      (is (= [2] (:project pat)))
      (is (= '[name] (:out-vars pat))))))

(deftest plan-chain-intermediate-permute-test
  (testing "the third join needs the intermediate result re-permuted"
    (let [p (dbsp/plan '{:find [?a ?d]
                         :where [[?a :r ?b]
                                 [?b :s ?c]
                                 [?c :t ?d]]})]
      (is (= 2 (count (:joins p))))
      (is (nil? (:left-permute (nth (:joins p) 0))))
      (is (= [2 0 1] (:left-permute (nth (:joins p) 1))))
      (is (= '[?c ?b ?a ?d] (:result-vars p)))
      (is (= [2 3] (:final-permute p))))))

(deftest plan-triangle-test
  (testing "a cyclic query joins the closing pattern on a two-variable key"
    (let [p (dbsp/plan '{:find [?a]
                         :where [[?a :r ?b]
                                 [?b :s ?c]
                                 [?c :t ?a]]})
          closing (last (:joins p))]
      (is (= 2 (:key-arity closing)))
      (is (= 2 (count (set (:key-vars closing))))))))

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
  (let [node (fresh-node)]
    (h/transact node [{:db/id 1 :name "Ivan"}])
    (let [db-before (h/db node)
          iq (dbsp/compile-query db-before '{:find [name] :where [[1 :name name]]})
          delta (dbsp/compute-delta! iq db-before [{:db/id 1 :name "Ivanov"}])]
      (is (= #{[["Ivan"] -1] [["Ivanov"] 1]} (set delta))))))

(deftest compute-delta-two-pattern-join-test
  (let [node (fresh-node)]
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
  (let [node (fresh-node)]
    (h/transact node [{:db/id 1 :name "Ivan"}])
    (let [db-before (h/db node)
          iq (dbsp/compile-query db-before '{:find [name] :where [[1 :name name]]})]
      (dbsp/compute-delta! iq db-before [{:db/id 1 :name "Ivanov"}])
      (is (= #{[["Ivan"] -1] [["Ivanov"] 1]} (set (dbsp/pop-result! iq))))
      (is (nil? (dbsp/pop-result! iq))))))

;; --------------------------------------------------------------------------
;; End-to-end via q-inc / transact / consume-delta!
;; --------------------------------------------------------------------------

(defn- standard-q-inc [node query]
  (binding [h/*dbsp-version* :standard]
    (h/q-inc node query)))

(deftest e2e-single-pattern-test
  (let [node (fresh-node)
        iq (standard-q-inc node '{:find [name] :where [[1 :name name]]})]
    (h/transact node [{:db/id 1 :name "Ivan"}])
    (is (= #{[["Ivan"] 1]} (set (h/consume-delta! iq))))))

(deftest e2e-update-test
  (let [node (fresh-node)]
    (h/transact node [{:db/id 1 :name "Ivan"}])
    (let [iq (standard-q-inc node '{:find [name] :where [[1 :name name]]})]
      (h/transact node [{:db/id 1 :name "Ivanov"}])
      (is (= #{[["Ivan"] -1] [["Ivanov"] 1]} (set (h/consume-delta! iq)))))))

(deftest e2e-two-pattern-join-test
  (let [node (fresh-node)]
    (h/transact node [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}])
    (let [iq (standard-q-inc node '{:find [name last-name]
                                    :where [[e :name name]
                                            [e :last-name last-name]]})]
      (h/transact node [{:db/id :petr :name "Petr" :last-name "Petrov"}])
      (is (= #{[["Petr" "Petrov"] 1]} (set (h/consume-delta! iq)))))))

(deftest e2e-self-join-test
  (let [node (fresh-node)
        iq (standard-q-inc node '{:find [?a ?b]
                                  :where [[?a :name n]
                                          [?b :name n]]})]
    (h/transact node [{:db/id 1 :name "Ivan"} {:db/id 2 :name "Ivan"}])
    ;; every ordered pair of entities sharing the name, including reflexive
    (is (= #{[[1 1] 1] [[1 2] 1] [[2 1] 1] [[2 2] 1]}
           (set (h/consume-delta! iq))))))

(deftest e2e-three-pattern-chain-test
  (let [node (fresh-node)
        iq (standard-q-inc node '{:find [?a ?d]
                                  :where [[?a :edge ?b]
                                          [?b :edge ?c]
                                          [?c :edge ?d]]})]
    ;; path n1 -> n2 -> n3 -> n4
    (h/transact node [{:db/id :n1 :edge :n2}
                      {:db/id :n2 :edge :n3}
                      {:db/id :n3 :edge :n4}])
    (is (= #{[[:n1 :n4] 1]} (set (h/consume-delta! iq))))))

(deftest e2e-triangle-test
  (let [node (fresh-node)
        iq (standard-q-inc node '{:find [?a ?b ?c]
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
  (let [node (fresh-node)
        iq (standard-q-inc node '{:find [n c]
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
  (let [node (fresh-node)]
    (h/transact node [[:db/add 1 :edge 2]])
    (let [iq (standard-q-inc node '{:find [?to] :where [[1 :edge ?to]]})]
      (h/transact node [[:db/add 1 :edge 2]])
      (is (nil? (h/consume-delta! iq)))
      (h/transact node [[:db/retract 1 :edge 2]])
      (is (= #{[[2] -1]} (set (h/consume-delta! iq)))))))

(deftest e2e-unregister-standard-query-test
  (let [node (fresh-node)
        iq (standard-q-inc node '{:find [name] :where [[?e :name name]]})]
    (h/transact node [{:db/id 1 :name "Ivan"}])
    (is (= #{[["Ivan"] 1]} (set (h/consume-delta! iq))))
    (h/unregister-inc-q node iq)
    (h/transact node [{:db/id 2 :name "Petr"}])
    (is (nil? (h/consume-delta! iq)))))

(deftest e2e-rejects-unsupported-query-options-test
  (let [node (fresh-node)]
    (is (thrown? clojure.lang.ExceptionInfo
                 (standard-q-inc node '{:find [name]
                                        :in [name]
                                        :where [[?e :name name]]})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (standard-q-inc node '{:find [name]
                                        :in []
                                        :where [[?e :name name]]})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (standard-q-inc node '{:find [name]
                                        :keys [name]
                                        :where [[?e :name name]]})))))

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
