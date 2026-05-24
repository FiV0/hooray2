(ns hooray.subscription-test
  (:require [clojure.core.async :as async]
            [clojure.test :as t :refer [deftest is testing]]
            [hooray.core :as h]
            [hooray.fixtures :as fix]))

(t/use-fixtures :each fix/with-each-dbsp-version fix/with-node fix/with-people-schema)

(def names-query
  '{:find [name]
    :where [[e :name name]]})

(defn- take-with-timeout [ch]
  (let [timeout (async/timeout 1000)
        [value port] (async/alts!! [ch timeout])]
    (if (= port timeout)
      ::timeout
      value)))

(defn- is-delta [expected actual]
  (is (not= ::timeout actual))
  (when-not (= ::timeout actual)
    (is (= expected (set actual)))))

(deftest incremental-subscription-apis-produce-deltas
  (testing "open-deltas"
    (with-open [deltas (h/open-deltas fix/*node* names-query)]
      (h/transact fix/*node* [{:db/id :ivan :name "Ivan"}])
      (is-delta #{[["Ivan"] 1]} (h/take! deltas))))

  (testing "delta-chan"
    (let [delta-ch (h/delta-chan fix/*node* names-query)]
      (try
        (h/transact fix/*node* [{:db/id :petr :name "Petr"}])
        (is-delta #{[["Petr"] 1]} (take-with-timeout delta-ch))
        (finally
          (async/close! delta-ch)))))

  (testing "subscribe"
    (let [delta (promise)]
      (with-open [_subscription (h/subscribe fix/*node* names-query #(deliver delta %))]
        (h/transact fix/*node* [{:db/id :anna :name "Anna"}])
        (is-delta #{[["Anna"] 1]} (deref delta 1000 ::timeout))))))

(deftest subscribe-callback-failures-do-not-break-transact
  (let [delta (promise)]
    (with-open [_failing-subscription (h/subscribe fix/*node* names-query
                                                   (fn [_delta]
                                                     (throw (ex-info "boom" {}))))
                _working-subscription (h/subscribe fix/*node* names-query
                                                   #(deliver delta %))]
      (is (nil? (h/transact fix/*node* [{:db/id :ivan :name "Ivan"}])))
      (is-delta #{[["Ivan"] 1]} (deref delta 1000 ::timeout)))))


(comment
  ;; README examples

  (def node (h/connect {:type :mem :storage :hash :algo :generic}))
  (h/transact node [{:db/id -100
                     :db/ident :name
                     :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one}])

  (def names-query
    '{:find [name]
      :where [[e :name name]]})

  ;; Pull deltas explicitly.
  (with-open [deltas (h/open-deltas node names-query)]
    (h/transact node [{:db/id :ivan :name "Ivan"}])
    (h/take! deltas))
  ;; => ([["Ivan"] 1])

  ;; Read deltas from a core.async channel.
  (defn- take-with-timeout [ch]
    (let [timeout (async/timeout 1000)
          [value port] (async/alts!! [ch timeout])]
      (if (= port timeout)
        ::timeout
        value)))

  (let [delta-ch (h/delta-chan node names-query)]
    (try
      (h/transact node [{:db/id :petr :name "Petr"}])
      (take-with-timeout delta-ch)
      (finally
        (async/close! delta-ch))))
  ;; => ([["Petr"] 1])

  ;; Receive deltas in a callback.
  (with-out-str
    (with-open [_subscription (h/subscribe node names-query println)]
      (h/transact node [{:db/id :anna :name "Anna"}])))
  ;; => "([[Anna] 1])\n"
  )
