(ns hooray.fixtures
  (:require [hooray.core :as h]
            [hooray.test-util :as tu]
            [hooray.graph-gen :as g]
            [clojure.tools.logging :as log]))

(def ^:dynamic *opts* {:type :mem :storage :hash :algo :generic})
(def ^:dynamic *node* nil)

(defn with-node
  ([f] (with-node *opts* f))
  ([opts f]
   (binding [*node* (h/connect opts)]
     (f))))

(defn with-edge-attribute [f]
  (h/transact *node* [g/edge-attribute])
  (f))

(defn with-timing [f]
  (let [start-time-ms (System/currentTimeMillis)
        ret (try
              (f)
              (catch Exception e
                (log/error e "caught exception during")
                {:error (str e)}))]
    (merge (when (map? ret) ret)
           {:time-taken-ms (- (System/currentTimeMillis) start-time-ms)})))

(defmacro with-timing* [& body]
  `(with-timing (fn [] ~@body)))

(defn with-timing-logged [f]
  (let [{:keys [time-taken-ms]} (with-timing f)]
    (log/infof "Test took %s" (tu/format-time time-taken-ms))))

(defmacro with-timing-logged* [& body]
  `(let [{:keys [~'time-taken-ms]} (with-timing (fn [] ~@body))]
     (log/infof "Test took %s" (tu/format-time ~'time-taken-ms))))
