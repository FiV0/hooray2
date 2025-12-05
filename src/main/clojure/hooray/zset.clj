(ns hooray.zset
  (:refer-clojure :exclude [update-keys update-vals])
  (:require [clojure.core-print]
            [clojure.set])
  (:import (org.hooray.incremental ZSet IndexedZSet IntegerWeight)))

(defn ->int-weight [i] (IntegerWeight. i))

(defn add [^IntegerWeight w1 ^IntegerWeight w2]
  (.add w1 w2))

(defn sub [^IntegerWeight w1 ^IntegerWeight w2]
  (.add w1 (.negate w2)))

(defn mul [^IntegerWeight w1 ^IntegerWeight w2]
  (.multiply w1 w2))

(def empty-zset (ZSet/empty))

(def empty-indexed-zset (IndexedZSet/empty))

(comment
  (require '[hooray.util :as util])
  (def eav empty-indexed-zset)

  (def zset-update-in (util/create-update-in empty-indexed-zset))

  (def indexed-zset (zset-update-in eav [1 2] (fnil assoc empty-zset) 3 (->int-weight 5)))
  (-> indexed-zset
      (zset-update-in [1 2] (fnil update empty-zset) 3 (fnil add IntegerWeight/ZERO) (->int-weight 5))
      (zset-update-in [1 2] (fnil update empty-zset) 2 (fnil add IntegerWeight/ZERO) (->int-weight 5))
      (zset-update-in [:foo :bar] (fnil update empty-zset) 2 (fnil add IntegerWeight/ZERO) (->int-weight 5))))


(defmethod print-method ZSet [z ^java.io.Writer w]
  (@#'clojure.core/print-meta z w)
  (@#'clojure.core/print-prefix-map "#zset" z (var clojure.core/pr-on) w))

(defmethod print-method IndexedZSet [z ^java.io.Writer w]
  (@#'clojure.core/print-meta z w)
  (@#'clojure.core/print-prefix-map "#indexed-zset" z (var clojure.core/pr-on) w))

(defmethod print-dup ZSet [z w]
  (print-method z w))

(defmethod print-dup IndexedZSet [z w]
  (print-method z w))

(defmethod print-method IntegerWeight [z ^java.io.Writer w]
  (.write w "IntegerWeight(")
  (.write w (str "value=" (.getValue z)))
  (.write w ")"))

(defmethod print-dup IntegerWeight [z w]
  (print-method z w))

(defn unwrap-weight [^IntegerWeight w]
  (.getValue w))

(defn indexed-zset->result-set [^IndexedZSet zset]
  (for [entry (.entries (.toFlatZSet zset))]
    [(key entry) (unwrap-weight (val entry))]))

(defn update-keys
  "Like clojure.core/update-keys, but for ZSets"
  [zset f]
  (reduce-kv (fn [acc k v] (assoc acc (f k) v))
             (if (instance? ZSet zset)
               empty-zset
               empty-indexed-zset)
             zset))

(defn update-vals
  "Like clojure.core/update-vals, but for ZSets"
  [zset f]
  (reduce-kv (fn [acc k v] (assoc acc k (f v)))
             (if (instance? ZSet zset)
               empty-zset
               empty-indexed-zset)
             zset))

(comment
  (-> (IndexedZSet/singleton "foo" (ZSet/singleton "bar" (IntegerWeight/ONE)) IntegerWeight/ZERO IntegerWeight/ONE)
      indexed-zset->result-set)

  (-> (ZSet/singleton 1 (IntegerWeight/ONE))
      (update-vals #(mul % (->int-weight 5))))

  (-> (IndexedZSet/singleton "foo" (ZSet/singleton "bar" (IntegerWeight/ONE)) IntegerWeight/ZERO IntegerWeight/ONE)
      (update-keys clojure.string/upper-case))
  )
