(ns hooray.zset
  (:require [clojure.core-print]
            [clojure.set])
  (:import (org.hooray.incremental ZSet IndexedZSet IntegerWeight)))

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

(comment
  (-> (IndexedZSet/singleton "foo" (ZSet/singleton "bar" (IntegerWeight/ONE)) IntegerWeight/ZERO IntegerWeight/ONE)
      indexed-zset->result-set)
  )
