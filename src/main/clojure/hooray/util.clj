(ns hooray.util
  (:import (java.util.concurrent Callable)
           (java.util.function Function BiFunction)
           (kotlin.jvm.functions Function0 Function1 Function2)))

(defn create-update-in
  "Creates a function that behaves like `clojure.core/update-in` but defaults to `default-map`
  for empty maps. Any collections left empty by the operation will be dissociated from their containing structures."
  [default-map]
  (let [m-or-default #(or % default-map)]
    (fn custom-update-in [m ks f & args]
      (let [up (fn up [m ks f args]
                 (let [[k & ks] ks
                       v (if ks (up (get m k) ks f args) (apply f (get m k) args))]
                   (if (and (coll? v) (empty? v))
                     (dissoc (m-or-default m) k)
                     (assoc (m-or-default m) k v))))]
        (up (m-or-default m) ks f args)))))

(comment
  (def sorted-update-in (create-update-in (sorted-map)))
  (def m (sorted-update-in (sorted-map) [1 2] (fnil conj (sorted-set)) 3))
  (sorted? (m 1))

  (def m2 (sorted-update-in m [1 2] disj 3))
  (sorted? m2))

(defn ->closure[f]
  (reify Function0
    (invoke [_this]
      (f))))

(defn ->function [f]
  (reify Function1
    (invoke [_this arg]
      (f arg))))

(defn ->bifunction [f]
  (reify Function2
    (invoke [_this arg1 arg2]
      (f arg1 arg2))))
