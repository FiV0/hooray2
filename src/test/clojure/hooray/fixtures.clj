(ns hooray.fixtures
  (:require [hooray.core :as h]))

(def ^:dynamic *opts* {:type :mem :storage :hash :algo :generic})
(def ^:dynamic *node* nil)

(defn with-node
  ([f] (with-node *opts* f))
  ([opts f]
   (binding [*node* (h/connect opts)]
     (f))))
