(ns hooray.util)

(defn create-update-in
  "Creates a function that behaves like `clojure.core/update-in` but defaults to `default-map`
  for empty maps."
  [default-map]
  (let [m-or-default #(or % default-map)]
    (fn custom-update-in [m ks f & args]
      (let [up (fn up [m ks f args]
                 (let [[k & ks] ks
                       v (if ks (up (get m k) ks f args) (apply f (get m k) args))]
                   (assoc (m-or-default m) k v)))]
        (up (m-or-default m) ks f args)))))

(comment
  (def sorted-update-in (create-update-in (sorted-map)))
  (def m (sorted-update-in (sorted-map) [1 2] (fnil conj (sorted-set)) 3))
  (sorted? (m 1)))
