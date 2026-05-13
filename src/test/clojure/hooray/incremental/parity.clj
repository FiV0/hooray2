(ns hooray.incremental.parity
  "Test helpers for running the same incremental-query scenario under
   both *circuit-version* :legacy and :stream."
  (:require [clojure.test :as t]
            [hooray.incremental :as inc]))

(defmacro deftest-both
  "Like clojure.test/deftest, but emits two tests — `<name>-legacy`
   and `<name>-stream` — that bind *circuit-version* accordingly.

   Use the metadata key `:skip-stream true` on a deftest-both form to
   skip the :stream variant when the new pipeline is known to be
   incomplete for that scenario. The :legacy variant always runs."
  [name & body]
  (let [m (meta name)
        skip-stream? (:skip-stream m)
        legacy-name (with-meta (symbol (str name "-legacy"))
                               (dissoc m :skip-stream))
        stream-name (with-meta (symbol (str name "-stream"))
                               (dissoc m :skip-stream))]
    `(do
       (t/deftest ~legacy-name
         (binding [inc/*circuit-version* :legacy]
           ~@body))
       ~(when-not skip-stream?
          `(t/deftest ~stream-name
             (binding [inc/*circuit-version* :stream]
               ~@body))))))
