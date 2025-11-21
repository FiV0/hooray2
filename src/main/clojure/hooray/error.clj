(ns hooray.error)

(defn unsupported-ex
  ([] (unsupported-ex "Unsupported operation!"))
  ([msg] (unsupported-ex msg :db.error/unsupported))
  ([msg db-error]
   (throw (ex-info msg {:cognitect.anomalies/category :cognitect.anomalies/unsupported,
                        :cognitect.anomalies/message msg,
                        :db/error db-error}))))

(defn incorrect-ex
  ([] (incorrect-ex "Incorrect operation!"))
  ([msg] (incorrect-ex msg :db.error/incorrect))
  ([msg db-error]
   (throw (ex-info msg {:cognitect.anomalies/category :cognitect.anomalies/incorrect,
                        :cognitect.anomalies/message msg,
                        :db/error db-error}))))
