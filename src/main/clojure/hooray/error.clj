(ns hooray.error)

(defn unsupported-ex
  ([] (unsupported-ex "Unsupported operation!"))
  ([msg] (unsupported-ex msg {} ))
  ([msg extra-data] (unsupported-ex msg extra-data :db.error/unsupported))
  ([msg extra-data db-error]
   (throw (ex-info msg (into extra-data {:cognitect.anomalies/category :cognitect.anomalies/unsupported,
                                         :cognitect.anomalies/message msg,
                                         :db/error db-error})))))

(defn incorrect-ex
  ([] (incorrect-ex "Incorrect operation!"))
  ([msg] (incorrect-ex msg {}))
  ([msg extra-data] (incorrect-ex msg extra-data :db.error/incorrect))
  ([msg extra-data db-error]
   (throw (ex-info msg (into extra-data {:cognitect.anomalies/category :cognitect.anomalies/incorrect,
                                         :cognitect.anomalies/message msg,
                                         :db/error db-error})))))
