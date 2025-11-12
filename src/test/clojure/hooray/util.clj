(ns hooray.util
  (:import (java.util.concurrent TimeUnit)))

(defonce year (* (.. TimeUnit/DAYS (toMillis 1)) 365))
(defonce day (.. TimeUnit/DAYS (toMillis 1)))
(defonce hour (.. TimeUnit/HOURS (toMillis 1)))
(defonce minute (.. TimeUnit/MINUTES (toMillis 1)))
(defonce sec (.. TimeUnit/SECONDS (toMillis 1)))

(defn format-time
  "Formats the given timestamp in human readable format."
  ([time-ms] (format-time "" time-ms))
  ([format-str time-ms]
   (cond
     (>= time-ms year)
     (format-time (str format-str " " (quot time-ms year) "years")
                  (mod time-ms year))

     (>= time-ms day)
     (format-time (str format-str " " (quot time-ms day) "days")
                  (mod time-ms day))

     (>= time-ms hour)
     (format-time (str format-str " " (quot time-ms hour) "hours")
                  (mod time-ms hour))

     (>= time-ms minute)
     (format-time (str format-str " " (quot time-ms minute) "minutes")
                  (mod time-ms minute))

     (>= time-ms sec)
     (format-time (str format-str " " (quot time-ms sec) "seconds")
                  (mod time-ms sec))

     :else
     (str format-str " " time-ms  "milliseconds"))))
