(ns juxt.crux-ui.frontend.logic.time
  (:require [goog.string :as gs]))

(defn format-iso-8601--utc [^js/Date d]
  (str ""  (.getUTCFullYear d)
       "-" (gs/padNumber (inc (.getUTCMonth d)) 2)
       "-" (gs/padNumber (.getUTCDate d) 2)
       "T" (gs/padNumber (.getUTCHours d) 2)
       ":" (gs/padNumber (.getUTCMinutes d) 2)
       ":" (gs/padNumber (.getUTCSeconds d) 2)
       "." (gs/padNumber (.getUTCMilliseconds d) 3)
       "Z"))

(defn format-for-dt-local [^js/Date d]
  (str ""  (.getFullYear d)
       "-" (gs/padNumber (inc (.getMonth d)) 2)
       "-" (gs/padNumber (.getDate d) 2)
       "T" (gs/padNumber (.getHours d) 2)
       ":" (gs/padNumber (.getMinutes d) 2)))

(def user-time-zone
  (.. js/Intl (^js DateTimeFormat) (^js resolvedOptions) ^js -timeZone))


(defn date->comps [^js/Date t]
  {:time/year   (.getFullYear t)
   :time/month  (inc (.getMonth t))
   :time/date   (.getDate t)
   :time/hour   (.getHours t)
   :time/minute (.getMinutes t)})

(defn comps->date [comps]
  (let [date-str (str (:time/year comps) "-"
                      (gs/padNumber (:time/month comps) 2) "-"
                      (gs/padNumber (:time/date comps) 2) " "
                      (gs/padNumber (:time/hour comps) 2)  ":"
                      (gs/padNumber (:time/minute comps) 2))
        ts (js/Date.parse date-str)]
    (if-not (js/isNaN ts)
      (js/Date. ts))))

(comment
  (let [d (js/Date.)]
    (date->comps d)
    (comps->date (date->comps d))))
