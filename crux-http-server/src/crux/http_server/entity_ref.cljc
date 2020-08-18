(ns crux.http-server.entity-ref
  (:require #?@ (:clj [[cognitect.transit :as transit]]
                 :cljs [[tick.alpha.api :as tick]
                        [goog.string :as string]]))
  #?(:clj (:import java.net.URLEncoder
                   java.util.Date
                   java.io.Writer)))

(defrecord EntityRef [eid])

(defn EntityRef->url [entity-ref {:keys [valid-time transaction-time]}]
  (let [eid (pr-str (:eid entity-ref))
        encoded-eid #?(:clj (URLEncoder/encode eid "UTF-8")
                       :cljs (string/urlEncode eid))
        vt #?(:clj (some-> ^Date valid-time .toInstant)
              :cljs (some-> valid-time tick/instant))
        tt #?(:clj (some-> ^Date transaction-time .toInstant)
              :cljs (some-> transaction-time tick/instant))
        query-params (cond-> (str "?eid=" encoded-eid)
                       vt (str "&valid-time=" vt)
                       tt (str "&transaction-time=" tt))]
    (str "/_crux/entity" query-params)))

#? (:clj
    (defmethod print-method EntityRef [ref ^Writer w]
      (.write w "#crux.http/entity-ref ")
      (print-method (:eid ref) w)))

#? (:clj
    (def ref-write-handler
      (transit/write-handler "crux.http/entity-ref" #(:eid %))))

#? (:clj
    (def ref-read-handler
      (transit/read-handler ->EntityRef)))
