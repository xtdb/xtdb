(ns crux.http-server.entity-ref
  (:require #?@ (:clj [[cognitect.transit :as transit]]
                 :cljs [[tick.alpha.api :as tick]
                        [goog.string :as string]]))
  #?(:clj (:import java.net.URLEncoder
                   java.util.Date
                   java.io.Writer
                   com.fasterxml.jackson.core.JsonGenerator)))

(defrecord EntityRef [eid])

(defn EntityRef->url [entity-ref {:keys [valid-time tx-id tx-time]}]
  (let [eid (pr-str (:eid entity-ref))
        encoded-eid #?(:clj (URLEncoder/encode eid "UTF-8")
                       :cljs (string/urlEncode eid))
        vt #?(:clj (some-> ^Date valid-time .toInstant)
              :cljs (some-> valid-time tick/instant))
        query-params (cond-> (str "?eid-edn=" encoded-eid)
                       vt (str "&valid-time=" vt)
                       tx-id (str "&tx-id=" tx-id)
                       tx-time (str "&tx-time=" tx-time))]
    (str "/_crux/entity" query-params)))

#? (:clj
    (defmethod print-method EntityRef [ref ^Writer w]
      (.write w "#xt.http/entity-ref ")
      (print-method (:eid ref) w)))

#? (:clj
    (def ref-write-handler
      (transit/write-handler "xt.http/entity-ref" #(:eid %))))

#? (:clj
    (def ref-read-handler
      (transit/read-handler ->EntityRef)))

#? (:clj
    (defn ref-json-encoder [^EntityRef entity-ref ^JsonGenerator gen]
      (.writeString gen (str (:eid entity-ref)))))
