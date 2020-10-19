(ns crux.http-server.util
  (:require [camel-snake-kebab.core :as csk]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.spec.alpha :as s]
            [cognitect.transit :as transit]
            [crux.api :as api]
            [crux.codec :as c]
            [crux.http-server.entity-ref :as entity-ref]
            [crux.io :as cio]
            [hiccup2.core :as hiccup2]
            [jsonista.core :as j]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mfc]
            [muuntaja.format.edn :as mfe]
            [muuntaja.format.transit :as mft]
            [spec-tools.core :as st])
  (:import (com.fasterxml.jackson.core JsonGenerator JsonParser)
           com.fasterxml.jackson.databind.DeserializationContext
           com.fasterxml.jackson.databind.module.SimpleModule
           com.fasterxml.jackson.databind.deser.std.StdDeserializer
           [crux.api ICruxAPI ICruxDatasource]
           crux.codec.Id
           crux.http_server.entity_ref.EntityRef
           clojure.lang.PersistentList
           [java.io ByteArrayOutputStream OutputStream]
           java.time.format.DateTimeFormatter
           jsonista.jackson.PersistentHashMapDeserializer))

(def ^:private crux-jackson-module
  (doto (SimpleModule. "Crux")
    (.addDeserializer java.util.Map
                      (let [jsonista-deser (PersistentHashMapDeserializer.)]
                        (proxy [StdDeserializer] [java.util.Map]
                          (deserialize [^JsonParser p ^DeserializationContext ctxt]
                            (let [res (.deserialize jsonista-deser p ctxt)]
                              (or (when-let [oid (:$oid res)]
                                    (c/new-id (c/hex->id-buffer oid)))
                                  (when-let [b64 (:$base64 res)]
                                    (c/base64-reader b64))
                                  res))))))))

(def crux-object-mapper
  (j/object-mapper
   {:encode-key-fn true
    :encoders {Id (fn [crux-id ^JsonGenerator gen]
                    (.writeObject gen {"$oid" (str crux-id)}))
               (Class/forName "[B") (fn [^bytes bytes ^JsonGenerator gen]
                                      (.writeObject gen {"$base64" (c/base64-writer bytes)}))
               EntityRef entity-ref/ref-json-encoder
               PersistentList (fn [pl ^JsonGenerator gen]
                                (.writeString gen (pr-str pl)))}
    :decode-key-fn true
    :modules [crux-jackson-module]}))

(defn try-decode-edn [edn]
  (try
    (cond-> edn
      (string? edn) c/read-edn-string-with-readers)
    (catch Exception _e
      ::s/invalid)))

(defn try-decode-json [json]
  (try
    (cond-> json
      (string? json) (j/read-value crux-object-mapper))
    (catch Exception _e
      ::s/invalid)))

(s/def ::eid (and string? c/valid-id?))

(s/def ::eid-edn
  (st/spec
   {:spec c/valid-id?
    :decode/string (fn [_ eid] (try-decode-edn eid))}))

(s/def ::eid-json
  (st/spec
   {:spec c/valid-id?
    :decode/string (fn [_ json] (try-decode-json json))}))

(s/def ::link-entities? boolean?)
(s/def ::valid-time inst?)
(s/def ::transaction-time inst?)
(s/def ::timeout int?)
(s/def ::tx-id int?)

(def ^DateTimeFormatter default-date-formatter
  (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSS"))

(defn ->edn-encoder [_]
  (reify
    mfc/EncodeToBytes
    (encode-to-bytes [_ data _]
      (.getBytes (pr-str data) "UTF-8"))
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [^Cursor results] :as data} _]
      (fn [^OutputStream output-stream]
        (with-open [w (io/writer output-stream)]
          (try
            (if results
              (print-method (or (iterator-seq results) '()) w)
              (.write w ^String (pr-str data)))
            (finally
              (cio/try-close results))))))))

(def crux-id-write-handler
  (transit/write-handler "crux.codec/id" #(str %)))

(defn- ->tj-encoder [_]
  (let [options {:handlers {Id crux-id-write-handler}}]
    (reify
      mfc/EncodeToBytes
      (encode-to-bytes [_ data _]
        (let [baos (ByteArrayOutputStream.)
              writer (transit/writer baos :json options)]
          (transit/write writer data)
          (.toByteArray baos)))
      mfc/EncodeToOutputStream
      (encode-to-output-stream [_ {:keys [^Cursor results] :as data} _]
        (fn [^OutputStream output-stream]
          (let [writer (transit/writer output-stream :json options)]
            (try
              (if results
                (transit/write writer (or (iterator-seq results) '()))
                (transit/write writer data))
              (finally
                (cio/try-close results)))))))))

(defn camel-case-keys [m]
  (into {} (map (fn [[k v]] [(csk/->camelCaseKeyword k) v]) m)))

(defn ->json-encoder [json-encode-fn options]
  (reify
    mfc/EncodeToBytes
    (encode-to-bytes [_ data _]
      (j/write-value-as-bytes (json-encode-fn data) crux-object-mapper))
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [^Cursor results] :as data} _]
      (fn [^OutputStream output-stream]
        (try
          (if results
            (let [results-seq (iterator-seq results)]
              (j/write-value output-stream (or (some-> results-seq json-encode-fn) '()) crux-object-mapper))
            (j/write-value output-stream data crux-object-mapper))
          (finally
            (cio/try-close results)))))))

(defn ->default-muuntaja
  ([]
   (->default-muuntaja camel-case-keys))
  ([json-encode-fn]
   (-> m/default-options
       (dissoc :formats)
       (assoc :default-format "application/edn")
       (m/install {:name "application/transit+json"
                   :encoder [->tj-encoder]
                   :decoder [(partial mft/decoder :json)]})
       (m/install {:name "application/edn"
                   :encoder [->edn-encoder]
                   :decoder [mfe/decoder]})
       (m/install {:name "application/json"
                   :encoder [(partial ->json-encoder json-encode-fn)]}))))

(defn db-for-request ^ICruxDatasource [^ICruxAPI crux-node {:keys [valid-time transact-time]}]
  (cond
    (and valid-time transact-time)
    (.db crux-node valid-time transact-time)

    valid-time
    (.db crux-node valid-time)

    ;; TODO: This could also be an error, depending how you see it,
    ;; not supported via the Java API itself.
    transact-time
    (.db crux-node (cio/next-monotonic-date) transact-time)

    :else
    (.db crux-node)))

(defn raw-html [{:keys [title crux-node http-options results]}]
  (let [latest-completed-tx (api/latest-completed-tx crux-node)]
    (str (hiccup2/html
          [:html
           {:lang "en"}
           [:head
            [:meta {:charset "utf-8"}]
            [:meta {:http-equiv "X-UA-Compatible" :content "IE=edge,chrome=1"}]
            [:meta
             {:name "viewport"
              :content "width=device-width, initial-scale=1.0, maximum-scale=1.0"}]
            [:link {:rel "icon" :href "/favicon.ico" :type "image/x-icon"}]
            [:meta {:title "options"
                    :content (pr-str {:http-options http-options
                                      :latest-completed-tx latest-completed-tx})}]
            (when results
              [:meta {:title "results", :content (pr-str results)}])
            [:link {:rel "stylesheet" :href "/css/all.css"}]
            [:link {:rel "stylesheet" :href "/latofonts.css"}]
            [:link {:rel "stylesheet" :href "/css/table.css"}]
            [:link {:rel "stylesheet" :href "/css/react-datetime.css"}]
            [:link {:rel "stylesheet" :href "/css/codemirror.css"}]
            [:link {:rel "stylesheet"
                    :href "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.12.1/css/all.min.css"}]
            [:title "Crux Console"]]
           [:body
            [:nav.header
             [:div.crux-logo
              [:a {:href "/_crux/query"}
               [:img.crux-logo__img {:src "/crux-horizontal-bw.svg.png" }]]]
             [:span.mobile-hidden
              [:b (:server-label http-options)]]
             [:div.header__links
              [:a.header__link {:href "https://opencrux.com/reference/get-started.html" :target "_blank"} "Documentation"]
              [:a.header__link {:href "https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux" :target "_blank"} "Zulip Chat"]
              [:a.header__link {:href "mailto:crux@juxt.pro" :target "_blank"} "Email Support"]]]
            [:div.console
             [:div#app
              [:noscript
               [:pre.noscript-content (with-out-str (pp/pprint results))]]]]
            [:script {:src "/cljs-out/dev-main.js" :type "text/javascript"}]]]))))
