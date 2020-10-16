(ns crux.http-server.util
  (:require [clojure.edn :as edn]
            [clojure.pprint :as pp]
            [clojure.spec.alpha :as s]
            [clojure.walk :as walk]
            [clojure.java.io :as io]
            [cognitect.transit :as transit]
            [crux.api :as api]
            [crux.codec :as c]
            [crux.io :as cio]
            [hiccup2.core :as hiccup2]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mfc]
            [muuntaja.format.transit :as mft]
            [muuntaja.format.edn :as mfe]
            [spec-tools.core :as st]
            [jsonista.core :as j]
            [camel-snake-kebab.core :as csk]
            [crux.http-server.entity-ref :as entity-ref]
            [clojure.set :as set]
            [crux.tx.conform :as txc])
  (:import [crux.api ICruxAPI ICruxDatasource]
           crux.codec.Id
           crux.http_server.entity_ref.EntityRef
           [java.io ByteArrayOutputStream OutputStream]
           [java.net URLDecoder URLEncoder]
           java.time.format.DateTimeFormatter
           java.util.Date
           com.fasterxml.jackson.databind.ObjectMapper
           com.fasterxml.jackson.core.JsonGenerator))

(defn try-decode-edn [edn]
  (try
    (cond->> edn
      (string? edn) (edn/read-string {:readers {'crux/id c/id-edn-reader}}))
    (catch Exception e
      ::s/invalid)))

(defn try-decode-json [json]
  (try
    (cond->> json
      (string? json) j/read-value)
    (catch Exception e
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

(def crux-object-mapper
  (j/object-mapper
   {:encode-key-fn (fn [k]
                     (cond
                       (= k :crux.db/id) "_id"
                       (= k :crux.db/fn) "_fn"
                       :else (str (symbol k))))
    :encoders {Id (fn [crux-id ^JsonGenerator gen] (.writeString gen (str crux-id)))
               EntityRef entity-ref/ref-json-encoder}}))

(defn camel-case-keys [m]
  (into {} (map (fn [[k v]] [(csk/->camelCaseKeyword k) v]) m)))

(defn transform-doc [doc]
  (cio/update-if doc :crux.db/fn pr-str))

(defn transform-tx-op [tx-op]
  (binding [txc/*xform-doc* transform-doc]
    (-> tx-op
        (txc/conform-tx-op)
        (txc/->tx-op)
        (update 0 name))))

(defn ->json-encoder [transform-fn options]
  (let [object-mapper crux-object-mapper]
    (reify
      mfc/EncodeToBytes
      (encode-to-bytes [_ data _]
        (j/write-value-as-bytes (transform-fn data) object-mapper))
      mfc/EncodeToOutputStream
      (encode-to-output-stream [_ {:keys [^Cursor results] :as data} _]
        (fn [^OutputStream output-stream]
          (try
            (if results
              (let [results-seq (iterator-seq results)]
                (j/write-value output-stream (or (some-> results-seq transform-fn) '()) object-mapper))
              (j/write-value output-stream data object-mapper))
            (finally
              (cio/try-close results))))))))

(defn ->default-muuntaja
  ([]
   (->default-muuntaja camel-case-keys))
  ([transform-fn]
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
                   :encoder [(partial ->json-encoder transform-fn)]}))))

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
