(ns xtdb.http-server.util
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.spec.alpha :as s]
            [cognitect.transit :as transit]
            [crux.api :as api]
            [crux.codec :as c]
            [xtdb.http-server.json :as http-json]
            [crux.io :as cio]
            [juxt.clojars-mirrors.hiccup.v2v0v0-alpha2.hiccup2.core :as hiccup2]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.core :as m]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.format.core :as mfc]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.format.edn :as mfe]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.format.transit :as mft]
            [juxt.clojars-mirrors.spec-tools.v0v10v5.spec-tools.core :as st]
            [xtdb.http-server.entity-ref :as entity-ref]
            [clojure.instant :as inst])
  (:import [crux.api ICruxDatasource]
           [crux.codec EDNId Id]
           xtdb.http_server.entity_ref.EntityRef
           [java.io ByteArrayOutputStream OutputStream]
           (java.util Date Map)))

(s/def ::eid (and string? c/valid-id?))

(s/def ::date
  (st/spec {:spec #(instance? Date %)
            :decode/string (fn [_ d] (inst/read-instant-date d))}))

(defn try-decode-edn [edn]
  (try
    (cond-> edn
      (string? edn) c/read-edn-string-with-readers)
    (catch Exception _e
      ::s/invalid)))

(s/def ::eid-edn
  (st/spec
   {:spec c/valid-id?
    :description "EDN formatted entity ID"
    :decode/string (fn [_ eid] (try-decode-edn eid))}))

(s/def ::eid-json
  (st/spec
   {:spec c/valid-id?
    :description "JSON formatted entity ID"
    :decode/string (fn [_ json] (http-json/try-decode-json json))}))

(s/def ::link-entities? boolean?)
(s/def ::valid-time ::date)
(s/def ::tx-time ::date)
(s/def ::timeout int?)
(s/def ::tx-id int?)

(defn ->edn-encoder [_]
  (reify
    mfc/EncodeToBytes
    (encode-to-bytes [_ data _]
      (binding [*print-length* nil, *print-level* nil]
        (.getBytes (pr-str data) "UTF-8")))

    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [^Cursor results] :as data} _]
      (fn [^OutputStream output-stream]
        (binding [*print-length* nil, *print-level* nil]
          (with-open [w (io/writer output-stream)]
            (try
              (if results
                (print-method (or (iterator-seq results) '()) w)
                (.write w ^String (pr-str data)))
              (finally
                (cio/try-close results)))))))))

(def tj-write-handlers
  {EntityRef entity-ref/ref-write-handler
   Id (transit/write-handler "xt/oid" str)
   EDNId (transit/write-handler "xt/oid" str)
   (Class/forName "[B") (transit/write-handler "xt/base64" c/base64-writer)})

(defn ->tj-encoder [_]
  (let [options {:handlers tj-write-handlers}]
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

(defn ->default-muuntaja
  ([] (->default-muuntaja {}))

  ([opts]
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
                   :encoder [http-json/->json-encoder opts]}))))

(defn db-for-request ^ICruxDatasource [crux-node {:keys [valid-time tx-time tx-id]}]
  (let [^Map db-basis {:xt/valid-time valid-time
                       :xt/tx-time tx-time
                       :xt/tx-id tx-id}]
    (api/db crux-node db-basis)))

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
              [:a {:href "/_xtdb/query"}
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
