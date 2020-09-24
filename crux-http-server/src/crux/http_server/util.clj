(ns crux.http-server.util
  (:require [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [cognitect.transit :as transit]
            [crux.api :as api]
            [crux.codec :as c]
            [crux.io :as cio]
            [hiccup2.core :as hiccup2]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mfc]
            [spec-tools.core :as st])
  (:import [crux.api ICruxAPI ICruxDatasource]
           crux.codec.Id
           [java.io ByteArrayOutputStream OutputStream]
           [java.net URLDecoder URLEncoder]
           java.time.format.DateTimeFormatter
           java.util.Date))

(defn try-decode-edn [edn]
  (try
    (cond->> edn
      (string? edn) (edn/read-string {:readers {'crux/id c/id-edn-reader}}))
    (catch Exception e
      ::s/invalid)))

(s/def ::eid
  (st/spec
   {:spec c/valid-id?
    :decode/string (fn [_ eid] (try-decode-edn eid))}))

(s/def ::link-entities? boolean?)
(s/def ::valid-time inst?)
(s/def ::transaction-time inst?)
(s/def ::timeout int?)
(s/def ::tx-id int?)

(def ^DateTimeFormatter default-date-formatter
  (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSS"))

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
      (encode-to-output-stream [_ data _]
        (fn [^OutputStream output-stream]
          (transit/write
           (transit/writer output-stream :json options) data)
          (.flush output-stream))))))

(def default-muuntaja-options
  (-> m/default-options
      (update :formats select-keys ["application/edn"])
      (assoc :default-format "application/edn")
      (m/install {:name "application/transit+json"
                  :encoder [->tj-encoder]})))

(def default-muuntaja
  (m/create default-muuntaja-options))

(def output-stream-muuntaja
  (m/create (assoc default-muuntaja-options :return :output-stream)))

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

(defn raw-html [{:keys [body title options results]}]
  (let [latest-completed-tx (api/latest-completed-tx (:crux-node options))]
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
            (when options [:meta {:title "options" :content (pr-str {:node-options (:node-options options)
                                                                     :latest-completed-tx latest-completed-tx})}])
            (when results [:meta {:title "results" :content (str results)}])
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
              [:b (when-let [label (get-in options [:node-options :server-label])] label)]]
             [:div.header__links
              [:a.header__link {:href "https://opencrux.com/reference/get-started.html" :target "_blank"} "Documentation"]
              [:a.header__link {:href "https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux" :target "_blank"} "Zulip Chat"]
              [:a.header__link {:href "mailto:crux@juxt.pro" :target "_blank"} "Email Support"]
              #_[:div.header-dropdown
               [:button.header-dropdown__button
                "Placeholder"
                [:i.fa.fa-caret-down]]
               [:div.header-dropdown__links
                [:a "Placeholder"]]]]]
            [:div.console
             [:div#app
              [:div.container.page-pane body]]]
            [:script {:src "/cljs-out/dev-main.js" :type "text/javascript"}]]]))))

(defn entity-link [eid {:keys [valid-time transaction-time]}]
  (let [encoded-eid (URLEncoder/encode (pr-str eid) "UTF-8")
        query-params (format "?eid=%s&valid-time=%s&transaction-time=%s"
                             encoded-eid
                             (.toInstant ^Date valid-time)
                             (.toInstant ^Date transaction-time))]
    (str "/entity" query-params)))
