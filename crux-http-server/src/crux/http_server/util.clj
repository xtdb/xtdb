(ns crux.http-server.util
  (:require [crux.io :as cio]
            [crux.api :as api]
            [cognitect.transit :as transit]
            [hiccup2.core :as hiccup2])
  (:import [crux.api ICruxAPI ICruxDatasource]
           java.time.format.DateTimeFormatter
           java.net.URLEncoder
           java.util.Date))

(def ^DateTimeFormatter default-date-formatter
  (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSS"))

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
    (str "/_crux/entity" query-params)))
