(ns crux.http-console
  (:require
   [crux.api :as crux]
   [crux.codec :as c]
   [hiccup.page :as hiccup])
  (:import [crux.api ICruxAPI]))

(defn- coerce-param
  ([param]
   (coerce-param param nil))
  ([param wrap?]
   (let [formatted-param
         (cond
           (vector? param) (str "[" (reduce str param) "]")
           wrap? (str "[" param "]")
           :else param)]
     (c/read-edn-string-with-readers formatted-param))))

(defn- build-query
  [{:strs [find where args order-by limit offset full-results]}]
  (cond-> {}
    :find (assoc :find (coerce-param find))
    :where (assoc :where (coerce-param where :wrap))
    args (assoc :args (coerce-param args))
    order-by (assoc :order-by (coerce-param args :wrap))
    limit (assoc :limit (Integer/parseInt limit))
    offset (assoc :offset (Integer/parseInt offset))
    full-results (assoc :full-results? true)))

(comment
  (let [query-params {"find" "[?id ?e]"
                      "where" ["[?id :crux.db/id _]" "[?e :name n]"]
                      "args" "[{'n \"Ivan\"}]"}]
    (build-query query-params)))

(defn link-entities
  [crux-node vt tt path result]
  (let [metadata (atom {})
        resolved-links
        (fn recur-on-result [result]
          (if (and (c/valid-id? result)
                   (crux/entity (crux/db crux-node vt tt) result))
            (let [query-params (cond-> "?"
                                 vt (str "valid-time=" vt "&")
                                 tt (str "transact-time=" tt))]
              (swap! metadata assoc result (str path "/" result query-params))
              result)
            (cond
              (map? result) (reduce-kv #(assoc %1 %2 (recur-on-result %3)) {} result)
              (seq? result) (map recur-on-result result)
              (vector? result) (mapv recur-on-result result)
              (set? result) (into #{} (map recur-on-result result))
              :else result)))]
    (with-meta (resolved-links result) @metadata)))

#_(comment
  ;; submit some documents
  (crux/submit-tx crux-node
                  [[:crux.tx/put
                    {:crux.db/id :some-id1
                     :name "LDA"}]
                   [:crux.tx/put
                    {:crux.db/id :some-id3
                     :name "DAN"}]
                   [:crux.tx/put
                    {:crux.db/id {:map :id}
                     :name "Lda"}]])

  (meta
   (link-entities crux-node nil nil "/_entity"
                        #{[:some-id1
                           {:example {:ref {:map :id}}}
                           {:example [{:ref {:map :id}}
                                      {:map :id} :some-id2]}
                           "string"]
                          [:some-id3]})))

(defn resolve-headers
  [{:keys [find full-results?] :as query} results]
  (if (and full-results? (= 1 (count (first results))))
    (->> results
         (mapv #(map keys %))
         (flatten)
         (into #{}))
    find))

(comment
  (let [query '{:find [?id ?name]
                :full-results? true}
        results #{[{:crux.db/id :id
                    :name "lda"
                    :hello "dan"}
                   "mal"]}]
    (resolve-headers query results)))

(comment
  (let [query '{:find [?id ?name]
                :full-results? true}
        results #{[{:crux.db/id :id
                    :name "lda"
                    :hello "dan"}]}]
    (resolve-headers query results)))

(defn resolve-rows
  [{:keys [find full-results?] :as query} results]
  (if (and full-results? (= 1 (count (first results))))
    (map first results)
    (map #(zipmap find %) results)))

(comment
  (let [query '{:find [?id ?name]
                :full-results? true}
        results #{[{:crux.db/id :id
                    :name "lda"}]}]
    (resolve-rows query results)))

(comment
  (let [query '{:find [?id ?name]}
        results #{[{:crux.db/id :id
                    :name "lda"}]
                  [{:crux.db/id :id2
                    :name "lda2"}]}]
    (resolve-rows query results)))

(comment
  (let [query '{:find [?id ?name]
                :full-results? true}
        results #{[{:crux.db/id :id
                    :name "lda"} "hello"]
                  [{:crux.db/id :id
                    :name "lda"} "something"]}]
    (resolve-rows query results)))

(defn html-resource
  [crux-node query]
  (let [results (->> query
                     (crux/q (crux/db crux-node))
                     (link-entities crux-node nil nil "/_entity"))
        headers (resolve-headers query results)
        rows (resolve-rows query results)]
    (let [metadata (meta results)]
      (hiccup/html5
       [:html
        {:lang "en"}
        [:head
         [:meta {:charset "utf-8"}]
         [:meta {:http-equiv "X-UA-Compatible" :content "IE=edge,chrome=1"}]
         [:meta
          {:name "viewport"
           :content "width=device-width, initial-scale=1.0, maximum-scale=1.0"}]
         [:link {:rel "icon" :href "/favicon.ico" :type "image/x-icon"}]]
        [:body
         (if (seq rows)
           [:table
            [:thead
             [:tr
              (for [header headers]
                [:th header])]]
            [:tbody
             (for [row rows]
               [:tr
                (for [header headers]
                  (let [cell-value (get row header)]
                    [:td
                     (if-let [href (get metadata cell-value)]
                       [:a {:href href}
                        cell-value]
                       cell-value)]))])]]
           [:div "No results found"])]]))))

(defn edn-resource
  [crux-node query]
  (crux/q
   (crux/db crux-node)
   query))

(defn query [^ICruxAPI crux-node request]
  (let [content-type (:content-type request)
        query (build-query (:query-params request))]
    (case content-type
      "application/edn" {:status 200
                         :content-type "application/edn"
                         :body (str (edn-resource crux-node query))}
      ("text/html" nil) {:status 200
                         :content-type "text/html"
                         :body (str (html-resource crux-node query))})))

(defn gen-group
  [movie director actor]
  (let [director-uuid (java.util.UUID/randomUUID)
        actor-uuid (java.util.UUID/randomUUID)]
    [[:crux.tx/put
      {:crux.db/id (java.util.UUID/randomUUID)
       :name (:name movie)
       :year (:year movie)
       :runtime (:runtime movie)
       :genre (:genre movie)
       :director director-uuid
       :lead-actor actor-uuid}]
     [:crux.tx/put
      {:crux.db/id director-uuid
       :role (:role director)
       :name (:name director)
       :lastname (:lastname director)
       :age (:age director)}]
     [:crux.tx/put
      {:crux.db/id actor-uuid
       :role (:role actor)
       :name (:name actor)
       :lastname (:lastname actor)
       :age (:age actor)}]]))

(defn seed-db
  [crux-node]
  (crux/submit-tx
   crux-node
   (concat
    (gen-group {:name "Titanic"
                :year "1997"
                :runtime "197 min"
                :genre "dramatic, romance"}
               {:name "James"
                :lastname "Cameron"
                :age 67
                :role "Director"}
               {:name "Leonardo"
                :lastname "Di Caprio"
                :age 45
                :role "Actor"})
    (gen-group {:name "The Lion King"
                :year "1994"
                :runtime "88 min"
                :genre "animation, adventure"}
               {:name "Roger"
                :lastname "Allers"
                :age 74
                :role "Director"}
               {:name "Francis"
                :lastname "Glebas"
                :age 53
                :role "Actor"})
    (gen-group {:name "Matrix"
                :year "1999"
                :runtime "136 min"
                :genre "action, sci-fi"}
               {:name "Lana"
                :lastname "Wachowski"
                :age "51"
                :role "Director"}
               {:name "Keanu"
                :lastname "Reeves"
                :age 48
                :role "Actor"}))))
