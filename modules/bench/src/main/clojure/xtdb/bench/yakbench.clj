(ns xtdb.bench.yakbench
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.test.check.random]
            [clojure.tools.logging :as log]
            [taoensso.tufte :as tufte :refer [p]]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.bench.stats :as stats]
            [xtdb.bench.random :as random]
            [xtdb.compactor :as c]
            [xtdb.datasets.yakbench :as yakbench]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(defn ?s [n]
  (str "(" (str/join ", " (repeat n "?")) ")"))

(defn get-conn ^java.sql.Connection [^xtdb.api.DataSource node]
  (.build (.createConnectionBuilder node)))

(def active-inst (.toInstant #inst "2025-01-01T00:00:00Z"))
(def feed-sync-inst (.toInstant #inst "2025-09-29T00:00:00Z"))
(def recent-inst (.toInstant #inst "2025-09-01T00:00:00Z"))
(def recent-bookmarks-inst (.toInstant #inst "2024-09-01T00:00:00Z"))
(def recent-ad-inst (.toInstant #inst "2025-01-01T00:00:00Z"))

(defn benchmarks-queries
  [active-user-ids biggest-feed]
  [{:id :active-users-by-joined-at
    :f #(xt/q % ["select _id from users where user$joined_at > ?"
                 active-inst])}
   {:id :active-users-by-viewed-at
    :f #(xt/q % ["select distinct user_item$user from user_items where user_item$viewed_at > ?"
                 active-inst])}
   {:id :active-users-by-ad-updated
    :f #(xt/q % ["select ad$user from ads where ad$updated_at > ?"
                 active-inst])}
   {:id :active-users-by-ad-clicked
    :f #(xt/q % ["select distinct ad.click$user from ad_clicks where ad.click$created_at > ?"
                 active-inst])}
   {:id :favorited-urls
    :f #(xt/q % [(str "select item$url "
                      "from items "
                      "join user_items on user_item$item = items._id "
                      "where user_item$favorited_at is not null")])}

   {:id :direct-urls
    :f #(xt/q % ["select item$url from items where item$doc_type = 'item/direct'"])}

   {:id :unique-ad-clicks
    :f #(xt/q % [(str "select ad$click$ad, count(distinct ad$click$user) "
                      "from ad_clicks "
                      "group by ad$click$ad")])}

   {:id :latest-ad-clicks
    :f #(xt/q % [(str "select ad$click$ad, max(ad$click$created_at) "
                      "from ad_clicks "
                      "group by ad$click$ad")])}

   {:id :charge-amounts-by-status
    :f #(xt/q % [(str "select ad$credit$ad, ad$credit$charge_status, sum(ad$credit$amount) "
                      "from ad_credits "
                      "where ad$credit$charge_status is not null "
                      "group by ad$credit$ad, ad$credit$charge_status")])}

   {:id :candidate-statuses
    :f #(xt/q % [(str "select item$direct$candidate_status, count(_id) "
                      "from items "
                      "where item$direct$candidate_status is not null "
                      "group by item$direct$candidate_status")])}

   {:id :favorites
    :f #(xt/q % [(str "select user_item$user, user_item$item "
                      "from user_items "
                      "where user_item$favorited_at is not null")])}

   {:id :approved-candidates
    :f #(xt/q % [(str "select _id, item$url "
                      "from items "
                      "where item$direct$candidate_status = 'approved'")])}

   {:id :ad-recent-cost
    :f #(xt/q % [(str "select ad$click$ad, sum(ad$click$cost) "
                      "from ad_clicks "
                      "where ad$click$created_at > ? "
                      "group by ad$click$ad")
                 recent-ad-inst])}

   {:id :ads-clicked-at
    :f #(xt/q % [(str "select ad$click$ad, ad$click$user, max(ad$click$created_at) "
                      "from ad_clicks "
                      "group by ad$click$ad, ad$click$user")])}

   {:id :all-n-likes
    :f #(xt/q % [(str "select user_item$item, count(_id) "
                      "from user_items "
                      "where user_item$favorited_at is not null "
                      "group by user_item$item")])}
   {:id :feeds-to-sync
    :f #(xt/q %
              (vec (concat [(str "select distinct sub$feed$feed "
                                 "from subs "
                                 "join feeds on feeds._id = sub$feed$feed "
                                 "where sub$user in " (?s (count active-user-ids))
                                 " and (feed$synced_at is null or feed$synced_at < ?)")]
                           active-user-ids
                           [feed-sync-inst])))}
   {:id :existing-feed-titles
    :f #(xt/q %
              (vec
               (concat [(str "select item$title "
                             "from items "
                             "where item$feed$feed = ? "
                             "and item$title in " (?s (count (:titles biggest-feed))))
                        (:id biggest-feed)]
                       (:titles biggest-feed))))}])

(defn item-pk-lookup-queries
  [item-ids]
  (for [n [1 10 100 1000]
        :let [ids (take n item-ids)]
        :when (= (count ids) n)]
    {:id (keyword (str "items-by-pks-" n))
     :f #(xt/q % (vec (concat [(str "select _id, item$url from items where _id in " (?s n))]
                              ids)))}))

(defn user-specific-benchmarks-queries
  [{:keys [user-id user-email]}]
  [{:id :get-user-by-email
    :f #(xt/q % ["select * from users where user$email = ?" user-email])}
   {:id :get-user-by-id
    :f #(xt/q % ["select * from users where _id = ?" user-id])}
   {:id :get-user-id-by-email
    :f #(xt/q % ["select _id from users where user$email = ?" user-email])}
   {:id :get-user-email-by-id
    :f #(xt/q % ["select user$email from users where _id = ?" user-id])}
   {:id :get-feeds
    :f #(xt/q % [(str "select count(sub$feed$feed) from subs "
                      "where sub$user = ? and sub$feed$feed is not null")
                 user-id])}
   {:id :get-items
    :f #(xt/q % [(str "select count(i._id) "
                      "from subs s "
                      "join items i on i.item$feed$feed = s.sub$feed$feed "
                      "where s.sub$user = ? "
                      "and s.sub$feed$feed is not null")
                 user-id])}
   {:id :read-urls
    :f #(xt/q % [(str "select distinct i.item$author_url "
                      "from user_items ui "
                      "join items i on i._id = ui.user_item$item "
                      "where ui.user_item$user = ? "
                      "and coalesce(ui.user_item$viewed_at, ui.user_item$favorited_at, ui.user_item$skipped_at) is not null "
                      "and i.item$author_url is not null")
                 user-id])}
   {:id :email-items
    :f #(xt/q % [(str "select s._id as sub_id, i._id as item_id "
                      "from subs s "
                      "join items i on i.item$email$sub = s._id "
                      "where s.sub$user = ?")
                 user-id])}
   {:id :rss-items
    :f #(xt/q % [(str "select s._id as sub_id, i._id as item_id "
                      "from subs s "
                      "join items i on i.item$feed$feed = s.sub$feed$feed "
                      "where s.sub$user = ? and s.sub$feed$feed is not null")
                 user-id])}
   {:id :user-items
    :f #(xt/q % [(str "select user_item$item, user_item$viewed_at, user_item$favorited_at, user_item$skipped_at "
                      "from user_items where user_item$user = ?")
                 user-id])}

   {:id :recent-email-items
    :f #(xt/q % [(str "select items._id, item$ingested_at "
                      "from subs "
                      "join items on subs._id = item$email$sub "
                      "where sub$user = ? "
                      "and item$ingested_at > ?")
                 user-id
                 recent-inst])}

   {:id :recent-rss-items
    :f #(xt/q % [(str "select items._id, item$ingested_at "
                      "from subs "
                      "join items on item$feed$feed = sub$feed$feed "
                      "where sub$user = ? "
                      "and sub$feed$feed is not null "
                      "and item$ingested_at > ?")
                 user-id
                 recent-inst])}

   {:id :recent-bookmarks
    :f #(xt/q % [(str "select user_item$item, user_item$bookmarked_at "
                      "from user_items "
                      "where user_item$user = ? "
                      "and user_item$bookmarked_at > ?")
                 user-id
                 recent-bookmarks-inst])}

   {:id :subscription-status
    :f #(xt/q % [(str "select _id, sub$email$unsubscribed_at "
                      "from subs "
                      "where sub$user = ?")
                 user-id])}

   {:id :latest-emails-received-at
    :f #(xt/q % [(str "select subs._id, max(item$ingested_at) "
                      "from subs "
                      "join items on item$email$sub = subs._id "
                      "where sub$user = ? "
                      "group by subs._id")
                 user-id])}])

(defn get-max-user
  [conn]
  (let [row (first (xt/q conn [(str
                                "select u._id, u.user$email, count(ui._id) as cnt "
                                "from user_items ui "
                                "join users u on u._id = ui.user_item$user "
                                "group by u._id, u.user$email "
                                "order by cnt desc "
                                "limit 1")]))]
    {:user-id (:xt/id row)
     :user-email (:user/email row)}))

(defn get-mean-user
  [conn]
  (let [row (first (xt/q conn [(str
                                 "with per as ("
                                 "  select u._id as _id, u.user$email as user_email, count(ui._id) as cnt "
                                 "  from user_items ui "
                                 "  join users u on u._id = ui.user_item$user "
                                 "  group by u._id, u.user$email"
                                 "), stats as (select avg(cnt) as avg_cnt from per) "
                                 "select u._id, u.user$email, per.cnt "
                                 "from per join users u on u._id = per._id, stats "
                                 "order by (per.cnt - stats.avg_cnt)*(per.cnt - stats.avg_cnt) asc "
                                 "limit 1")]))]
    {:user-id (:xt/id row)
     :user-email (:user/email row)}))

(defn get-active-users
  [conn ^java.util.Random random scale-factor]
  (let [active-users (stats/round-down (* yakbench/active-user-baseline scale-factor))
        joined (xt/q conn ["select u._id as _id from users u where u.user$joined_at > ?" active-inst])
        viewed (xt/q conn ["select distinct ui.user_item$user as _id from user_items ui where ui.user_item$viewed_at > ?" active-inst])
        ad-updated (xt/q conn ["select distinct a.ad$user as _id from ads a where a.ad$updated_at > ?" active-inst])
        ids (->> (concat joined viewed ad-updated)
                 (map :xt/id)
                 (filter some?)
                 distinct
                 vec)
        ids (let [al (java.util.ArrayList. ^java.util.Collection ids)]
              (java.util.Collections/shuffle al random)
              (vec al))]
    (if (pos? active-users)
      (vec (take active-users ids))
      ids)))

(defn get-biggest-feed
  [conn random]
  (let [feed (first (xt/q conn [(str
                                 "select item$feed$feed as _id, count(*) as cnt "
                                 "from items "
                                 "group by item$feed$feed "
                                 "order by cnt desc "
                                 "limit 1")]))]
    (when feed
      (let [titles (map :item/title (xt/q conn ["select item$title from items where item$feed$feed = ? limit 10" (:xt/id feed)]))
            random-titles (repeatedly (count titles) #(random/next-string random (+ 10 (random/next-int random 10))))
            titles (concat titles random-titles)]
        {:id (:xt/id feed)
         :titles titles}))))

(defn get-distributed-items
  "Returns item data at various positions for scan benchmarking.
   Fetches 1000 spread and 1000 clustered items with _id, content_key, lang, length, published_at.
   Spread items are sampled via take-nth to maintain distribution across IID space."
  [conn]
  (let [n 1000
        cnt (:cnt (first (xt/q conn ["SELECT count(*) as cnt FROM items"])))
        step (max (quot cnt n) 1)
        mid (quot cnt 2)

        cols "_id, item$content_key, item$lang, item$length, item$published_at"

        ;; Spread - evenly distributed across the IID space
        spread-1000 (vec (xt/q conn
                               [(str "SELECT " cols " FROM ("
                                     "  SELECT " cols ", row_number() OVER (ORDER BY _id) as rn"
                                     "  FROM items"
                                     ") AS numbered WHERE (rn - 1) % ? = 0"
                                     "  ORDER BY rn"
                                     "  LIMIT ?")
                                step n]))

        ;; Clustered - consecutive from middle of IID space
        clustered-1000 (vec (xt/q conn
                                  [(str "SELECT " cols " FROM ("
                                        "  SELECT " cols ", row_number() OVER (ORDER BY _id) as rn"
                                        "  FROM items"
                                        ") AS numbered WHERE rn BETWEEN ? AND ?"
                                        "  ORDER BY rn")
                                   mid (+ mid n -1)]))]
    ;; Pre-sample for different sizes
    {:spread-10 (vec (take-nth 100 spread-1000))
     :spread-100 (vec (take-nth 10 spread-1000))
     :spread-1000 spread-1000
     :clustered-10 (vec (take 10 clustered-1000))
     :clustered-100 (vec (take 100 clustered-1000))
     :clustered-1000 clustered-1000
     :langs (vec (distinct (map :item/lang spread-1000)))}))

(defn- or-clause [col n]
  (str/join " OR " (repeat n (str col " = ?"))))

(defn scan-items-individual-benchmark-queries
  "Generates benchmark queries for individual point lookups at different IID positions."
  [{:keys [spread-10]}]
  (for [[i item] (map-indexed vector spread-10)]
    {:id (keyword (str "scan-p" (* i 10)))
     :f #(xt/q % ["SELECT _id FROM items WHERE _id = ?" (:xt/id item)])}))

(defn scan-items-set-benchmark-queries
  "Generates benchmark queries for set lookups (IN and OR) with varying sizes on _id (IID path)."
  [{:keys [spread-10 spread-100 spread-1000 clustered-10 clustered-100 clustered-1000]}]
  (let [ids #(mapv :xt/id %)]
    [{:id :scan-spread-in-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE _id IN " (?s 10))] (ids spread-10)))}
     {:id :scan-clustered-in-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE _id IN " (?s 10))] (ids clustered-10)))}
     {:id :scan-spread-or-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE " (or-clause "_id" 10))] (ids spread-10)))}
     {:id :scan-clustered-or-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE " (or-clause "_id" 10))] (ids clustered-10)))}

     {:id :scan-spread-in-100
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE _id IN " (?s 100))] (ids spread-100)))}
     {:id :scan-clustered-in-100
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE _id IN " (?s 100))] (ids clustered-100)))}

     {:id :scan-spread-in-1000
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE _id IN " (?s 1000))] (ids spread-1000)))}
     {:id :scan-clustered-in-1000
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE _id IN " (?s 1000))] (ids clustered-1000)))}]))

(defn scan-items-by-content-key-queries
  "Generates benchmark queries for IN lookups on content_key (unique non-_id UUID column)."
  [{:keys [spread-10 spread-100 spread-1000 clustered-10 clustered-100 clustered-1000]}]
  (let [ckeys #(mapv :item/content-key %)]
    [{:id :content-key-spread-in-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$content_key IN " (?s 10))] (ckeys spread-10)))}
     {:id :content-key-clustered-in-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$content_key IN " (?s 10))] (ckeys clustered-10)))}
     {:id :content-key-spread-or-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE " (or-clause "item$content_key" 10))] (ckeys spread-10)))}
     {:id :content-key-clustered-or-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE " (or-clause "item$content_key" 10))] (ckeys clustered-10)))}

     {:id :content-key-spread-in-100
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$content_key IN " (?s 100))] (ckeys spread-100)))}
     {:id :content-key-clustered-in-100
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$content_key IN " (?s 100))] (ckeys clustered-100)))}

     {:id :content-key-spread-in-1000
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$content_key IN " (?s 1000))] (ckeys spread-1000)))}
     {:id :content-key-clustered-in-1000
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$content_key IN " (?s 1000))] (ckeys clustered-1000)))}]))

(defn scan-items-filter-queries
  "Generates benchmark queries for various filter types: equality, range, sparse."
  [{:keys [spread-1000 langs]}]
  (let [lengths (mapv :item/length spread-1000)
        min-len (apply min lengths)
        max-len (apply max lengths)
        len-range (- max-len min-len)
        ;; Thresholds for > filter selecting ~10%, ~25%, ~50% of rows
        len-10pct (+ min-len (quot (* len-range 9) 10))
        len-25pct (+ min-len (quot (* len-range 3) 4))
        len-50pct (+ min-len (quot len-range 2))

        timestamps (mapv :item/published-at spread-1000)
        min-ts ^java.time.ZonedDateTime (reduce #(if (.isBefore ^java.time.ZonedDateTime %1 ^java.time.ZonedDateTime %2) %1 %2) timestamps)
        max-ts (reduce #(if (.isAfter ^java.time.ZonedDateTime %1 ^java.time.ZonedDateTime %2) %1 %2) timestamps)
        ts-range (.toMillis (java.time.Duration/between min-ts max-ts))
        ;; Selectivity targets for timestamp
        ts-10pct (.plus ^java.time.ZonedDateTime min-ts (java.time.Duration/ofMillis (quot (* ts-range 9) 10)))
        ts-50pct (.plus ^java.time.ZonedDateTime min-ts (java.time.Duration/ofMillis (quot ts-range 2)))

        n-langs (count langs)
        _ (assert (>= n-langs 8) (str "Need at least 8 distinct langs for selectivity targets, got " n-langs))
        langs-12pct (vec (take (quot n-langs 8) langs))
        langs-25pct (vec (take (quot n-langs 4) langs))
        langs-50pct (vec (take (quot n-langs 2) langs))]

    [{:id :lang-select-12pct
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$lang IN " (?s (count langs-12pct)))] langs-12pct))}
     {:id :lang-select-25pct
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$lang IN " (?s (count langs-25pct)))] langs-25pct))}
     {:id :lang-select-50pct
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$lang IN " (?s (count langs-50pct)))] langs-50pct))}

     ;; Length range filters
     {:id :length-select-10pct
      :f #(xt/q % ["SELECT _id FROM items WHERE item$length > ?" len-10pct])}
     {:id :length-select-25pct
      :f #(xt/q % ["SELECT _id FROM items WHERE item$length > ?" len-25pct])}
     {:id :length-select-50pct
      :f #(xt/q % ["SELECT _id FROM items WHERE item$length > ?" len-50pct])}

     ;; Timestamp range filters (recent items)
     {:id :published-select-10pct
      :f #(xt/q % ["SELECT _id FROM items WHERE item$published_at > ?" ts-10pct])}
     {:id :published-select-50pct
      :f #(xt/q % ["SELECT _id FROM items WHERE item$published_at > ?" ts-50pct])}

     ;; Sparse column filter (~0.1% have doc_type)
     {:id :doc-type-not-null
      :f #(xt/q % ["SELECT _id FROM items WHERE item$doc_type IS NOT NULL"])}]))

(defn profile-data
  [pstats]
  (let [rows (for [[id {:keys [n min p50 p90 p95 p99 max mean mad sum]}]
                   (get pstats :stats {})]
               {:id id
                :n n
                :min min
                :p50 p50
                :p90 p90
                :p95 p95
                :p99 p99
                :max max
                :mean mean
                :mad mad
                :sum sum})]
    (sort-by :sum > rows)))

(defn profile
  [node iterations suite]
  (with-open [conn (get-conn node)]
    (let [_throwaway (doseq [{:keys [f]} suite]
                       (f conn))
          [_ pstats] (tufte/profiled
                      {}
                      (doseq [{:keys [id f]} suite
                              _ (range iterations)]
                        (p id (f conn))))]
      {:profile (profile-data @pstats)
       :formatted (tufte/format-pstats pstats)})))

(defn stats-from-sql
  "Run a SQL that returns rows with a single count column (aliased as `cnt` by default)
   and compute distribution stats over those counts. Provide `count-key` if alias differs."
  ([conn sql]
   (stats-from-sql conn sql :cnt))
  ([conn sql count-key]
   (let [rows (xt/q conn [sql])
         counts (map count-key rows)]
     (stats/distribution-stats counts))))

(defn user-items-stats
  [conn]
  (stats-from-sql conn
                        (str "select count(*) as cnt "
                             "from user_items ui "
                             "group by ui.user_item$user")
                        :cnt))

(defn user-subs-stats
  [conn]
  (stats-from-sql conn
                        (str "select count(*) as cnt "
                             "from subs s "
                             "group by s.sub$user")
                        :cnt))

(defn feed-items-stats
  [conn]
  (stats-from-sql conn
                        (str "select count(*) as cnt "
                             "from items i "
                             "group by i.item$feed$feed")
                        :cnt))

(defn inspect
  [node {{:keys [user-id]} :max-user :keys [active-users]}]
  (with-open [conn (get-conn node)]
    (let [tables ["users" "feeds" "subs"
                  "items" "user_items" "ads"
                  "ad_credits" "ad_clicks" "digests"
                  "bulk_sends" "skips"]
          rows (for [t tables]
                 {:table t
                  :count (:xt/column-1 (first (xt/q conn [(str "select count(*) from " t)])))})]
      (println)
      (println "Table counts")
      (pp/print-table [:table :count] rows)
      (println))

    (let [queries [["subs" "select count(*) from subs where sub$user = ?" [user-id]]
                   ["items" "select count(*) from items where item$feed$feed is not null and item$feed$feed in (select sub$feed$feed from subs where sub$user = ?)" [user-id]]
                   ["user_items" "select count(*) from user_items where user_item$user = ?" [user-id]]
                   ["ad_clicks" "select count(*) from ad_clicks where ad.click$user = ?" [user-id]]
                   ["digests" "select count(*) from digests where digest$user = ?" [user-id]]
                   ["skips" "select count(*) from skips where skip$user = ?" [user-id]]
                   ["ads" "select count(*) from ads where ad$user = ?" [user-id]]
                   ["ad_credits"
                    (str "select count(c._id) \n"
                         "from ad_credits c \n"
                         "join ads a on c.ad.credit$ad = a._id \n"
                         "where a.`ad$user` = ?")
                    [user-id]]]]

      (println "Picked User" (xt/q conn ["select * from users where _id = ?" user-id]))
      (println)
      (println "Max User Counts")
      (let [rows (for [[label sql args] queries]
                   {:table label
                    :count (:xt/column-1 (first (xt/q conn (into [sql] args))))})]
        (pp/print-table [:table :count] rows))
      (println))

    (println "Active Users" (count active-users))
    (println)

    (println "User items stats")
    (pp/print-table [(user-items-stats conn)])
    (println)

    (println "User subs stats")
    (pp/print-table [(user-subs-stats conn)])
    (println)

    (println "Feed items stats")
    (pp/print-table [(feed-items-stats conn)])
    (println)))

(defmethod b/cli-flags :yakbench [_]
  [["-s" "--scale-factor SCALE_FACTOR" "Yakbench scale factor to use"
    :parse-fn parse-double
    :default 0.1]
   ["-h" "--help"]])

(defmethod b/->benchmark :yakbench [_ {:keys [scale-factor seed no-load?],
                                       :or {scale-factor 0.1, seed 0}}]
  (log/info {:scale-factor scale-factor :seed seed :no-load? no-load?})

  {:title "Yakbench"
   :benchmark-type :yakbench
   :seed seed
   :parameters {:scale-factor scale-factor :seed seed :no-load? no-load?}
   :->state #(do {:!state (atom {})})
   :tasks [(when-not no-load?
             {:t :do
              :stage :ingest
              :tasks [{:t :call
                       :stage :submit-docs
                       :f (fn [{:keys [node random]}]
                            (with-open [conn (get-conn node)]
                              (yakbench/load-data! conn random scale-factor)))}
                      {:t :call
                       :stage :await-transactions
                       :f (fn [{:keys [node]}] (b/sync-node node))}
                      {:t :call
                       :stage :flush-block
                       :f (fn [{:keys [node]}] (tu/flush-block! node))}]})

           {:t :call
            :stage :compact
            :f (fn [{:keys [node]}] (c/compact-all! node nil))}

           {:t :call
            :stage :get-query-data
            :f (fn [{:keys [node !state random]}]
                 (with-open [conn (get-conn node)]
                   (swap! !state merge {:max-user (get-max-user conn)
                                        :mean-user (get-mean-user conn)
                                        :active-users (get-active-users conn random scale-factor)
                                        :biggest-feed (get-biggest-feed conn random)
                                        :distributed-items (get-distributed-items conn)})))}

           {:t :call
            :stage :profile-global-queries
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [active-users biggest-feed]} @!state
                       {:keys [profile formatted]} (profile node 10 (benchmarks-queries active-users biggest-feed))]
                   (swap! !state update :profiles assoc :global profile)
                   (println formatted)))}

           {:t :call
            :stage :profile-max-user
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [max-user]} @!state
                       {:keys [profile formatted]} (profile node 10 (user-specific-benchmarks-queries max-user))]
                   (swap! !state update :profiles assoc :max-user profile)
                   (println formatted)))}
           {:t :call
            :stage :profile-mean-user
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [mean-user]} @!state
                       {:keys [profile formatted]} (profile node 50 (user-specific-benchmarks-queries mean-user))]
                   (swap! !state update :profiles assoc :mean-user profile)
                   (println formatted)))}
           {:t :call
            :stage :profile-scan-items-individual
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [distributed-items]} @!state
                       {:keys [profile formatted]} (profile node 1000 (scan-items-individual-benchmark-queries distributed-items))]
                   (swap! !state update :profiles assoc :scan-individual profile)
                   (println formatted)))}
           {:t :call
            :stage :profile-scan-items-sets
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [distributed-items]} @!state
                       {:keys [profile formatted]} (profile node 50 (scan-items-set-benchmark-queries distributed-items))]
                   (swap! !state update :profiles assoc :scan-sets profile)
                   (println formatted)))}
           {:t :call
            :stage :profile-scan-content-key
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [distributed-items]} @!state
                       {:keys [profile formatted]} (profile node 10 (scan-items-by-content-key-queries distributed-items))]
                   (swap! !state update :profiles assoc :scan-content-key profile)
                   (println formatted)))}
           {:t :call
            :stage :profile-scan-filters
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [distributed-items]} @!state
                       {:keys [profile formatted]} (profile node 5 (scan-items-filter-queries distributed-items))]
                   (swap! !state update :profiles assoc :scan-filters profile)
                   (println formatted)))}
           {:t :call
            :stage :inspect
            :f (fn [{:keys [node !state]}]
                 (inspect node @!state))}
           {:t :call
            :stage :output-profile-data
            :f (fn [{:keys [!state] :as worker}]
                 (let [{:keys [profiles]} @!state]
                   (b/log-report worker {:profiles profiles})))}]})

(comment
  (try
    (let [random (java.util.Random. 0)
          scale 1.0
          dir (util/->path "../../tmp/yakbench")
          clear-dir (fn [^java.nio.file.Path path]
                      (when (util/path-exists path)
                        (log/info "Clearing directory" path)
                        (util/delete-dir path)))]
      #_#_(println "Clear dir")
      (clear-dir dir)
      (with-open [node (tu/->local-node {:node-dir dir
                                         :instant-src (java.time.InstantSource/system)})
                  conn (get-conn node)]
        #_#_#_#_#_#_(println "Load data")
        (yakbench/load-data! conn random scale)
        (println "Flush block")
        (b/sync-node node)
        (tu/flush-block! node)
        (println "Compact")
        (c/compact-all! node nil)
        (println "Get Query Data")
        (let [max-user (get-max-user conn)
              mean-user (get-mean-user conn)
              active-users (get-active-users conn random scale)
              biggest-feed (get-biggest-feed conn random)
              distributed-items (get-distributed-items conn)]

          #_#_#_#_#_#_#_#_#_(println "Profile Global")
          (println (:formatted (profile node 10 (benchmarks-queries active-users biggest-feed))))
          (println "Profile Max User")
          (println (:formatted (profile node 10 (user-specific-benchmarks-queries max-user))))
          (println "Profile Mean User")
          (println (:formatted (profile node 50 (user-specific-benchmarks-queries mean-user))))
          (println "Profile Item Scan by IDs")
          (println (:formatted (profile node 1000 (scan-items-individual-benchmark-queries distributed-items))))
          (println (:formatted (profile node 50 (scan-items-set-benchmark-queries distributed-items))))
          (println "Profile Item Scan by Content Key")
          (println (:formatted (profile node 10 (scan-items-by-content-key-queries distributed-items))))
          (println "Profile Filter Queries")
          (println (:formatted (profile node 5 (scan-items-filter-queries distributed-items))))
          (println "Inspect")
          (inspect node {:max-user max-user
                         :active-users active-users}))))
    (catch Exception e
      (log/error e)))

)
