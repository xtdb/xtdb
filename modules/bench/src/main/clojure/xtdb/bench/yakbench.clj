(ns xtdb.bench.yakbench
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.pprint :as pp]
            [taoensso.tufte :as tufte :refer [p]]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
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
  [{:keys [active-user-ids biggest-feed]}]
  (cond-> [{:id :active-users-by-joined-at
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
                              "group by user_item$item")])}]

    (seq active-user-ids)
    (conj {:id :feeds-to-sync
           :f #(xt/q %
                     (vec (concat [(str "select distinct sub$feed$feed "
                                        "from subs "
                                        "join feeds on feeds._id = sub$feed$feed "
                                        "where sub$user in " (?s (count active-user-ids))
                                        " and (feed$synced_at is null or feed$synced_at < ?)")]
                                  active-user-ids
                                  [feed-sync-inst])))})

    (and biggest-feed (seq (:titles biggest-feed)))
    (conj {:id :existing-feed-titles
           :f #(xt/q %
                     (vec
                      (concat [(str "select item$title "
                                    "from items "
                                    "where item$feed$feed = ? "
                                    "and item$title in " (?s (count (:titles biggest-feed))))
                               (:id biggest-feed)]
                              (:titles biggest-feed))))})))

(defn user-specific-benchmarks-queries
  [{{:keys [user-id user-email]} :picked-user}]
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

(defn get-worst-case-user
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
  (let [active-users (yakbench/round-down (* yakbench/active-user-baseline scale-factor))
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
            random-titles (repeatedly (count titles) #(yakbench/next-string random (+ 10 (yakbench/next-int random 10))))
            titles (concat titles random-titles)]
        {:id (:xt/id feed)
         :titles titles}))))

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
  [node !state suite]
  (with-open [conn (get-conn node)]
    (let [iterations 10
          _throwaway (doseq [{:keys [f]} suite]
                       (f conn))
          [_ pstats] (tufte/profiled
                      {}
                      (doseq [{:keys [id f]} suite
                              _ (range iterations)]
                        (p id (f conn))))]
      (prn {:profile (profile-data @pstats)})
      (println (tufte/format-pstats pstats))
      (swap! !state assoc :pstats @pstats))))

(defn distribution-stats
  "Compute mean/median/p90/p99/min/max from a seq of numeric counts."
  [counts]
  (let [counts (vec (map long counts))
        n (count counts)
        sorted (vec (sort counts))
        mean (if (pos? n) (/ (reduce + 0 sorted) (double n)) 0.0)
        mn (if (pos? n) (first sorted) 0)
        mx (if (pos? n) (peek sorted) 0)
        idx (fn [p]
              (cond
                (zero? n) 0
                :else (-> (Math/floor (* p (dec n))) long (max 0) (min (dec n)))))
        median (if (pos? n) (sorted (idx 0.5)) 0)
        p90 (if (pos? n) (sorted (idx 0.90)) 0)
        p99 (if (pos? n) (sorted (idx 0.99)) 0)]
    {:mean mean :median median :p90 p90 :p99 p99 :min mn :max mx}))

(defn stats-from-sql
  "Run a SQL that returns rows with a single count column (aliased as `cnt` by default)
   and compute distribution stats over those counts. Provide `count-key` if alias differs."
  ([conn sql]
   (stats-from-sql conn sql :cnt))
  ([conn sql count-key]
   (let [rows (xt/q conn [sql])
         counts (map count-key rows)]
     (distribution-stats counts))))

(defn get-user-items-stats
  [conn]
  (stats-from-sql conn
                  (str "select count(*) as cnt "
                       "from user_items ui "
                       "group by ui.user_item$user")
                  :cnt))

(defn get-user-subs-stats
  [conn]
  (stats-from-sql conn
                  (str "select count(*) as cnt "
                       "from subs s "
                       "group by s.sub$user")
                  :cnt))

(defn get-feed-items-stats
  [conn]
  (stats-from-sql conn
                  (str "select count(*) as cnt "
                       "from items i "
                       "group by i.item$feed$feed")
                  :cnt))

(defn inspect
  [node !state]
  (with-open [conn (get-conn node)]
    ;; Data
    (let [tables ["users" "feeds" "subs"
                  "items" "user_items" "ads"
                  "ad_credits" "ad_clicks" "digests"
                  "bulk_sends" "skips"]
          rows (for [t tables]
                 {:table t
                  :count (:xt/column-1 (first (xt/q conn [(str "select count(*) from " t)])))})]
      (println)
      (println "Table counts")
      (pp/print-table [:table :count] rows))
      (println)

    ;; User
    (let [{{:keys [user-id]} :picked-user} @!state
          queries [["subs" "select count(*) from subs where sub$user = ?" [user-id]]
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
      (println "User Counts")
      (let [rows (for [[label sql args] queries]
                   {:table label
                    :count (:xt/column-1 (first (xt/q conn (into [sql] args))))})]
        (pp/print-table [:table :count] rows))
      (println))

    ;; Active Users
    (println "Active Users" (count (:active-user-ids @!state)))
    (println)

    ;; User items stats
    (println "User items stats")
    (pp/print-table [(get-user-items-stats conn)])
    (println)

    ;; User subs stats
    (println "User subs stats")
    (pp/print-table [(get-user-subs-stats conn)])
    (println)

    ;; Feed items stats
    (println "Feed items stats")
    (pp/print-table [(get-feed-items-stats conn)])
    (println)))

(defn load-data!
  "Load data in chunks if scale factor is greater than 0.1 under the assumption
  that the whole dataset is roughly the same as an aggregation of chunk sized
  datasets"
  [node random scale-factor]
  (with-open [conn (get-conn node)]
    (let [scale (Double/parseDouble (str scale-factor))]
      (when (pos? scale)
        (let [chunk 0.1
              n-full (long (Math/floor (/ scale chunk)))
              rem (- scale (* n-full chunk))
              sub-scales (cond
                           (<= scale chunk) [scale]
                           (pos? rem) (concat (repeat n-full chunk) [rem])
                           :else (repeat n-full chunk))
              v-sub-scales (vec sub-scales)
              total (count v-sub-scales)]
          (doseq [[idx sub-scale] (map-indexed vector v-sub-scales)]
            (log/debug (format "Loading chunk %d/%d (scale %.3f)" (inc idx) total sub-scale))
            (let [docs (yakbench/generate-data random sub-scale)]
              (yakbench/load-generated! conn docs))))))))

(defmethod b/cli-flags :yakbench [_]
  [["-s" "--scale-factor SCALE_FACTOR" "Yakbench scale factor to use"
    :parse-fn parse-double
    :default 0.1]
   ["-h" "--help"]])

(defmethod b/->benchmark :yakbench [_ {:keys [scale-factor seed no-load?],
                                       :or {scale-factor 0.1, seed 0}}]
  (log/info {:scale-factor scale-factor :seed seed :no-load? no-load?})

  {:title "Yakbench", :seed seed
   :->state #(do {:!state (atom {})})
   :tasks [(when-not no-load?
             {:t :do
              :stage :ingest
              :tasks [{:t :call
                       :stage :submit-docs
                       :f (fn [{:keys [node random]}] (load-data! node random scale-factor))}
                      {:t :call
                      :stage :await-transactions
                      :f (fn [{:keys [node]}] (b/sync-node node))}
                     {:t :call
                      :stage :flush-block
                      :f (fn [{:keys [node]}] (tu/flush-block! node))}
                     {:t :call
                      :stage :compact
                      :f (fn [{:keys [node]}] (c/compact-all! node nil))}]})
           {:t :do
            :stage :get-query-data
            :tasks [{:t :call
                     :f (fn [{:keys [node !state]}]
                          (with-open [conn (get-conn node)]
                            (let [user (get-worst-case-user conn)]
                              (swap! !state assoc :picked-user user))))}
                    {:t :call
                     :f (fn [{:keys [node !state random]}]
                          (with-open [conn (get-conn node)]
                            (let [active-users (get-active-users conn random scale-factor)]
                              (swap! !state assoc :active-user-ids active-users))))}
                    {:t :call
                     :f (fn [{:keys [node !state random]}]
                          (with-open [conn (get-conn node)]
                            (let [biggest-feed (get-biggest-feed conn random)]
                              (swap! !state assoc :biggest-feed biggest-feed))))}]}

           {:t :call
            :stage :profile-global-queries
            :f (fn [{:keys [node !state]}]
                 (profile node !state (benchmarks-queries @!state)))}
           {:t :call
            :stage :profile-max-user
            :f (fn [{:keys [node !state]}]
                 (profile node !state (user-specific-benchmarks-queries @!state)))}
           {:t :call
            :stage :profile-mean-user
            :f (fn [{:keys [node]}]
                 (let [state (atom {:picked-user (get-mean-user (get-conn node))})]
                   (profile node state (user-specific-benchmarks-queries state))))}
           #_
           {:t :call
            :stage :inspect
            :f (fn [{:keys [node !state]}]
                 (inspect node !state))}]})

(comment
  (try
    (let [rnd (java.util.Random. 0)
          scale 1.0
          !state (atom {})
          dir (util/->path "/tmp/yakbench")
          clear-dir (fn [^java.nio.file.Path path]
                      (when (util/path-exists path)
                        (log/info "Clearing directory" path)
                        (util/delete-dir path)))]
      (with-open [node (tu/->local-node {:node-dir dir})
                  conn (get-conn node)]
        (println "Clear dir")
        (clear-dir dir)
        (println "Load data")
        (load-data! node rnd scale)
        (println "Flush block")
        (b/sync-node node)
        (tu/flush-block! node)
        (println "Compact")
        (c/compact-all! node nil)
        (println "Get Query Data")
        (swap! !state assoc :picked-user (get-worst-case-user conn))
        (swap! !state assoc :active-user-ids (get-active-users conn rnd scale))
        (swap! !state assoc :biggest-feed (get-biggest-feed conn rnd))
        (println "Profile")
        (profile node !state)
        (println "Inspect")
        (inspect node !state)))
    (catch Exception e
      (log/error e)))

  )