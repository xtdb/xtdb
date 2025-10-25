(ns xtdb.bench.yakbench
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.pprint :as pp]
            [taoensso.tufte :as tufte :refer [p]]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.compactor :as c]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.time Duration Instant LocalTime LocalDate ZoneOffset]
           [java.util UUID]))

;; Scale-factor generation based on README proportions
(def baseline-counts
  {:users 4835
   :feeds 24468
   :subs 107597
   :items 17460648
   :user-items 236804
   :ad-clicks 3341
   :ad-credits 184
   :ads 89
   :bulk-sends 829
   :digests 23091
   :skips 4494})

(def active-user-baseline 1768)

(def )

(defn round-down [x]
  (long (Math/floor (double x))))

(defn -rand-int
  ^long [^java.util.Random random ^long bound]
  (.nextInt random (int (max 1 bound))))

(defn -rand-nth
  [^java.util.Random random coll]
  (let [c (count coll)]
    (when (pos? c)
      (nth coll (-rand-int random c)))))

(defn chance?
  [^java.util.Random random ^double p]
  (< (.nextDouble random) p))

(defn rand-gaussian
  "Standard normal (mean 0, stddev 1) using seeded RNG. Returns double.
   Arity-2 returns N(mean, stddev)."
  (^double [^java.util.Random random]
   (.nextGaussian random))
  (^double [^java.util.Random random ^double mean ^double stddev]
   (+ mean (* stddev (rand-gaussian random)))))

(defn rand-uuid
  [^java.util.Random random]
  (let [msb (.nextLong random)
        lsb (.nextLong random)
        msb (-> msb
                (bit-and (bit-not (bit-shift-left 0xF 12)))
                (bit-or  (bit-shift-left 0x4 12)))
        lsb (-> lsb
                (bit-and (bit-not (bit-shift-left 0x3 62)))
                (bit-or  (bit-shift-left 0x2 62)))]
    (UUID. msb lsb)))

(def ^:private ^String charset "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

(defn rand-string
  ^String [^java.util.Random random ^long n]
  (let [sb (StringBuilder. n)]
    (loop [i 0]
      (if (< i n)
        (do (.append sb (.charAt charset (int (-rand-int random (.length charset)))))
            (recur (unchecked-inc i)))
        (.toString sb)))))

(defn rand-email [^java.util.Random random i]
  (str (rand-string random 8) i "@example.com"))

(defn rand-instant
  ([^java.util.Random random] (rand-instant random 2020 2025))
  ([^java.util.Random random ^long start-year ^long end-year]
   (let [^ZoneOffset zone ZoneOffset/UTC
         ^Instant start (-> (LocalDate/of start-year 1 1)
                            (.atStartOfDay zone)
                            (.toInstant))
         ^Instant end-excl (-> (LocalDate/of (inc end-year) 1 1)
                               (.atStartOfDay zone)
                               (.toInstant))
         start-ms (.toEpochMilli start)
         end-ms (.toEpochMilli end-excl)
         span (max 0 (- end-ms start-ms))
         n (if (pos? span)
             (long (Math/floor (* (.nextDouble random) (double span))))
             0)]
     (Instant/ofEpochMilli (+ start-ms n)))))

(defn rand-local-time [^java.util.Random random]
  (LocalTime/of (-rand-int random 24) (-rand-int random 60)))

(def weekdays [:monday :tuesday :wednesday :thursday :friday :saturday :sunday])

(defn rand-weekday-set [^java.util.Random random]
  (into #{} (for [d weekdays :when (chance? random 0.5)] d)))

(defn poisson
  "Knuth’s algorithm for small λ; Normal approximation for large λ.
   Returns a non-negative long."
  ^long [^java.util.Random random ^double lambda]
  (cond
    (<= lambda 0.0) 0
    (<= lambda 50.0)
    (let [L (Math/exp (- lambda))]
      (loop [k 0, p 1.0]
        (let [p (* p (.nextDouble random))]
          (if (> p L)
            (recur (inc k) p)
            k))))
    :else
    (let [k (long (Math/round (+ lambda (* (Math/sqrt lambda)
                                           (rand-gaussian random)))))]
      (max 0 k))))

(defn lognormal-lambda
  "Draw λ from LogNormal so E[λ]≈mean. sigma controls tail (1.0–2.0 typical)."
  ^double [^java.util.Random random ^double mean ^double sigma]
  (let [mu (- (Math/log (max mean 1e-9)) (/ (* sigma sigma) 2.0))]
    (Math/exp (+ mu (* sigma (rand-gaussian random))))))

(defn ?s [n]
  (str "(" (str/join ", " (repeat n "?")) ")"))

(defn get-conn ^java.sql.Connection [^xtdb.api.DataSource node]
  (.build (.createConnectionBuilder node)))

;; Match yakread behavior: take the first group (8 hex chars) from
;; uuid-prefix and the remaining groups from uuid-rest.
(defn uuid-with-prefix
  [uuid-prefix uuid-rest]
  (let [prefix-str (str uuid-prefix)
        rest-str (str uuid-rest)
        prefix (first (str/split prefix-str #"-"))
        rest-groups (rest (str/split rest-str #"-"))]
    (UUID/fromString (str/join "-" (cons prefix rest-groups)))))

(defn gen-users
  [random n]
  (vec
   (for [i (range n)
         :let [id (rand-uuid random)
               email (rand-email random i)
               uname (subs email 0 (str/index-of email "@"))]]
     {:xt/id id
      :user/email email
      :user/email-username uname
      :user/digest-days (rand-weekday-set random)
      :user/send-digest-at (rand-local-time random)
      :user/joined-at (rand-instant random)
      :user/digest-last-sent (when (chance? random 0.8) (rand-instant random))
      :user/suppressed-at (when (chance? random 0.05) (Instant/parse "2025-01-01T00:00:00Z"))
      :user/use-original-links (chance? random 0.5)})))

(defn gen-feeds
  [random n]
  (vec
   (for [i (range n)
         :let [id (rand-uuid random)]]
     {:xt/id id
      :feed/url (str "https://example.com/feed/" i)
      :feed/title (rand-string random (+ 5 (-rand-int random 15)))
      :feed/etag (rand-string random (+ 10 (-rand-int random 20)))
      :feed/last-modified (rand-string random (+ 10 (-rand-int random 20)))
      :feed/synced-at (rand-instant random)
      :feed/image-url (when (chance? random 0.3) (str "https://img.example/" (rand-string random 16)))
      :feed/description (rand-string random (+ 20 (-rand-int random 80)))
      :feed/failed-syncs (when (chance? random 0.1) (-rand-int random 1000))})))

(defn gen-subs
  [random users feeds avg-subs-per-user]
  (let [feed-ids (mapv :xt/id feeds)
        n-feeds (count feed-ids)
        sigma 1.2]
    (->> users
         (mapcat (fn [{user :xt/id}]
                   (let [lambda (lognormal-lambda random (double (max 0.0 avg-subs-per-user)) sigma)
                         k (min (poisson random lambda) n-feeds)
                         chosen (->> (repeatedly #(-rand-int random n-feeds))
                                     (distinct)
                                     (take k)
                                     (map feed-ids))]
                     (for [f chosen]
                       {:xt/id (uuid-with-prefix user (rand-uuid random))
                        :sub/user user
                        :sub/created-at (rand-instant random)
                        :sub.feed/feed f
                        :sub.email/from (when (chance? random 0.2) (rand-string random (+ 5 (-rand-int random 15))))}))))
         vec)))

(defn gen-items
  [random feeds subs items-per-feed n-items]
  (let [subs-by-feed (->> subs (group-by :sub.feed/feed))
        langs ["EN" "ES" "DE" "FR" "IT" "PT" "PL" "NL"]
        cat-pool (vec (repeatedly 64 #(rand-string random (+ 3 (-rand-int random 10)))))
        name-pool (vec (repeatedly 64 #(rand-string random (+ 6 (-rand-int random 20)))))]
    (letfn [(items-for-feed [{:keys [xt/id]}]
              (let [feed-id ^java.util.UUID id
                    subs* (get subs-by-feed feed-id)
                    sub-ids (when (seq subs*) (mapv :xt/id subs*))
                    sub-count (count sub-ids)
                    sigma 1.3
                    lambda (double (max 0.0 (lognormal-lambda random (double items-per-feed) sigma)))
                    k (poisson random lambda)]
                (map (fn [i]
                       (let [raw-id (rand-uuid random)
                             email-sub (when (and (pos? sub-count) (chance? random 0.2))
                                         (-rand-nth random sub-ids))
                             prefix (or feed-id email-sub #uuid "00000000-0000-0000-0000-000000000000")
                             id (uuid-with-prefix prefix raw-id)
                             categories (vec (repeatedly (-rand-int random 5) #(-rand-nth random cat-pool)))]
                         (cond-> {:xt/id id
                                  :item.feed/feed feed-id
                                  :item/title (str "Item " feed-id "-" i)
                                  :item/excerpt (rand-string random (+ 40 (-rand-int random 120)))
                                  :item/published-at (rand-instant random)
                                  :item/length (+ 100 (-rand-int random 2000))
                                  :item/content-key (rand-uuid random)
                                  :item/lang (-rand-nth random langs)
                                  :item/author-name (-rand-nth random name-pool)
                                  :item/author-email (rand-email random i)
                                  :item/author-url (str "https://example.com/author/" i)
                                  :item/categories categories}
                           email-sub (assoc :item.email/sub email-sub))))
                     (range k))))]
      (->> feeds
           (mapcat items-for-feed)
           (take n-items)))))

(defn gen-user-items
  [random users items avg-items-per-user]
  (let [item-ids (mapv :xt/id items)
        n-items (count item-ids)
        sigma 1.6]
    (->> users
         (mapcat (fn [{user :xt/id}]
                   (let [lambda (lognormal-lambda random (double (max 0.0 avg-items-per-user)) sigma)
                         k (poisson random lambda)
                         k (min k n-items) ;; cap to available items
                         chosen (->> (repeatedly #(-rand-int random n-items))
                                     (distinct)
                                     (take k)
                                     (map item-ids))]
                     (for [item chosen]
                       (cond-> {:xt/id (uuid-with-prefix user (rand-uuid random))
                                :user-item/user user
                                :user-item/item item}
                         (chance? random 0.5) (assoc :user-item/viewed-at (rand-instant random))
                         (chance? random 0.2) (assoc :user-item/favorited-at (rand-instant random))
                         (chance? random 0.1) (assoc :user-item/skipped-at (rand-instant random)))))))
         vec)))

(defn gen-ads
  [random users n]
  (vec
   (for [_ (range n)]
     {:xt/id (rand-uuid random)
      :ad/user (:xt/id (-rand-nth random users))
      :ad/approve-state (-rand-nth random [:approved :pending :rejected])
      :ad/paused (chance? random 0.2)
      :ad/title (rand-string random (+ 12 (-rand-int random 24)))
      :ad/description (rand-string random (+ 180 (-rand-int random 360)))
      :ad/url (str "https://ads.example/" (rand-string random 16))
      :ad/image-url (str "https://img.example/" (rand-string random 24))
      :ad/payment-method (rand-string random (+ 24 (-rand-int random 24)))
      :ad/card-details {:brand (rand-string random 6)
                        :last4 (format "%04d" (mod (-rand-int random 10000) 10000))
                        :exp-month (inc (-rand-int random 12))
                        :exp-year (+ 2025 (-rand-int random 6))}
      :ad/customer-id (rand-string random (+ 18 (-rand-int random 10)))
      :ad/payment-failed (chance? random 0.1)
      :ad/budget (+ 100 (-rand-int random 2000))
      :ad/balance (+ (-rand-int random 500) 0)
      :ad/bid (max 1 (-rand-int random 200))
      :ad/recent-cost (max 0 (-rand-int random 1000))
      :ad/updated-at (rand-instant random)})))

(defn gen-ad-credits
  [random ads n]
  (vec
   (for [_ (range n)]
     {:xt/id (rand-uuid random)
      :ad.credit/ad (:xt/id (-rand-nth random ads))
      :ad.credit/cents (+ 100 (-rand-int random 10000))
      :ad.credit/created-at (rand-instant random)
      :ad.credit/charge-status (if (chance? random 0.9) :confirmed :failed)
      :ad.credit/source (if (chance? random 0.9) :charge :manual)
      :ad.credit/amount (+ 100 (-rand-int random 1000))})))

(defn gen-ad-clicks
  [random ads users n]
  (vec
   (for [_ (range n)]
     {:xt/id (rand-uuid random)
      :ad.click/ad (:xt/id (-rand-nth random ads))
      :ad.click/user (:xt/id (-rand-nth random users))
      :ad.click/created-at (rand-instant random)
      :ad.click/url (str "https://clk.example/" (rand-string random 12))})))

(defn gen-digests
  [random users n]
  (vec
   (for [_ (range n)]
     {:xt/id (rand-uuid random)
      :digest/user (:xt/id (-rand-nth random users))
      :digest/sent-at (rand-instant random)
      :digest/subject (rand-uuid random)
      :digest/discover (rand-uuid random)
      :digest/count (+ 1 (-rand-int random 200))})))

(defn gen-bulk-sends
  [random n]
  (vec
   (let [types [:digest :announcement :maintenance]]
     (for [_ (range n)]
       {:xt/id (rand-uuid random)
        :bulk-send/sent-at (rand-instant random)
        :bulk-send/type (nth types (-rand-int random (count types)))
        :bulk-send/num-users (+ 1 (-rand-int random 10000))
        :bulk-send/payload-size (+ 1 (-rand-int random 10000))}))))

(defn gen-skips
  [random users n]
  (vec
   (for [_ (range n)]
     {:xt/id (rand-uuid random)
      :skip/user (:xt/id (-rand-nth random users))
      :skip/created-at (rand-instant random)})))

(defn generate-data
  "Generate synthetic data according to a scale factor and return a map of
   table keyword -> vector of docs"
  [random scale]
  (let [{:keys [users feeds subs items user-items ads ad-credits ad-clicks digests bulk-sends skips]} baseline-counts
        n-users (round-down (* users scale))
        n-feeds (round-down (* feeds scale))
        n-subs (round-down (* subs scale))
        n-items (round-down (* items scale))
        n-user-items (round-down (* user-items scale))
        n-ads (round-down (* ads scale))
        n-ad-credits (round-down (* ad-credits scale))
        n-ad-clicks (round-down (* ad-clicks scale))
        n-digests (round-down (* digests scale))
        n-bulk-sends (round-down (* bulk-sends scale))
        n-skips (round-down (* skips scale))
        users* (gen-users random n-users)
        feeds* (gen-feeds random n-feeds)
        avg-subs-per-user (double (if (pos? n-users) (/ n-subs (max 1 n-users)) 5.0))
        subs* (gen-subs random users* feeds* (max 1 avg-subs-per-user))
        avg-items-per-feed (double (if (pos? n-feeds) (/ n-items (max 1 n-feeds)) 5.0))
        items* (gen-items random feeds* subs* avg-items-per-feed n-items)
        avg-items-per-user (double (if (pos? n-users) (/ n-user-items (max 1 n-users)) 0.0))
        user-items* (gen-user-items random users* items* avg-items-per-user)
        ads* (gen-ads random users* n-ads)
        ad-credits* (gen-ad-credits random ads* n-ad-credits)
        ad-clicks* (gen-ad-clicks random ads* users* n-ad-clicks)
        digests* (gen-digests random users* n-digests)
        bulk-sends* (gen-bulk-sends random n-bulk-sends)
        skips* (gen-skips random users* n-skips)]
    {:users users*
     :feeds feeds*
     :subs subs*
     :items items*
     :user_items user-items*
     :ads ads*
     :ad_credits ad-credits*
     :ad_clicks ad-clicks*
     :digests digests*
     :bulk_sends bulk-sends*
     :skips skips*}))

(defn load-generated!
  [conn docs-by-table]
  (doseq [table [:users :feeds :subs :items :user_items :ads :ad_credits
                 :ad_clicks :digests :bulk_sends :skips]
          :let [docs (get docs-by-table table)]
          :when (seq docs)]
    (let [batches (partition-all 1000 docs)]
      (loop [bs batches]
        (when-let [batch (first bs)]
          (xt/submit-tx conn [(into [:put-docs table] batch)])
          (recur (rest bs)))))))

(def recent-inst #inst "2025-01-01T00:00:00Z")
(def feed-sync-inst #inst "2025-09-29T00:00:00Z")

(defn benchmarks-for
  [{{:keys [user-id user-email]} :picked-user
    :keys [active-user-ids biggest-feed]}]
  (cond-> [{:id :get-user-by-email
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
           {:id :active-users-by-joined-at
            :f #(xt/q % ["select _id from users where user$joined_at > ?"
                         recent-inst])}
           {:id :active-users-by-viewed-at
            :f #(xt/q % ["select distinct user_item$user from user_items where user_item$viewed_at > ?"
                         recent-inst])}
           {:id :active-users-by-ad-updated
            :f #(xt/q % ["select ad$user from ads where ad$updated_at > ?"
                         recent-inst])}
           {:id :active-users-by-ad-clicked
            :f #(xt/q % ["select distinct ad.click$user from ad_clicks where ad.click$created_at > ?"
                         recent-inst])}]

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
                           :else (repeat n-full chunk))]
          (doseq [sub-scale sub-scales]
            (let [docs (generate-data random sub-scale)]
              (load-generated! conn docs))))))))

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

(defn get-active-users
  [conn random scale-factor]
  (let [active-users (round-down (* active-user-baseline scale-factor))
        joined (xt/q conn ["select u._id as _id from users u where u.user$joined_at > ?" recent-inst])
        viewed (xt/q conn ["select distinct ui.user_item$user as _id from user_items ui where ui.user_item$viewed_at > ?" recent-inst])
        ad-updated (xt/q conn ["select distinct a.ad$user as _id from ads a where a.ad$updated_at > ?" recent-inst])
        ids (->> (concat joined viewed ad-updated)
                 (map :xt/id)
                 (filter some?)
                 distinct
                 vec)
        ids (let [al (java.util.ArrayList. ids)]
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
      (let [titles (map :item/title (xt/q conn ["select item$title from items where item$feed$feed = ?" (:xt/id feed)]))
            random-titles (repeatedly (count titles) #(rand-string random (+ 10 (-rand-int random 10))))
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

(defn benchmark
  [node !state]
  (with-open [conn (get-conn node)]
    (let [iterations 10
          suite (benchmarks-for @!state)
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

(defn ->vega-bar-html
  "Return a complete Vega-Lite HTML string for a simple bar chart, given:
   - title: chart title
   - x-title: x-axis title (x field is fixed as 'k')
   - y-title: y-axis title
   - y-field: name of the y field present in the inline values (e.g. 'users' or 'feeds')
   - values-str: inline JSON array elements, already joined with commas"
  [title x-title y-title y-field values-str]
  (format
   (str "<!doctype html>\n"
        "<html>\n"
        "<head>\n"
        "  <meta charset=\"utf-8\"/>\n"
        "  <script src=\"https://cdn.jsdelivr.net/npm/vega@5\"></script>\n"
        "  <script src=\"https://cdn.jsdelivr.net/npm/vega-lite@5\"></script>\n"
        "  <script src=\"https://cdn.jsdelivr.net/npm/vega-embed@6\"></script>\n"
        "  <style> body { margin: 0; padding: 16px; font-family: sans-serif; } #vis { width: 1200px; height: 700px; } </style>\n"
        "</head>\n"
        "<body>\n"
        "  <div id=\"vis\"></div>\n"
        "  <script>\n"
        "    const spec = {\n"
        "      $schema: \"https://vega.github.io/schema/vega-lite/v5.json\",\n"
        "      width: 900,\n"
        "      height: 600,\n"
        "      data: { values: [%s] },\n"
        "      mark: \"bar\",\n"
        "      title: \"%s\",\n"
        "      encoding: {\n"
        "        x: { field: \"k\", type: \"quantitative\", bin: false, axis: { title: \"%s\" } },\n"
        "        y: { field: \"%s\", type: \"quantitative\", axis: { title: \"%s\" } }\n"
        "      }\n"
        "    };\n"
        "    vegaEmbed('#vis', spec, { actions: false });\n"
        "  </script>\n"
        "</body>\n"
        "</html>\n")
   values-str title x-title y-field y-title))

(defn write-user-items-histogram-html-inline!
  "Write a Vega-Lite HTML bar chart with inline data values (no external CSV)."
  [node html-path]
  (with-open [conn (get-conn node)]
    (let [title "Items per User"
          rows (xt/q conn
                     [(str "select t.item_count as k, count(*) as users "
                           "from (select ui.user_item$user as user_id, count(*) as item_count "
                           "      from user_items ui group by ui.user_item$user) t "
                           "group by t.item_count order by t.item_count")])
          values-str (->> rows
                          (map (fn [{:keys [k users]}]
                                 (format "{\"k\":%d,\"users\":%d}" (long k) (long users))))
                          (str/join ","))
          html (->vega-bar-html title "items per user" "user count" "users" values-str)]
      (spit html-path html))))

(defn write-user-subs-histogram-html-inline!
  "Write a Vega-Lite HTML bar chart for subs per user with inline data."
  [node html-path]
  (with-open [conn (get-conn node)]
    (let [title "Subs per User"
          rows (xt/q conn
                     [(str "select t.sub_count as k, count(*) as users "
                           "from (select s.sub$user as user_id, count(*) as sub_count "
                           "      from subs s group by s.sub$user) t "
                           "group by t.sub_count order by t.sub_count")])
          values-str (->> rows
                          (map (fn [{:keys [k users]}]
                                 (format "{\"k\":%d,\"users\":%d}" (long k) (long users))))
                          (str/join ","))
          html (->vega-bar-html title "subs per user" "user count" "users" values-str)]
      (spit html-path html))))

(defn write-feed-items-histogram-html-inline!
  "Write a Vega-Lite HTML bar chart for items per feed with inline data."
  [node html-path]
  (with-open [conn (get-conn node)]
    (let [title "Items per Feed"
          rows (xt/q conn
                     [(str "select t.item_count as k, count(*) as feeds "
                           "from (select i.item$feed$feed as feed_id, count(*) as item_count "
                           "      from items i group by i.item$feed$feed) t "
                           "group by t.item_count order by t.item_count")])
          values-str (->> rows
                          (map (fn [{:keys [k feeds]}]
                                 (format "{\"k\":%d,\"feeds\":%d}" (long k) (long feeds))))
                          (str/join ","))
          html (->vega-bar-html title "items per feed" "feed count" "feeds" values-str)]
      (spit html-path html))))

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
    (let [{{:keys [user-id user-email]} :picked-user} @!state
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
    (pp/print-table [(user-items-stats conn)])
    (println)

    ;; User subs stats
    (println "User subs stats")
    (pp/print-table [(user-subs-stats conn)])
    (println)

    ;; Feed items stats
    (println "Feed items stats")
    (pp/print-table [(feed-items-stats conn)])
    (println)

    ;; User items histogram
    (let [html-path "/tmp/user_items_histogram.html"]
      (println "User items histogram written to " html-path)
      (write-user-items-histogram-html-inline! node html-path))

    ;; User subs histogram
    (let [html-path "/tmp/user_subs_histogram.html"]
      (println "User subs histogram written to " html-path)
      (write-user-subs-histogram-html-inline! node html-path))

    ;; Feed items histogram
    (let [html-path "/tmp/feed_items_histogram.html"]
      (println "Feed items histogram written to " html-path)
      (write-feed-items-histogram-html-inline! node html-path))))

(defmethod b/cli-flags :yakbench [_]
  [["-s" "--scale-factor SCALE_FACTOR" "Yakbench scale factor to use"
    :parse-fn parse-double
    :default 0.01]
   ["-h" "--help"]])

(defmethod b/->benchmark :yakbench [_ {:keys [scale-factor seed no-load?],
                                       :or {scale-factor 0.01, seed 0}}]
  (log/info {:scale-factor scale-factor :seed seed :no-load? no-load?})

  {:title "Yakbench", :seed seed
   :->state #(do {:!state (atom {})})
   :tasks [{:t :do
            :stage :ingest
            :tasks (concat (when-not no-load?
                             [{:t :call
                               :stage :submit-docs
                               :f (fn [{:keys [node random]}] (load-data! node random scale-factor))}])
                           [{:t :do
                             :stage :finish-block
                             :tasks [{:t :call :f (fn [{:keys [node]}] (b/finish-block! node))}]}
                            {:t :do
                             :stage :compact
                             :tasks [{:t :call :f (fn [{:keys [node]}] (c/compact-all! node (Duration/ofSeconds 30)) #_(b/compact! node))}]}])}
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
           {:t :do
            :stage :profile
            :tasks [{:t :call
                     :f (fn [{:keys [node !state]}]
                          (benchmark node !state))}]}
           {:t :do
            :stage :inspect
            :tasks [{:t :call
                     :f (fn [{:keys [node !state]}]
                          (inspect node !state))}]}]})

(comment
  (try
    (with-open [node (tu/->local-node {:node-dir (util/->path "/tmp/yakbench-alt")})
                conn (get-conn node)]
      (let [rnd (java.util.Random. 0)
            scale 0.1
            !state (atom {})]
        (load-data! node rnd scale)
        (swap! !state assoc :picked-user (get-worst-case-user conn))
        (swap! !state assoc :active-user-ids (get-active-users conn rnd scale))
        (swap! !state assoc :biggest-feed (get-biggest-feed conn rnd))
        (benchmark node !state)
        (inspect node !state)))
    (catch Exception e
      (log/error e)))

  )
