(ns xtdb.datasets.yakbench
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench.random :as random]
            [xtdb.bench.stats :as stats])
  (:import [java.util UUID]))

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

(def user-items-stats
  {:mean 27.2125948057918
   :median 2
   :p90 24
   :p99 272
   :min 1
   :max 16708})

(def user-subs-stats
  {:mean 106.955268389662
   :median 5
   :p90 114
   :p99 1865
   :min 1
   :max 13356})

(def feed-items-stats
  {:mean 736.407677681279
   :median 25
   :p90 589
   :p99 12004
   :min 1
   :max 363444})

(defn next-email [^java.util.Random random]
  (str (random/next-string random 8) "@example.com"))

(def weekdays [:monday :tuesday :wednesday :thursday :friday :saturday :sunday])

(defn next-weekday-set [^java.util.Random random]
  (into #{} (for [d weekdays :when (random/chance? random 0.5)] d)))

(defn next-recent-instant [^java.util.Random random]
  (random/next-instant random 2020 2025))

(defn uuid-with-prefix
  "Match yakread behavior: take the first group (8 hex chars) from
  uuid-prefix and the remaining groups from uuid-rest."
  [uuid-prefix uuid-rest]
  (let [prefix-str (str uuid-prefix)
        rest-str (str uuid-rest)
        prefix (first (str/split prefix-str #"-"))
        rest-groups (rest (str/split rest-str #"-"))]
    (UUID/fromString (str/join "-" (cons prefix rest-groups)))))

(defn gen-users
  "For users we add an extra :user/k field that is a poisson-distributed long.
  This is used to determine the number of items and subs a user has."
  [random n]
  (vec
   (for [_ (range n)
         :let [email (next-email random)]]
     {:xt/id (random/next-uuid random)
      :user/email email
      :user/email-username (subs email 0 (str/index-of email "@"))
      :user/digest-days (next-weekday-set random)
      :user/send-digest-at (random/next-local-time random)
      :user/joined-at (next-recent-instant random)
      :user/digest-last-sent (when (random/chance? random 0.8) (next-recent-instant random))
      :user/suppressed-at (when (random/chance? random 0.05) (next-recent-instant random))
      :user/use-original-links (random/chance? random 0.5)
      :user/k (stats/draw-lognormal-poisson random (:mean user-items-stats) (:median user-items-stats))})))

(defn gen-feeds
  [random n]
  (vec
   (for [i (range n)]
     {:xt/id (random/next-uuid random)
      :feed/url (str "https://example.com/feed/" i)
      :feed/title (random/next-string random (+ 5 (random/next-int random 15)))
      :feed/etag (random/next-string random (+ 10 (random/next-int random 20)))
      :feed/last-modified (random/next-string random (+ 10 (random/next-int random 20)))
      :feed/synced-at (next-recent-instant random)
      :feed/image-url (when (random/chance? random 0.3) (str "https://img.example/" (random/next-string random 16)))
      :feed/description (random/next-string random (+ 20 (random/next-int random 80)))
      :feed/failed-syncs (when (random/chance? random 0.1) (random/next-int random 1000))})))

(defn gen-subs
  [random users feeds n-subs]
  (let [feed-ids (mapv :xt/id feeds)
        n-feeds (count feed-ids)]
    (->> users
         cycle
         (mapcat (fn [{user :xt/id
                       user-k :user/k}]
                   (let [k (min (long (* user-k 0.25)) n-feeds)]
                     (for [f (->> (repeatedly #(random/next-int random n-feeds))
                                  (distinct)
                                  (take k)
                                  (map feed-ids))]
                       {:xt/id (uuid-with-prefix user (random/next-uuid random))
                        :sub/user user
                        :sub/created-at (next-recent-instant random)
                        :sub.feed/feed f
                        :sub.email/from (when (random/chance? random 0.2) (random/next-string random (+ 5 (random/next-int random 15))))
                        :sub.email/unsubscribed-at (when (random/chance? random 0.05) (random/next-instant random))}))))
         (take n-subs)
         vec)))

(defn gen-items
  [random feeds subs n-items]
  (let [subs-by-feed (->> subs (group-by :sub.feed/feed))
        langs ["EN" "ES" "DE" "FR" "IT" "PT" "PL" "NL"]
        cat-pool (vec (repeatedly 64 #(random/next-string random (+ 3 (random/next-int random 10)))))
        name-pool (vec (repeatedly 64 #(random/next-string random (+ 6 (random/next-int random 20)))))]
    (letfn [(items-for-feed [{feed-id :xt/id}]
              (let [subs* (get subs-by-feed feed-id)
                    sub-ids (when (seq subs*) (mapv :xt/id subs*))
                    sub-count (count sub-ids)
                    k (min (stats/draw-lognormal-poisson random (:mean feed-items-stats) (:median feed-items-stats))
                           n-items)]
                (map (fn [i]
                       (let [raw-id (random/next-uuid random)
                             email-sub (when (and (pos? sub-count) (random/chance? random 0.2))
                                         (random/uniform-nth random sub-ids))
                             id (uuid-with-prefix feed-id raw-id)
                             categories (vec (repeatedly (random/next-int random 5) #(random/uniform-nth random cat-pool)))]
                         (cond-> {:xt/id id
                                  :item.feed/feed feed-id
                                  :item/title (str "Item " feed-id "-" i)
                                  :item/excerpt (random/next-string random (+ 40 (random/next-int random 120)))
                                  :item/published-at (next-recent-instant random)
                                  :item/length (+ 100 (random/next-int random 2000))
                                  :item/content-key (random/next-uuid random)
                                  :item/lang (random/uniform-nth random langs)
                                  :item/author-name (random/uniform-nth random name-pool)
                                  :item/author-email (next-email random)
                                  :item/author-url (str "https://example.com/author/" i)
                                  :item/categories categories
                                  :item/ingested-at (next-recent-instant random)
                                  :item/url (random/next-string random (+ 10 (random/next-int random 50)))}
                           (random/chance? random 0.001) (assoc :item/doc-type :item/direct
                                                                :item.direct/candidate-status (random/weighted-nth random [0.956 0.035 0.008] [:approved :failed :ingest-failed]))
                           email-sub (assoc :item.email/sub email-sub))))
                     (range k))))]
      (->> feeds
           cycle
           (mapcat items-for-feed)
           (take n-items)
           vec))))

(defn gen-user-items
  [random users items n-user-items]
  (let [item-ids (mapv :xt/id items)
        n-items (count item-ids)]
    (->> users
         cycle
         (mapcat (fn [{user :xt/id
                       user-k :user/k}]
                   (let [k (min user-k n-items)
                         chosen (->> (repeatedly #(random/next-int random n-items))
                                     (distinct)
                                     (take k)
                                     (map item-ids))]
                     (for [item chosen]
                       (cond-> {:xt/id (uuid-with-prefix user (random/next-uuid random))
                                :user-item/user user
                                :user-item/item item}
                         (random/chance? random 0.5) (assoc :user-item/viewed-at (next-recent-instant random))
                         (random/chance? random 0.2) (assoc :user-item/favorited-at (next-recent-instant random))
                         (random/chance? random 0.1) (assoc :user-item/bookmarked-at (next-recent-instant random))
                         (random/chance? random 0.1) (assoc :user-item/skipped-at (next-recent-instant random)))))))
         (take n-user-items)
         vec)))

(defn gen-ads
  [random users n]
  (vec
   (for [_ (range n)]
     {:xt/id (random/next-uuid random)
      :ad/user (:xt/id (random/uniform-nth random users))
      :ad/approve-state (random/uniform-nth random [:approved :pending :rejected])
      :ad/paused (random/chance? random 0.2)
      :ad/title (random/next-string random (+ 12 (random/next-int random 24)))
      :ad/description (random/next-string random (+ 180 (random/next-int random 360)))
      :ad/url (str "https://ads.example/" (random/next-string random 16))
      :ad/image-url (str "https://img.example/" (random/next-string random 24))
      :ad/payment-method (random/next-string random (+ 24 (random/next-int random 24)))
      :ad/card-details {:brand (random/next-string random 6)
                        :last4 (format "%04d" (mod (random/next-int random 10000) 10000))
                        :exp-month (inc (random/next-int random 12))
                        :exp-year (+ 2025 (random/next-int random 6))}
      :ad/customer-id (random/next-string random (+ 18 (random/next-int random 10)))
      :ad/payment-failed (random/chance? random 0.1)
      :ad/budget (+ 100 (random/next-int random 2000))
      :ad/balance (+ (random/next-int random 500) 0)
      :ad/bid (max 1 (random/next-int random 200))
      :ad/recent-cost (max 0 (random/next-int random 1000))
      :ad/updated-at (next-recent-instant random)})))

(defn gen-ad-credits
  [random ads n]
  (vec
   (for [_ (range n)]
     {:xt/id (random/next-uuid random)
      :ad.credit/ad (:xt/id (random/uniform-nth random ads))
      :ad.credit/cents (+ 100 (random/next-int random 10000))
      :ad.credit/created-at (next-recent-instant random)
      :ad.credit/charge-status (if (random/chance? random 0.9) :confirmed :failed)
      :ad.credit/source (if (random/chance? random 0.9) :charge :manual)
      :ad.credit/amount (+ 100 (random/next-int random 1000))})))

(defn gen-ad-clicks
  [random ads users n]
  (vec
   (for [_ (range n)]
     {:xt/id (random/next-uuid random)
      :ad.click/ad (:xt/id (random/uniform-nth random ads))
      :ad.click/user (:xt/id (random/uniform-nth random users))
      :ad.click/created-at (next-recent-instant random)
      :ad.click/url (str "https://clk.example/" (random/next-string random 12))
      :ad.click/cost (random/next-int random 1000)})))

(defn gen-digests
  [random users n]
  (vec
   (for [_ (range n)]
     {:xt/id (random/next-uuid random)
      :digest/user (:xt/id (random/uniform-nth random users))
      :digest/sent-at (next-recent-instant random)
      :digest/subject (random/next-uuid random)
      :digest/discover (random/next-uuid random)
      :digest/count (+ 1 (random/next-int random 200))})))

(defn gen-bulk-sends
  [random digests n]
  (vec
   (for [_ (range n)]
     {:xt/id (random/next-uuid random)
      :bulk-send/sent-at (next-recent-instant random)
      :bulk-send/digests (vec (repeatedly (random/next-int random 10) #(random/uniform-nth random digests)))
      :bulk-send/mailersend-id (random/next-string random 24)
      :bulk-send/num-users (+ 1 (random/next-int random 10000))
      :bulk-send/payload-size (+ 1 (random/next-int random 10000))})))

(defn gen-skips
  [random users n]
  (vec
   (for [_ (range n)]
     {:xt/id (random/next-uuid random)
      :skip/user (:xt/id (random/uniform-nth random users))
      :skip/created-at (next-recent-instant random)})))

(defn generate-data
  "Generate synthetic data according to a scale factor and return a map of
   table keyword -> vector of docs"
  [random scale]
  (let [{:keys [users feeds subs items user-items ads ad-credits ad-clicks digests bulk-sends skips]} baseline-counts
        users* (gen-users random (stats/round-down (* users scale)))
        feeds* (gen-feeds random (stats/round-down (* feeds scale)))
        subs* (gen-subs random users* feeds* (stats/round-down (* subs scale)))
        items* (gen-items random feeds* subs* (stats/round-down (* items scale)))
        digests* (gen-digests random users* (stats/round-down (* digests scale)))
        ads* (gen-ads random users* (stats/round-down (* ads scale)))]
    {:users users*
     :feeds feeds*
     :subs subs*
     :items items*
     :user_items (gen-user-items random users* items* (stats/round-down (* user-items scale)))
     :ads ads*
     :ad_credits (gen-ad-credits random ads* (stats/round-down (* ad-credits scale)))
     :ad_clicks (gen-ad-clicks random ads* users* (stats/round-down (* ad-clicks scale)))
     :digests digests*
     :bulk_sends (gen-bulk-sends random digests* (stats/round-down (* bulk-sends scale)))
     :skips (gen-skips random users* (stats/round-down (* skips scale)))}))

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

(defn load-data!
  "Load data in chunks if scale factor is greater than 0.1 under the assumption
  that the whole dataset is roughly the same as an aggregation of chunk sized
  datasets"
  [conn random scale-factor]
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
          (let [docs (generate-data random sub-scale)]
            (load-generated! conn docs)))))))
