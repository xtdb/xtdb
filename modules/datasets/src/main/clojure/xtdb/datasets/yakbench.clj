(ns xtdb.datasets.yakbench
  (:require [clojure.string :as str]
            [xtdb.api :as xt])
  (:import [java.time Instant LocalTime LocalDate ZoneOffset]
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

(defn round-down [x]
  (long (Math/floor (double x))))

(defn next-int
  ^long [^java.util.Random random ^long bound]
  (.nextInt random (max 1 bound)))

(defn uniform-nth
  [^java.util.Random random coll]
  (let [c (count coll)]
    (when (pos? c)
      (nth coll (next-int random c)))))

(defn chance?
  [^java.util.Random random ^double p]
  (< (.nextDouble random) p))

(defn weighted-nth
  "Draw a single element from xs with non-negative weights.
   Returns nil if xs empty. If all weights are zero, falls back to uniform."
  [^java.util.Random rng weights xs]
  (let [ws (mapv #(double (max 0.0 %)) weights)
        xs (vec xs)
        n  (count xs)]
    (when (pos? n)
      (let [s (reduce + 0.0 ws)]
        (if (pos? s)
          (let [target (* (.nextDouble rng) s)]
            (loop [i 0 acc 0.0]
              (let [acc' (+ acc (ws i))]
                (if (or (>= acc' target) (= i (dec n)))
                  (xs i)
                  (recur (inc i) acc')))))
          ;; uniform fallback
          (xs (next-int rng n)))))))

(defn next-uuid
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

(defn next-string
  ^String [^java.util.Random random ^long n]
  (let [sb (StringBuilder. n)]
    (loop [i 0]
      (if (< i n)
        (do (.append sb (.charAt charset (next-int random (.length charset))))
            (recur (unchecked-inc i)))
        (.toString sb)))))

(defn next-email [^java.util.Random random]
  (str (next-string random 8) "@example.com"))

(defn next-instant
  ([^java.util.Random random] (next-instant random 2020 2025))
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

(defn next-local-time [^java.util.Random random]
  (LocalTime/of (next-int random 24) (next-int random 60)))

(def weekdays [:monday :tuesday :wednesday :thursday :friday :saturday :sunday])

(defn next-weekday-set [^java.util.Random random]
  (into #{} (for [d weekdays :when (chance? random 0.5)] d)))

(defn draw-poisson
  "Knuth's algorithm for small λ; Normal approximation for large λ.
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
                                           (.nextGaussian random)))))]
      (max 0 k))))

(defn lognormal-lambda
  "Draw λ from LogNormal so E[λ]≈mean. sigma controls tail (1.0–2.0 typical)."
  ^double [^java.util.Random random ^double mean ^double sigma]
  (let [mu (- (Math/log (max mean 1e-9)) (/ (* sigma sigma) 2.0))]
    (Math/exp (+ mu (* sigma (.nextGaussian random))))))

(defn sigma-from-stats
  "Compute lognormal sigma from mean and median."
  [{:keys [mean median]}]
  (Math/sqrt (* 2.0 (Math/log (/ (double mean) (double median))))))

(defn draw-lognormal-poisson
  "Takes a map of :mean and :median and returns a poisson-distributed long."
  [^java.util.Random random {:keys [mean median]}]
  (let [sigma (sigma-from-stats {:mean mean :median median})]
    (draw-poisson random (lognormal-lambda random mean sigma))))

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
  "For users we add an extra :user/k field that is a poisson-distributed long.
  This is used to determine the number of items and subs a user has."
  [random n]
  (vec
   (for [_ (range n)
         :let [id (next-uuid random)
               email (next-email random)
               uname (subs email 0 (str/index-of email "@"))]]
     {:xt/id id
      :user/email email
      :user/email-username uname
      :user/digest-days (next-weekday-set random)
      :user/send-digest-at (next-local-time random)
      :user/joined-at (next-instant random)
      :user/digest-last-sent (when (chance? random 0.8) (next-instant random))
      :user/suppressed-at (when (chance? random 0.05) (Instant/parse "2025-01-01T00:00:00Z"))
      :user/use-original-links (chance? random 0.5)
      :user/k (draw-lognormal-poisson random user-items-stats)})))

(defn gen-feeds
  [random n]
  (vec
   (for [i (range n)
         :let [id (next-uuid random)]]
     {:xt/id id
      :feed/url (str "https://example.com/feed/" i)
      :feed/title (next-string random (+ 5 (next-int random 15)))
      :feed/etag (next-string random (+ 10 (next-int random 20)))
      :feed/last-modified (next-string random (+ 10 (next-int random 20)))
      :feed/synced-at (next-instant random)
      :feed/image-url (when (chance? random 0.3) (str "https://img.example/" (next-string random 16)))
      :feed/description (next-string random (+ 20 (next-int random 80)))
      :feed/failed-syncs (when (chance? random 0.1) (next-int random 1000))})))

(defn gen-subs
  [random users feeds]
  (let [feed-ids (mapv :xt/id feeds)
        n-feeds (count feed-ids)]
    (->> users
         (mapcat (fn [{user :xt/id
                       user-k :user/k}]
                   (let [k (min (long (* user-k 0.25)) n-feeds)
                         chosen (->> (repeatedly #(next-int random n-feeds))
                                     (distinct)
                                     (take k)
                                     (map feed-ids))]
                     (for [f chosen]
                       {:xt/id (uuid-with-prefix user (next-uuid random))
                        :sub/user user
                        :sub/created-at (next-instant random)
                        :sub.feed/feed f
                        :sub.email/from (when (chance? random 0.2) (next-string random (+ 5 (next-int random 15))))
                        :sub.email/unsubscribed-at (when (chance? random 0.05) (next-instant random))}))))
         vec)))

(defn gen-items
  [random feeds subs n-items]
  (let [subs-by-feed (->> subs (group-by :sub.feed/feed))
        langs ["EN" "ES" "DE" "FR" "IT" "PT" "PL" "NL"]
        cat-pool (vec (repeatedly 64 #(next-string random (+ 3 (next-int random 10)))))
        name-pool (vec (repeatedly 64 #(next-string random (+ 6 (next-int random 20)))))]
    (letfn [(items-for-feed [{feed-id :xt/id}]
                            (let [subs* (get subs-by-feed feed-id)
                                  sub-ids (when (seq subs*) (mapv :xt/id subs*))
                                  sub-count (count sub-ids)
                                  k (min (draw-lognormal-poisson random feed-items-stats) n-items)]
                              (map (fn [i]
                                     (let [raw-id (next-uuid random)
                                           email-sub (when (and (pos? sub-count) (chance? random 0.2))
                                                       (uniform-nth random sub-ids))
                                           prefix (or feed-id email-sub #uuid "00000000-0000-0000-0000-000000000000")
                                           id (uuid-with-prefix prefix raw-id)
                                           categories (vec (repeatedly (next-int random 5) #(uniform-nth random cat-pool)))]
                                       (cond-> {:xt/id id
                                                :item.feed/feed feed-id
                                                :item/title (str "Item " feed-id "-" i)
                                                :item/excerpt (next-string random (+ 40 (next-int random 120)))
                                                :item/published-at (next-instant random)
                                                :item/length (+ 100 (next-int random 2000))
                                                :item/content-key (next-uuid random)
                                                :item/lang (uniform-nth random langs)
                                                :item/author-name (uniform-nth random name-pool)
                                                :item/author-email (next-email random)
                                                :item/author-url (str "https://example.com/author/" i)
                                                :item/categories categories
                                                :item/ingested-at (next-instant random)
                                                :item/url (next-string random (+ 10 (next-int random 50)))}
                                         (chance? random 0.01) (assoc :item/doc-type :item/direct
                                                                      :item.direct/candidate-status (weighted-nth random [0.9 0.08 0.02] [:approved :failed :ingest-failed]))
                                         email-sub (assoc :item.email/sub email-sub))))
                                   (range k))))]
      (->> feeds
           cycle
           (mapcat items-for-feed)
           (take n-items)
           vec))))

(defn gen-user-items
  [random users items]
  (let [item-ids (mapv :xt/id items)
        n-items (count item-ids)]
    (->> users
         (mapcat (fn [{user :xt/id
                       user-k :user/k}]
                   (let [k (min user-k n-items)
                         chosen (->> (repeatedly #(next-int random n-items))
                                     (distinct)
                                     (take k)
                                     (map item-ids))]
                     (for [item chosen]
                       (cond-> {:xt/id (uuid-with-prefix user (next-uuid random))
                                :user-item/user user
                                :user-item/item item}
                         (chance? random 0.5) (assoc :user-item/viewed-at (next-instant random))
                         (chance? random 0.2) (assoc :user-item/favorited-at (next-instant random))
                         (chance? random 0.1) (assoc :user-item/bookmarked-at (next-instant random))
                         (chance? random 0.1) (assoc :user-item/skipped-at (next-instant random)))))))
         vec)))

(defn gen-ads
  [random users n]
  (vec
   (for [_ (range n)]
     {:xt/id (next-uuid random)
      :ad/user (:xt/id (uniform-nth random users))
      :ad/approve-state (uniform-nth random [:approved :pending :rejected])
      :ad/paused (chance? random 0.2)
      :ad/title (next-string random (+ 12 (next-int random 24)))
      :ad/description (next-string random (+ 180 (next-int random 360)))
      :ad/url (str "https://ads.example/" (next-string random 16))
      :ad/image-url (str "https://img.example/" (next-string random 24))
      :ad/payment-method (next-string random (+ 24 (next-int random 24)))
      :ad/card-details {:brand (next-string random 6)
                        :last4 (format "%04d" (mod (next-int random 10000) 10000))
                        :exp-month (inc (next-int random 12))
                        :exp-year (+ 2025 (next-int random 6))}
      :ad/customer-id (next-string random (+ 18 (next-int random 10)))
      :ad/payment-failed (chance? random 0.1)
      :ad/budget (+ 100 (next-int random 2000))
      :ad/balance (+ (next-int random 500) 0)
      :ad/bid (max 1 (next-int random 200))
      :ad/recent-cost (max 0 (next-int random 1000))
      :ad/updated-at (next-instant random)})))

(defn gen-ad-credits
  [random ads n]
  (vec
   (for [_ (range n)]
     {:xt/id (next-uuid random)
      :ad.credit/ad (:xt/id (uniform-nth random ads))
      :ad.credit/cents (+ 100 (next-int random 10000))
      :ad.credit/created-at (next-instant random)
      :ad.credit/charge-status (if (chance? random 0.9) :confirmed :failed)
      :ad.credit/source (if (chance? random 0.9) :charge :manual)
      :ad.credit/amount (+ 100 (next-int random 1000))})))

(defn gen-ad-clicks
  [random ads users n]
  (vec
   (for [_ (range n)]
     {:xt/id (next-uuid random)
      :ad.click/ad (:xt/id (uniform-nth random ads))
      :ad.click/user (:xt/id (uniform-nth random users))
      :ad.click/created-at (next-instant random)
      :ad.click/url (str "https://clk.example/" (next-string random 12))
      :ad.click/cost (next-int random 1000)})))

(defn gen-digests
  [random users n]
  (vec
   (for [_ (range n)]
     {:xt/id (next-uuid random)
      :digest/user (:xt/id (uniform-nth random users))
      :digest/sent-at (next-instant random)
      :digest/subject (next-uuid random)
      :digest/discover (next-uuid random)
      :digest/count (+ 1 (next-int random 200))})))

(defn gen-bulk-sends
  [random digests n]
  (vec
   (for [_ (range n)]
     {:xt/id (next-uuid random)
      :bulk-send/sent-at (next-instant random)
      :bulk-send/digests (vec (repeatedly (next-int random 10) #(uniform-nth random digests)))
      :bulk-send/mailersend-id (next-string random 24)
      :bulk-send/num-users (+ 1 (next-int random 10000))
      :bulk-send/payload-size (+ 1 (next-int random 10000))})))

(defn gen-skips
  [random users n]
  (vec
   (for [_ (range n)]
     {:xt/id (next-uuid random)
      :skip/user (:xt/id (uniform-nth random users))
      :skip/created-at (next-instant random)})))

(defn generate-data
  "Generate synthetic data according to a scale factor and return a map of
   table keyword -> vector of docs"
  [random scale]
  (let [{:keys [users feeds items ads ad-credits ad-clicks digests bulk-sends skips]} baseline-counts
        n-users (round-down (* users scale))
        n-feeds (round-down (* feeds scale))
        n-items (round-down (* items scale))
        n-ads (round-down (* ads scale))
        n-ad-credits (round-down (* ad-credits scale))
        n-ad-clicks (round-down (* ad-clicks scale))
        n-digests (round-down (* digests scale))
        n-bulk-sends (round-down (* bulk-sends scale))
        n-skips (round-down (* skips scale))
        users* (gen-users random n-users)
        feeds* (gen-feeds random n-feeds)
        subs* (gen-subs random users* feeds*)
        items* (gen-items random feeds* subs* n-items)
        user-items* (gen-user-items random users* items*)
        ads* (gen-ads random users* n-ads)
        ad-credits* (gen-ad-credits random ads* n-ad-credits)
        ad-clicks* (gen-ad-clicks random ads* users* n-ad-clicks)
        digests* (gen-digests random users* n-digests)
        bulk-sends* (gen-bulk-sends random digests* n-bulk-sends)
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
