(ns xtdb.bench.auctionmark
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.time :as time])
  (:import (java.time Duration)
           (java.util Map UUID)
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicLong]))

(defn random-price [worker] (.nextDouble (b/rng worker)))

(defn str->uuid ^java.util.UUID [^String s]
  (UUID/nameUUIDFromBytes (.getBytes s)))

(defn uuid->msb [^UUID u]
  (.getMostSignificantBits u))

(def ->user-id (comp str->uuid (partial str "u_")))

(defn- inc-count! [{:keys [^Map !entity-counts]} k]
  (b/inc-count! (.computeIfAbsent !entity-counts k (fn [_] (AtomicLong. 0)))))

(defn- set-count! [{:keys [^Map !entity-counts]} k v]
  (b/set-count! (.computeIfAbsent !entity-counts k (fn [_] (AtomicLong. 0))) v))

(defn random-id [{:keys [^Map !entity-counts] :as worker} k]
  (b/sample-flat worker (.get !entity-counts k)))

(defn gaussian-random-id [{:keys [^Map !entity-counts] :as worker} k]
  (b/sample-gaussian worker (.get !entity-counts k)))

(defn generate-user [worker]
  {:xt/id (->user-id (inc-count! worker :users))
   :region-id (random-id worker :regions)
   :rating 0
   :balance 0.0
   :created (b/current-timestamp worker)
   :sattr0 (b/random-str worker)
   :sattr1 (b/random-str worker)
   :sattr2 (b/random-str worker)
   :sattr3 (b/random-str worker)
   :sattr4 (b/random-str worker)
   :sattr5 (b/random-str worker)
   :sattr6 (b/random-str worker)
   :sattr7 (b/random-str worker)})

(defn proc-new-user
  "Creates a new USER record. The rating and balance are both set to zero.

  The benchmark randomly selects id from a pool of region ids as an input for u_r_id parameter using flat distribution."
  [{:keys [node] :as worker}]
  (xt/execute-tx node [[:put-docs :user (generate-user worker)]]))

(defn- sample-category-id [{:keys [category-weighting] :as worker}]
  (if category-weighting
    (category-weighting (b/rng worker))
    (gaussian-random-id worker :categories)))

(defn sample-status [worker]
  (nth [:open :waiting-for-purchase :closed]
       (.nextInt (b/rng worker) 3)))

(defn- generate-item-description [{:keys [node] :as worker}]
  (loop [[gag-id & gag-ids] (->> (b/random-seq worker {:min 0, :max 16, :unique true} random-id :gags)
                                 (remove nil?))
         [gav-id & gav-ids] (->> (b/random-seq worker {:min 0, :max 16, :unique true} random-id :gavs)
                                 (remove nil?))
         res []]
    (if (and gag-id gav-id)
      (let [{:keys [gag-name gav-name]}
            (first (xt/q node
                         ["SELECT gag.name gag_name, gav.name gav_name
                           FROM gag, gav
                           WHERE gav._id = ? AND gag._id = ? AND gav.gag_id = gag._id"
                          gav-id gag-id]))]
        (recur gag-ids gav-ids (into res [gag-name gav-name])))
      (str (b/random-str worker) " " (str/join " " res)))))

(defn proc-new-item
  "Insert a new ITEM record for a user.

  The benchmark client provides all the preliminary information required for the new item, as well as optional information to create derivative image and attribute records.
  After inserting the new ITEM record, the transaction then inserts any GLOBAL ATTRIBUTE VALUE and ITEM IMAGE.

  After these records are inserted, the transaction then updates the USER record to add the listing fee to the seller’s balance.

  The benchmark randomly selects id from a pool of users as an input for user-id parameter using Gaussian distribution.
  A category-id parameter is randomly selected using a flat histogram from the real auction site’s item category statistic."
  [{:keys [node] :as worker}]

  (let [seller-id (->user-id (gaussian-random-id worker :users))
        item-id (UUID. (uuid->msb seller-id) (inc-count! worker :items))

        start-date (time/->zdt (b/current-timestamp worker))
        end-date (.plusDays start-date (inc (.nextInt (b/rng worker) 42)))]

    (xt/execute-tx node (concat
                         [[:put-docs :item
                           {:xt/id item-id
                            :seller-id seller-id
                            :category-id (sample-category-id worker)
                            :item-name (b/random-str worker)
                            :item-description (generate-item-description worker)
                            :user-attributes (b/random-str worker)
                            :initial-price (random-price worker)
                            :bid-count 0
                            :global-attr-count (count (->> (b/random-seq worker {:min 0, :max 16, :unique true} random-id :gavs)
                                                           (remove nil?)))
                            :start-date start-date
                            :end-date end-date

                            :images (b/random-seq worker {:min 0, :max 16, :unique true} b/random-str)

                            :status :open}]]

                         (when seller-id
                           [[:sql "UPDATE user SET balance = balance - 1 WHERE _id = ? "
                             [seller-id]]])))))

(defn random-item
  ([worker] (random-item worker {}))

  ([{:keys [!items-by-status] :as worker} {:keys [status] :or {status :all}}]
   (b/random-nth worker (vec (get @!items-by-status status)))))

(defn generate-new-bid-params [worker]
  (loop []
    (let [{:keys [item-id seller-id]} (random-item worker {:status :open})]
      (when-let [bidder-id (->user-id (gaussian-random-id worker :users))]
        (if (= bidder-id seller-id)
          (recur)
          {:item-id item-id,
           :bidder-id bidder-id
           :new-curr-bid (random-price worker)
           :new-max-bid (random-price worker)
           :new-bid-id (UUID. (uuid->msb item-id) (inc-count! worker :item-bids))})))))

(defn proc-new-bid [{:keys [node] :as worker}]
  (when-let [{:keys [item-id bidder-id new-curr-bid new-max-bid new-bid-id]} (generate-new-bid-params worker)]
    (let [{:keys [max-bid-id]} (first (xt/q node ["SELECT max_bid_id, curr_bid FROM item_max_bid WHERE _id = ?" item-id]))
          {:keys [curr-max-bid]} (first (xt/q node ["SELECT max_bid AS curr_max_bid FROM item_bid WHERE _id = ?" max-bid-id]))]

      (xt/execute-tx node
                     [[:put-docs :item-bid
                       {:xt/id new-bid-id
                        :item-id item-id
                        :bidder-id bidder-id
                        :max-bid new-max-bid}]

                      (cond
                        ;; no previous max bid, insert new max bid
                        (nil? max-bid-id)
                        [:put-docs :item-max-bid
                         {:xt/id item-id
                          :max-bid-id new-bid-id
                          :curr-bid new-curr-bid}]

                        ;; we exceed the old max, win the bid.
                        (< curr-max-bid new-max-bid)
                        [:patch-docs :item-max-bid
                         {:xt/id item-id, :curr-bid new-curr-bid, :max-bid-id new-bid-id}]

                        ;; bump the curr-bid
                        :else
                        [:patch-docs :item-max-bid
                         {:xt/id item-id, :curr-bid (+ new-max-bid 0.01)}])]))))

(defn proc-new-comment [{:keys [node !item-comment-ids] :as worker}]
  (when-let [{:keys [item-id]} (random-item worker {:status :open})]
    (let [item-comment-id (UUID. (uuid->msb item-id) (inc-count! worker :item-comments))]
      (swap! !item-comment-ids (fnil conj []) item-comment-id)
      (xt/execute-tx node [[:put-docs :item-comment
                            {:xt/id item-comment-id
                             :item-id item-id
                             :author-id (->user-id (random-id worker :users))
                             :commented-at (b/current-timestamp worker)
                             :comment (b/random-str worker)}]]))))

(defn proc-new-comment-response [{:keys [node !item-comment-ids] :as worker}]
  (when-let [comment-id (b/random-nth worker (vec @!item-comment-ids))]
    (xt/execute-tx node [[:patch-docs :item-comment {:xt/id comment-id, :response (b/random-str worker)}]])))

(defn proc-new-purchase [{:keys [node] :as worker}]
  (when-let [{:keys [item-id]} (random-item worker {:status :waiting-for-purchase})]
    (xt/execute-tx node [["UPDATE item SET status = ?, purchased_at = ? WHERE _id = ?"
                          :closed (b/current-timestamp worker) item-id]])))

(defn proc-new-feedback [{:keys [node] :as worker}]
  (when-let [{:keys [item-id]} (random-item worker {:status :closed})]
    ;; TODO verify buyer-id
    (xt/execute-tx node [[:put-docs :item-feedback
                          {:xt/id item-id
                           :rating (b/random-nth worker [-1 0 1])
                           :created-at (b/current-timestamp worker)
                           :comment (b/random-str worker)}]])))

(defn proc-get-item [{:keys [node] :as worker}]
  (let [{:keys [item-id]} (random-item worker {:status :open})]
    (xt/q node ["SELECT _id, seller_id, name, end_date, status FROM item WHERE _id = ?" item-id])))

(defn proc-update-item [{:keys [node] :as worker}]
  (let [{:keys [item-id]} (random-item worker {:status :open})
        description (b/random-str worker)]
    (xt/execute-tx node [[:sql "UPDATE item SET description = ? WHERE _id = ?" [description item-id]]])))

(defn proc-post-auction [{:keys [node]} due-items]
  (let [{items-with-bid true, items-without-bid false} (group-by (comp some? :buyer-id) due-items)]
    (xt/execute-tx node
                   (cond-> []
                     (seq items-without-bid)
                     (conj (into [:patch-docs :item]
                                 (for [{:keys [item-id]} items-without-bid]
                                   {:xt/id item-id, :status :closed})))

                     (seq items-with-bid)
                     (conj (into [:patch-docs :item]
                                 (for [{:keys [buyer-id item-id]} items-with-bid]
                                   {:xt/id item-id, :buyer-id buyer-id, :status :waiting-for-purchase})))))))

(defn proc-check-winning-bids [{:keys [node] :as worker}]
  (let [now (b/current-timestamp worker)
        now-minus-60s (.minusSeconds now 60)]
    (proc-post-auction worker
                       (vec (for [{:keys [item-id] :as due-item} (xt/q node ["SELECT _id AS item_id FROM item WHERE (start_date BETWEEN ? AND ?) AND status = ? ORDER BY _id ASC LIMIT 100"
                                                                             now-minus-60s now :open])]
                              (if-let [{:keys [buyer-id]} (first
                                                           (xt/q node ["SELECT ib.bidder_id AS buyer_id
                                                                   FROM item_max_bid AS imb
                                                                     JOIN item_bid AS ib ON (ib._id = imb.max_bid_id)
                                                                   WHERE imb._id = ?"
                                                                       item-id]))]
                                (assoc due-item :buyer-id buyer-id)
                                due-item))))))

(defn proc-get-comment [{:keys [node] :as worker}]
  (let [{:keys [item-id]} (random-item worker {:status :open})]
    (xt/q node ["SELECT * FROM item_comment WHERE item_id = ? AND response IS NULL"
                item-id])))

(defn get-user-info [node user-id {:keys [seller-items? buyer-items? feedback?]}]
  [(xt/q node ["SELECT u._id user_id, rating, created, balance,
                       sattr0, sattr1, sattr2, sattr3, sattr4, r.name AS region
                FROM user u JOIN region r ON (u.region_id = r._id)
                WHERE u._id = ?"
               user-id])

   #_
   (cond
     seller-items?
     (xt/q node ["SELECT _id, user_id, name, current_price, num_bids, end_date, status
                  FROM item
                  WHERE user_id = ?
                  ORDER BY end_date DESC
                  LIMIT 20"
                 user-id])

     (and buyer-items? (not seller-items?))
     (xt/q node ["SELECT _id, user_id, name, current_price, num_bids, end_date, status
                  FROM user_item AS ui JOIN item i ON (ui.item_id = i._id AND ui.user_id = i.user_id)
                  WHERE ui.user_id = ?
                  ORDER BY i.end_date DESC
                  LIMIT 20"
                 user-id]))

   #_
   (when feedback?
     (xt/q node ["SELECT if.rating, if.comment, if.date,
                         i._id item_id, i.name, i.end_date, i.status,
                         u._id user_id, u.rating, u.sattr0, u.sattr1
                  FROM item_feedback AS if
                    JOIN item i ON (if.item_id = i._id)
                    JOIN user u ON (i.user_id = u._id)
                  WHERE if.buyer_id = ?
                  ORDER BY if.date DESC
                  LIMIT 10"
                 user-id]))])

(defn proc-get-user-info [{:keys [node] :as worker}]
  (get-user-info node (->user-id (random-id worker :users))
                 {:seller-items? (b/random-bool worker)
                  :buyer-items? (b/random-bool worker)
                  :feedback? (b/random-bool worker)}))

(defn items-by-status [node]
  (let [all (vec (xt/plan-q node '(from :item [{:xt/id item-id} status])))]
    (into {:all all} (group-by :status all))))

(defn add-item-status! [{:keys [!items-by-status]} {:keys [status] :as item-sample}]
  (swap! !items-by-status
         (fn [items]
           (-> items
               (update :all (fnil conj []) item-sample)
               (update status (fnil conj []) item-sample)))))

;; do every now and again to provide inputs for item-dependent computations
(defn index-item-status-groups! [{:keys [node !items-by-status]}]
  (reset! !items-by-status (items-by-status node)))

(defn read-category-tsv []
  (let [cat-tsv-rows (with-open [rdr (io/reader (io/resource "data/auctionmark/auctionmark-categories.tsv"))]
                       (vec (for [line (line-seq rdr)
                                  :let [split (str/split line #"\t")]]
                              {:parts (->> (butlast split) (into [] (remove str/blank?)))
                               :item-count (parse-long (last split))})))
        extract-cats (fn extract-cats [parts]
                       (when (seq parts)
                         (cons parts (extract-cats (pop parts)))))
        all-paths (into #{} (comp (map :parts) (mapcat extract-cats)) cat-tsv-rows)
        path-i (into {} (map-indexed (fn [i x] [x i])) all-paths)
        trie (reduce #(assoc-in %1 (:parts %2) (:item-count %2)) {} cat-tsv-rows)
        trie-node-item-count (fn trie-node-item-count [path]
                               (let [n (get-in trie path)]
                                 (if (integer? n)
                                   n
                                   (reduce + 0 (map trie-node-item-count (keys n))))))]
    (->> (for [[path i] path-i]
           [i
            {:i i
             :xt/id i
             :category-name (str/join "/" path)
             :parent (path-i i)
             :item-count (trie-node-item-count path)}])
         (into {}))))

(defn generate-region [worker]
  {:xt/id (inc-count! worker :regions)
   :name (b/random-str worker 6 32)})

(defn generate-global-attribute-group [worker]
  {:xt/id (inc-count! worker :gags)
   :category-id (random-id worker :categories)
   :name (b/random-str worker 6 32)})

(defn generate-global-attribute-value [worker]
  {:xt/id (inc-count! worker :gavs)
   :gag-id (random-id worker :gags)
   :name (b/random-str worker 6 32)})

(defn generate-category [{:keys [categories] :as worker}]
  (let [cat-id (inc-count! worker :categories)
        {:keys [category-name parent]} (categories cat-id)]
    {:xt/id cat-id
     :parent-id (when parent (:xt/id (categories parent)))
     :name (or category-name (b/random-str worker 6 32))}))

(defn generate-user-attributes [worker]
  (when-let [user-id (->user-id (random-id worker :users))]
    {:xt/id (UUID. (uuid->msb user-id) (inc-count! worker :user-attributes))
     :user-id user-id
     :name (b/random-str worker 5 32)
     :value (b/random-str worker 5 32)
     :created (b/current-timestamp worker)}))

(defn generate-item [worker]
  (let [seller-id (->user-id (random-id worker :users))
        item-id (UUID. (uuid->msb seller-id) (inc-count! worker :items))
        cat-id (sample-category-id worker)
        start-date (b/current-timestamp worker)
        end-date (-> (time/->zdt (b/current-timestamp worker)) (.plusDays 32))
        status (sample-status worker)]
    (when seller-id
      (add-item-status! worker {:item-id item-id, :seller-id seller-id, :status status})

      {:xt/id item-id
       :item-id item-id
       :seller-id seller-id
       :category-id cat-id
       :name (b/random-str worker 6 32)
       :description (b/random-str worker 50 255)
       :user-attributes (b/random-str worker 20 255)
       :initial-price (random-price worker)
       :global-attr-count 0
       :start-date start-date
       :end-date end-date
       :status status})))

(defn load-phase-submit-tasks [sf]
  [{:t :call, :f [b/generate :region generate-region 75]}
   {:t :call, :f [b/generate :category generate-category 16908]}
   {:t :call, :f [b/generate :user generate-user (* sf 1e6)]}
   {:t :call, :f [b/generate :user-attribute generate-user-attributes (* sf 1e6 1.3)]}
   {:t :call, :f [b/generate :item generate-item (* sf 1e6 10)]}
   {:t :call, :f [b/generate :gag generate-global-attribute-group 100]}
   {:t :call, :f [b/generate :gav generate-global-attribute-value 1000]}
   {:t :call, :f (comp b/sync-node :node)}])

(defn row-count [node table]
  (-> (xt/q node (xt/template
                  (-> (from ~table [xt/id])
                      #_{:clj-kondo/ignore [:invalid-arity]}
                      (aggregate {:count (row-count)}))))
      first
      :count))

(defn load-stats-into-worker [{:keys [node]
                               :as worker}]
  (index-item-status-groups! worker)
  (set-count! worker :regions (row-count node :region))
  (set-count! worker :gags (row-count node :gag))
  (set-count! worker :gavs (row-count node :gav))
  (set-count! worker :categorys (row-count node :category))
  (set-count! worker :users (row-count node :user))
  (set-count! worker :user-attributes (row-count node :user-attribute))
  (set-count! worker :items (row-count node :item))
  (set-count! worker :item-comments (row-count node :item-comment))
  (set-count! worker :item-feedbacks (row-count node :item-feedback))
  (set-count! worker :item-bids (row-count node :item-bid)))

(defmethod b/cli-flags :auctionmark [_]
  [["-d" "--duration DURATION"
    :parse-fn #(Duration/parse %)
    :default #xt/duration "PT30S"]

   ["-s" "--scale-factor SCALE_FACTOR"
    :parse-fn parse-double
    :default 0.01]

   ["-t" "--threads THREADS"
    :parse-fn parse-long
    :default (max 1 (/ (.availableProcessors (Runtime/getRuntime)) 2))]

   [nil "--only-load" "Runs the load phase, then exits"
    :id :only-load?]

   ["-h" "--help"]])

(defn ->initial-state []
  (let [cats (read-category-tsv)]
    {:!entity-counts (ConcurrentHashMap.)
     :categories cats
     :category-weighting (b/weighted-sample-fn (map (juxt :xt/id :item-count) (vals cats)))
     :!item-comment-ids (atom [])
     :!items-by-status (atom {})}))

(defmethod b/->benchmark :auctionmark [_ {:keys [seed threads duration scale-factor no-load? only-load? sync]
                                          :or {seed 0, threads 8, sync false
                                               duration "PT30S", scale-factor 0.1}}]
  (let [^Duration duration (cond-> duration (string? duration) Duration/parse)]
    (log/info {:scale-factor scale-factor :no-load? no-load? :only-load? only-load? :duration duration :seed seed})
    {:title "Auction Mark OLTP"
     :seed seed
     :parameters {:scale-factor scale-factor
                  :no-load? no-load?
                  :only-load? only-load?
                  :duration duration
                  :seed seed
                  :threads threads
                  :sync sync}
     :->state ->initial-state
     :tasks (concat (when-not no-load?
                      [{:t :do
                        :stage :load
                        :tasks (load-phase-submit-tasks scale-factor)}])

                    (when-not only-load?
                      [{:t :do
                        :stage :setup-worker
                        :tasks [;; wait for node to catch up
                                {:t :call, :f (fn [{:keys [node] :as worker}]
                                                (when no-load?
                                                  (b/sync-node node)
                                                  (load-stats-into-worker worker)))}]}

                       {:t :concurrently
                        :stage :oltp
                        :duration duration
                        :join-wait (Duration/ofMinutes 1)
                        :thread-tasks [{:t :pool
                                        :duration duration
                                        :join-wait (Duration/ofMinutes 1)
                                        :thread-count threads
                                        :think Duration/ZERO
                                        :pooled-task {:t :pick-weighted
                                                      :choices [[5.0 {:t :call, :transaction :new-user, :f (b/wrap-in-catch proc-new-user)}]
                                                                [10.0 {:t :call, :transaction :new-item, :f (b/wrap-in-catch proc-new-item)}]
                                                                [18.0 {:t :call, :transaction :new-bid, :f (b/wrap-in-catch proc-new-bid)}]
                                                                [2.0 {:t :call, :transaction :new-comment, :f (b/wrap-in-catch proc-new-comment)}]
                                                                [1.0 {:t :call, :transaction :new-comment-response, :f (b/wrap-in-catch proc-new-comment-response)}]
                                                                [2.0 {:t :call, :transaction :new-purchase, :f (b/wrap-in-catch proc-new-purchase)}]
                                                                [3.0 {:t :call, :transaction :new-feedback, :f (b/wrap-in-catch proc-new-feedback)}]
                                                                [45.0 {:t :call, :transaction :get-item, :f (b/wrap-in-catch proc-get-item)}]
                                                                [2.0 {:t :call, :transaction :update-item, :f (b/wrap-in-catch proc-update-item)}]
                                                                [2.0 {:t :call, :transaction :get-comment, :f (b/wrap-in-catch proc-get-comment)}]
                                                                [10.0 {:t :call, :transaction :get-user-info, :f (b/wrap-in-catch proc-get-user-info)}]]}}
                                       {:t :freq-job
                                        :duration duration
                                        :freq (Duration/ofMillis (* 0.2 (.toMillis duration)))
                                        :job-task {:t :call, :transaction :index-item-status-groups, :f (b/wrap-in-catch index-item-status-groups!)}}]}
                       (when sync {:t :call, :f (comp b/sync-node :node)})]))}))
