(ns core2.bench2.auctionmark
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [core2.datalog :as c2]
            [core2.bench2 :as b2]
            [core2.bench2.core2 :as bcore2]
            [core2.test-util :as tu])
  (:import (java.time Duration Instant)
           (java.util ArrayList Random)
           (java.util.concurrent ConcurrentHashMap)))

(defn random-price [worker] (.nextDouble (b2/rng worker)))

(def user-id (partial str "u_"))
(def region-id (partial str "r_"))
(def item-id (partial str "i_"))
(def item-bid-id (partial str "ib_"))
(def category-id (partial str "c_"))
(def global-attribute-group-id (partial str "gag_"))
(def gag-id global-attribute-group-id)
(def global-attribute-value-id (partial str "gav_"))
(def gav-id global-attribute-value-id)

(def user-attribute-id (partial str "ua_"))
(def item-name (b2/id))
(def item-description (b2/id))
(def initial-price (b2/id))
(def reserve-price (b2/id))
(def buy-now (b2/id))
(def item-attributes-blob (b2/id))
(def item-image-path (b2/id))
(def auction-start-date (b2/id))

(defn composite-id [& ids] (apply str (butlast (interleave ids (repeat "-")))))

(defn generate-user [worker]
  (let [u_id (b2/increment worker user-id)]
    {:id u_id
     :u_id u_id
     :u_r_id (b2/sample-flat worker region-id)
     :u_rating 0
     :u_balance 0.0
     :u_created (b2/current-timestamp worker)
     :u_sattr0 (b2/random-str worker)
     :u_sattr1 (b2/random-str worker)
     :u_sattr2 (b2/random-str worker)
     :u_sattr3 (b2/random-str worker)
     :u_sattr4 (b2/random-str worker)
     :u_sattr5 (b2/random-str worker)
     :u_sattr6 (b2/random-str worker)
     :u_sattr7 (b2/random-str worker)}))

(defn proc-new-user
  "Creates a new USER record. The rating and balance are both set to zero.

  The benchmark randomly selects id from a pool of region ids as an input for u_r_id parameter using flat distribution."
  [worker]
  (->> [[:put
         'user
         (generate-user worker)]]
       (c2/submit-tx (:sut worker))))

(def tx-fn-apply-seller-fee
  #c2/clj-form
  (fn apply-seller-fee [u_id]
    (let [[u] (q '{:find [id u_id u_r_id u_rating u_balance u_created
                          u_sattr0 u_sattr1 u_sattr2 u_sattr3 u_sattr4 u_sattr5 u_sattr6 u_sattr7]
                   :in [u_id]
                   :where [(match user [id])
                           [id :u_id u_id]
                           [id :u_r_id u_r_id]
                           [id :u_rating u_rating]
                           [id :u_balance u_balance]
                           [id :u_created u_created]
                           [id :u_sattr0 u_sattr0]
                           [id :u_sattr1 u_sattr1]
                           [id :u_sattr2 u_sattr2]
                           [id :u_sattr3 u_sattr3]
                           [id :u_sattr4 u_sattr4]
                           [id :u_sattr5 u_sattr5]
                           [id :u_sattr6 u_sattr6]
                           [id :u_sattr7 u_sattr7]]}
                 u_id)]
      (if u
        [[:put 'user (update u :u_balance dec)]]
        []))))

(def item-query
  '{:find [id i_u_id i_c_id i_name i_description i_user_attributes i_initial_price
           i_current_price i_num_bids i_num_images i_num_global_attrs i_start_date
           i_end-date]
    :in [i]
    :where
    [(match item [id {:id i}])
     [i :i_u_id i_u_id]
     [i :i_c_id i_c_id]
     [i :i_name i_name]
     [i :i_description i_description]
     [i :i_user_attributes i_user_attributes]
     [i :i_initial_price i_initial_price]
     [i :i_current_price i_current_price]
     [i :i_num_bids i_num_bids]
     [i :i_num_images i_num_images]
     [i :i_num_global_attrs i_num_global_attrs]
     [i :i_start_date i_start_date]
     [i :i_end_date i_end-date]]})

(def item-max-bid-query
  '{:find [id imb_i_id imb_u_id imb_ib_id imb_ib_i_id imb_ib_u_id imb_created]
    :in [imb]
    :where [(match item-max-bid [id {:id imb}])
            [imb :imb_i_id imb_i_id]
            [imb :imb_u_id imb_u_id]
            [imb :imb_ib_id imb_ib_id]
            [imb :imb_ib_i_id imb_ib_i_id]
            [imb :imb_ib_u_id imb_ib_u_id]
            [imb :imb_created imb_created]
            [imb :imb_updated imb_updated]]})

(def tx-fn-new-bid
  "Transaction function.

  Enters a new bid for an item"
  #c2/clj-form
  (fn new-bid [{:keys [i_id
                       u_id
                       i_buyer_id
                       bid
                       max-bid
                       ;; pass in from ctr rather than select-max+1 so ctr gets incremented
                       new-bid-id
                       ;; 'current timestamp'
                       now] :as params}]
    (let [;; current max bid id
          {:keys [imb imb_ib_id] :as res}
          (-> (quote {:find [imb, imb_ib_id]
                      :in [i_id]
                      :where [(match item-max-bid {:id imb})
                              [imb :imb_i_id i_id]
                              [imb :imb_u_id u_id]
                              [imb :imb_ib_id imb_ib_id]]})
              (q i_id)
              first)
          ;; _ (println res)


          ;; current number of bids
          {:keys [i nbids] :as res}
          (-> (quote {:find [i, nbids]
                      :in [i_id]
                      :where [(match item {:id i})
                              [i :i_id i_id]
                              [i :i_num_bids nbids]
                              [i :i_status :open]]})
              (q i_id)
              first)
          ;; _ (println res)

          ;; current bid/max
          {:keys [curr-bid, curr-max] :as res}
          (when imb_ib_id
            (-> (quote {:find [curr-bid curr-max]
                        :in [imb_ib_id]
                        :where [(match item-bid {:id ib})
                                [ib :ib_id imb_ib_id]
                                [ib :ib_bid curr-bid]
                                [ib :ib_max_bid curr-max]]})
                (q imb_ib_id)
                first))

          new-bid-win (or (nil? imb_ib_id) (< curr-max max-bid))
          new-bid (if (and new-bid-win curr-max (< bid curr-max) curr-max) curr-max bid)
          upd-curr-bid (and curr-bid (not new-bid-win) (< curr-bid bid))
          composite-id-fn (fn [& ids] (apply str (butlast (interleave ids (repeat "-")))))
          item-query '{:find [id i_id i_u_id i_c_id i_name i_description i_user_attributes i_initial_price
                              i_current_price i_num_bids i_num_images i_num_global_attrs i_start_date
                              i_end_date i_status]
                       :in [i]
                       :where [(match item [id {:id i}])
                               [i :i_id i_id]
                               [i :i_u_id i_u_id]
                               [i :i_c_id i_c_id]
                               [i :i_name i_name]
                               [i :i_description i_description]
                               [i :i_user_attributes i_user_attributes]
                               [i :i_initial_price i_initial_price]
                               [i :i_current_price i_current_price]
                               [i :i_num_bids i_num_bids]
                               [i :i_num_images i_num_images]
                               [i :i_num_global_attrs i_num_global_attrs]
                               [i :i_start_date i_start_date]
                               [i :i_end_date i_end_date]
                               [i :i_status i_status]]}
          item-max-bid-query '{:find [id imb_i_id imb_u_id imb_ib_id imb_ib_i_id imb_ib_u_id imb_created]
                               :in [imb]
                               :where [(match item-max-bid [id {:id imb}])
                                       [imb :id id]
                                       [imb :_table :item-max-bid]
                                       [imb :imb_i_id imb_i_id]
                                       [imb :imb_u_id imb_u_id]
                                       [imb :imb_ib_id imb_ib_id]
                                       [imb :imb_ib_i_id imb_ib_i_id]
                                       [imb :imb_ib_u_id imb_ib_u_id]
                                       [imb :imb_created imb_created]
                                       [imb :imb_updated imb_updated]]}]
      (cond-> []
        ;; increment number of bids on item
        i
        (conj [:put 'item (assoc (first (q item-query i))
                                 :i_num_bids (inc nbids))])

        ;; if new bid exceeds old, bump it
        upd-curr-bid
        (conj [:put 'item-max-bid (assoc (first (q item-max-bid-query imb))
                                         :imb_bid bid)])

        ;; we exceed the old max, win the bid.
        (and curr-bid new-bid-win)
        (conj [:put 'item-max-bid (assoc (first (q item-max-bid-query imb))
                                         :imb_ib_id new-bid-id
                                         :imb_ib_u_id u_id
                                         :imb_updated now)])

        ;; no previous max bid, insert new max bid
        (nil? imb_ib_id)
        (conj [:put 'item-max-bid {:id (composite-id-fn new-bid-id i_id)
                                   :imb_i_id i_id
                                   :imb_u_id u_id
                                   :imb_ib_id new-bid-id
                                   :imb_ib_i_id i_id
                                   :imb_ib_u_id u_id
                                   :imb_created now
                                   :imb_updated now}])

        :always
        ;; add new bid
        (conj [:put 'item-bid {:id new-bid-id
                               :ib_id new-bid-id
                               :ib_i_id i_id
                               :ib_u_id u_id
                               :ib_buyer_id i_buyer_id
                               :ib_bid new-bid
                               :ib_max_bid max-bid
                               :ib_created_at now
                               :ib_updated now}])))))

(defn- sample-category-id [worker]
  (if-some [weighting (::category-weighting (:custom-state worker))]
    (weighting (b2/rng worker))
    (b2/sample-gaussian worker category-id)))

(defn sample-status [worker]
  (nth [:open :waiting-for-purchase :closed] (mod (.nextInt ^Random (b2/rng worker)) 3)))

(defn proc-new-item
  "Insert a new ITEM record for a user.

  The benchmark client provides all the preliminary information required for the new item, as well as optional information to create derivative image and attribute records.
  After inserting the new ITEM record, the transaction then inserts any GLOBAL ATTRIBUTE VALUE and ITEM IMAGE.

  After these records are inserted, the transaction then updates the USER record to add the listing fee to the seller’s balance.

  The benchmark randomly selects id from a pool of users as an input for u_id parameter using Gaussian distribution. A c_id parameter is randomly selected using a flat histogram from the real auction site’s item category statistic."
  [worker]
  (let [i_id-raw (.getAndIncrement (b2/counter worker item-id))
        i_id (item-id i_id-raw)
        u_id (b2/sample-gaussian worker user-id)
        c_id (sample-category-id worker)
        name (b2/random-str worker)
        description (b2/random-str worker)
        initial-price (random-price worker)
        attributes (b2/random-str worker)
        gag-ids (remove nil? (b2/random-seq worker {:min 0, :max 16, :unique true} b2/sample-flat global-attribute-group-id))
        gav-ids (remove nil? (b2/random-seq worker {:min 0, :max 16, :unique true} b2/sample-flat global-attribute-value-id))
        images (b2/random-seq worker {:min 0, :max 16, :unique true} b2/random-str)
        start-date (b2/current-timestamp worker)
        ;; up to 42 days
        end-date (.plusSeconds ^Instant start-date (* 60 60 24 (* (inc (.nextInt (b2/rng worker) 42)))))
        ;; append attribute names to desc
        description-with-attributes
        (let [q '{:find [gag-name gav-name]
                  :in [[gag-id ...] [gav-id ...]]
                  :where [(match gag {:id gag-id})
                          [gag-id :gag_name gag-name]
                          (match gav {:id gav-id})
                          [gav-id :gav_gag_id gag-id]
                          [gav-id :gav_name gav-name]]}]
          (->> (c2/q (:sut worker) q gag-ids gav-ids)
               (str/join " ")
               (str description " ")))]

    (->> (concat
          [[:put
            'item
            {:id i_id
             :i_id i_id
             :i_u_id u_id
             :i_c_id c_id
             :i_name name
             :i_description description-with-attributes
             :i_user_attributes attributes
             :i_initial_price initial-price
             :i_num_bids 0
             :i_num_images (count images)
             :i_num_global_attrs (count gav-ids)
             :i_start_date start-date
             :i_end_date end-date
             :i_status :open}]]
          (for [[i image] (map-indexed vector images)
                :let [ii_id (bit-or (bit-shift-left i 60) (bit-and i_id-raw 0x0FFFFFFFFFFFFFFF))]]
            [:put
             'item-comment
             {:id (str "ii_" ii_id)
              :ii_id ii_id
              :ii_i_id i_id
              :ii_u_id u_id
              :ii_path image}])
          ;; fix BitVector metadata issue
          (when u_id [[:call :apply-seller-fee u_id]]))
         (c2/submit-tx (:sut worker)))))

;; represents a probable state of an item that can be sampled randomly
(defrecord ItemSample [i_id, i_u_id, i_status, i_end_date, i_num_bids])

(defn- project-item-status
  [i_status, ^Instant i_end_date, i_num_bids, ^Instant now]
  (let [remaining (- (.toEpochMilli i_end_date) (.toEpochMilli now))
        item-ending-soon-ms (* 36000 1000)]
    (cond
      (<= remaining 0) :closed
      (< remaining item-ending-soon-ms) :ending-soon
      (and (pos? i_num_bids) (not= :closed i_status)) :waiting-for-purchase
      :else i_status)))

(defn item-status-groups [node ^Instant now]
  (let [items (c2/q node '{:find [i, i_id, i_u_id, i_status, i_end_date, i_num_bids]
                           :where [(match item {:id i})
                                   [i :i_id i_id]
                                   [i :i_u_id i_u_id]
                                   [i :i_status i_status]
                                   [i :i_end_date i_end_date]
                                   [i :i_num_bids i_num_bids]]})
        all (ArrayList.)
        open (ArrayList.)
        ending-soon (ArrayList.)
        waiting-for-purchase (ArrayList.)
        closed (ArrayList.)]
    (doseq [{:keys [i_id i_u_id i_status ^Instant i_end_date i_num_bids]} items
            :let [projected-status i_status #_(project-item-status i_status i_end_date i_num_bids now)

                  ^ArrayList alist
                  (case projected-status
                    :open open
                    :closed closed
                    :waiting-for-purchase waiting-for-purchase
                    :ending-soon ending-soon
                    ;; TODO debug why this happens
                    nil)

                  item-sample (->ItemSample i_id i_u_id i_status i_end_date i_num_bids)]]

      (.add all item-sample)
      (when alist
        (.add alist item-sample)))
    {:all (vec all)
     :open (vec open)
     :ending-soon (vec ending-soon)
     :waiting-for-purchase (vec waiting-for-purchase)
     :closed (vec closed)}))

;; do every now and again to provide inputs for item-dependent computations
(defn index-item-status-groups [worker]
  (let [{:keys [sut, ^ConcurrentHashMap custom-state]} worker
        now (b2/current-timestamp worker)
        node sut
        res (item-status-groups node now)]
    (.putAll custom-state {:item-status-groups res #_(item-status-groups node now)})
    #_(with-open [db (xt/open-db sut)]
        (.putAll custom-state {:item-status-groups (item-status-groups db now)}))))

(defn largest-id [node table prefix-length]
  (let [id (->> (c2/q node `{:find [~'id]
                             :where [(match ~table [~'id])
                                     [~'id :_table ~table]]})
                (sort-by :id #(cond (< (count %1) (count %2)) 1
                                    (< (count %2) (count %1)) -1
                                    :else (compare %2 %1)))
                first
                :id)]
    (when id
      (parse-long (subs id prefix-length)))))

(defn load-stats-into-worker [{:keys [sut] :as worker}]
  (index-item-status-groups worker)
  (log/info "query for user")
  (b2/set-domain worker user-id (or (largest-id sut :user 2) 0))
  (log/info "query for region")
  (b2/set-domain worker region-id (or (largest-id sut :region 2) 0))
  (log/info "query for item")
  (b2/set-domain worker item-id (or (largest-id sut :item 2) 0))
  (log/info "query for item-bid")
  (b2/set-domain worker item-bid-id (or (largest-id sut :item-bid 3) 0))
  (log/info "query for category")
  (b2/set-domain worker category-id (or (largest-id sut :category 2) 0))
  (log/info "query for gag")
  (b2/set-domain worker gag-id (or (largest-id sut :gag 4) 0))
  (log/info "query for gav")
  (b2/set-domain worker gav-id (or (largest-id sut :gav 4) 0)))

(defn log-stats [worker]
  (log/info "#user " (.get (b2/counter worker user-id)))
  (log/info "#region " (.get (b2/counter worker region-id)))
  (log/info "#item " (.get (b2/counter worker item-id)))
  (log/info "#item-bid " (.get (b2/counter worker item-bid-id)))
  (log/info "#category " (.get (b2/counter worker category-id)))
  (log/info "#gag " (.get (b2/counter worker gag-id)))
  (log/info "#gav " (.get (b2/counter worker gav-id))))


(defn random-item [worker & {:keys [status] :or {status :all}}]
  (let [isg (-> worker :custom-state :item-status-groups (get status) vec)
        item (b2/random-nth worker isg)]
    item))

(defn add-item-status [{:keys [^ConcurrentHashMap custom-state] :as worker}
                       {:keys [i_status] :as item-sample}]
  (.putAll custom-state {:item-status-groups (-> custom-state :item-status-groups
                                                 (update :all (fnil conj []) item-sample)
                                                 (update i_status (fnil conj []) item-sample))}))

(defn generate-new-bid-params [worker]
  (let [{:keys [i_id, i_u_id]} (random-item worker :status :open)
        i_buyer_id (b2/sample-gaussian worker user-id)]
    (if (and i_buyer_id (= i_buyer_id i_u_id))
      (generate-new-bid-params worker)
      {:i_id i_id,
       :u_id i_u_id,
       :i_buyer_id i_buyer_id
       :bid (random-price worker)
       :max-bid (random-price worker)
       :new-bid-id (b2/increment worker item-bid-id)
       :now (b2/current-timestamp worker)})))

(defn proc-new-bid [worker]
  (let [params (generate-new-bid-params worker)]
    (when (and (:i_id params) (:u_id params))
      (c2/submit-tx (:sut worker) [[:call :new-bid params]]))))

(defn- pull-query [table attributes]
  (-> {:find (into '[id] (map symbol attributes))
       :in '[id]}
      (assoc :where
             (->>
              (mapv #(vector 'id % (symbol %)) attributes)
              (into [['id :_table table]])))))

(comment
  (pull-query :item [:i_id, :i_u_id, :i_initial_price, :i_current_price]))

(defn proc-get-item [worker]
  (let [{:keys [sut]} worker
        ;; the benchbase project uses a profile that keeps item pairs around
        ;; selects only closed items for a particular user profile (they are sampled together)
        ;; right now this is a totally random sample with one less join than we need.
        {:keys [i_id]} (random-item worker :status :open)
        ;; _ (log/info "id:" i_id)
        ;; i_id (b2/sample-flat worker item-id)
        q '{:find [i_id i_u_id i_initial_price i_current_price]
            #_(pull ?i [:i_id, :i_u_id, :i_initial_price, :i_current_price])
            :in [i_id]
            :where [(match item {:id i_id})
                    [i_id :_table :item]
                    ;; [?i :i_id i_id]
                    [i_id :i_status :open]
                    [i_id :i_u_id i_u_id]
                    [i_id :i_initial_price i_initial_price]
                    [i_id :i_current_price i_current_price]]}]
    (c2/q sut q i_id)))

(defn read-category-tsv []
  (let [cat-tsv-rows
        (with-open [rdr (io/reader (io/resource "data/auctionmark/auctionmark-categories.tsv"))]
          (vec (for [line (line-seq rdr)
                     :let [split (str/split line #"\t")
                           cat-parts (butlast split)
                           item-count (last split)
                           parts (remove str/blank? cat-parts)]]
                 {:parts (vec parts)
                  :item-count (parse-long item-count)})))
        extract-cats
        (fn extract-cats [parts]
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
           [(category-id i)
            {:i i
             :id (category-id i)
             :category-name (str/join "/" path)
             :parent (category-id (path-i i))
             :item-count (trie-node-item-count path)}])
         (into {}))))

(defn load-categories-tsv [worker]
  (let [cats (read-category-tsv)
        {:keys [^ConcurrentHashMap custom-state]} worker]
    ;; squirrel these data-structures away for later (see category-generator, sample-category-id)
    (.putAll custom-state {::categories cats
                           ::category-weighting (b2/weighted-sample-fn (map (juxt :id :item-count) (vals cats)))})))

(defn generate-region [worker]
  (let [r-id (b2/increment worker region-id)]
    {:id r-id
     :r_id r-id
     :r_name (b2/random-str worker 6 32)}))

(defn generate-global-attribute-group [worker]
  (let [gag-id (b2/increment worker gag-id)
        category-id (b2/sample-flat worker category-id)]
    {:id gag-id
     :gag_c_id category-id
     :gag_name (b2/random-str worker 6 32)}))

(defn generate-global-attribute-value [worker]
  (let [gav-id (b2/increment worker gav-id)
        gag-id (b2/sample-flat worker gag-id)]
    {:id gav-id
     :gav_gag_id gag-id
     :gav_name (b2/random-str worker 6 32)}))

(defn generate-category [worker]
  (let [{::keys [categories]} (:custom-state worker)
        c-id (b2/increment worker category-id)
        {:keys [category-name, parent]} (categories c-id)]
    {:id c-id
     :c_id c-id
     :c_parent_id (when (seq parent) (:id (categories parent)))
     :c_name (or category-name (b2/random-str worker 6 32))}))

(defn generate-user-attributes [worker]
  (let [u_id (b2/sample-flat worker user-id)
        ua-id (b2/increment worker user-attribute-id)]
    (when u_id
      {:id ua-id
       :ua_u_id u_id
       :ua_name (b2/random-str worker 5 32)
       :ua_value (b2/random-str worker 5 32)
       :u_created (b2/current-timestamp worker)})))

(defn generate-item [worker]
  (let [i_id (b2/increment worker item-id)
        i_u_id (b2/sample-flat worker user-id)
        i_c_id (sample-category-id worker)
        i_start_date (b2/current-timestamp worker)
        i_end_date (.plus ^Instant (b2/current-timestamp worker) (Duration/ofDays 32))
        i_status (sample-status worker)]
    (add-item-status worker (->ItemSample i_id i_u_id i_status i_end_date 0))
    (when i_u_id
      {:id i_id
       :i_id i_id
       :i_u_id i_u_id
       :i_c_id i_c_id
       :i_name (b2/random-str worker 6 32)
       :i_description (b2/random-str worker 50 255)
       :i_user_attributes (b2/random-str worker 20 255)
       :i_initial_price (random-price worker)
       :i_current_price (random-price worker)
       :i_num_bids 0
       :i_num_images 0
       :i_num_global_attrs 0
       :i_start_date i_start_date
       :i_end_date i_end_date
       #_(.plus ^Instant (b2/current-timestamp worker) (Duration/ofDays 32))
       :i_status i_status})))

(defn- wrap-in-logging [f]
  (fn [& args]
    (log/trace (str "Start of " f))
    (let [res (apply f args)]
      (log/trace (str "Finish of " f))
      res)))

(defn- wrap-in-catch [f]
  (fn [& args]
    (try
      (apply f args)
      (catch Throwable t
        (log/error t (str "Error while executing " f))))))

(defn benchmark [{:keys [seed,
                         threads,
                         duration
                         scale-factor]
                  :or {seed 0,
                       threads 8,
                       duration "PT30S"
                       scale-factor 0.1}}]
  (let [duration (Duration/parse duration)
        sf scale-factor]
    (log/trace {:scale-factor scale-factor})
    {:title "Auction Mark OLTP"
     :seed seed
     :tasks
     [{:t :do
         :stage :load
         :tasks [{:t :call, :f (fn [_] (log/info "start load stage"))}
                 {:t :call, :f [bcore2/install-tx-fns {:apply-seller-fee tx-fn-apply-seller-fee, :new-bid tx-fn-new-bid}]}
                 {:t :call, :f load-categories-tsv}
                 {:t :call, :f [bcore2/generate 'region generate-region 75]}
                 {:t :call, :f [bcore2/generate 'category generate-category 16908]}
                 {:t :call, :f [bcore2/generate 'user generate-user (* sf 1e6)]}
                 {:t :call, :f [bcore2/generate 'user-attribute generate-user-attributes (* sf 1e6 1.3)]}
                 {:t :call, :f [bcore2/generate 'item generate-item (* sf 1e6 10)]}
                 {:t :call, :f (fn [_] (log/info "finished load stage"))}]}
      {:t :do
       :stage :setup-worker
       :tasks [{:t :call, :f (fn [_] (log/info "setting up worker with stats"))}
               {:t :call, :f load-stats-into-worker}
               {:t :call, :f log-stats}
               {:t :call, :f (fn [_] (log/info "finished setting up worker with stats"))}]}
      {:t :concurrently
       :stage :oltp
       :duration duration
       :join-wait (Duration/ofSeconds 5)
       :thread-tasks [{:t :pool
                       :duration duration
                       :join-wait (Duration/ofMinutes 5)
                       :thread-count threads
                       :think Duration/ZERO
                       :pooled-task {:t :pick-weighted
                                     :choices [[{:t :call, :transaction :get-item, :f proc-get-item} 12.0]
                                               [{:t :call, :transaction :new-user, :f proc-new-user} 0.5]
                                               [{:t :call, :transaction :new-item, :f proc-new-item} 1.0]
                                               [{:t :call, :transaction :new-bid,  :f proc-new-bid} 2.0]]}}
                      {:t :freq-job
                       :duration duration
                       :freq (Duration/ofMillis (* 0.2 (.toMillis duration)))
                       :job-task {:t :call, :transaction :index-item-status-groups, :f index-item-status-groups}}]}]}))
