(ns xtdb.baby-deterministic-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.db-catalog :as db]
            [xtdb.node.impl] ;;TODO probably move internal methods to main node interface
            [xtdb.object-store :as os]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]
            [xtdb.node :as xtn])
  (:import [java.nio.file Path]))

(defn rand-between [^java.util.Random rng min max]
  (+ min (.nextInt rng (- max min))))

;; Forward declaration for recursion
(declare rand-map-a-z)
(declare generate-random-value)
(declare generate-hashable-value)

;; Simple/hashable value functions
(def hashable-value-fns
  [; Based on ->arrow-type in types.clj
   (constantly nil)
   (constantly true)

   (constantly (byte 42))
   (constantly (short 1000))
   (constantly 123)
   (constantly (Long. 456789))
   (constantly 3.14)
   (constantly (Double. 2.718))

   (constantly "example string")
   (constantly (byte-array [1 2 3]))

   ; TODO: Should we allowed
   ;(constantly #xt/tstz-range [#xt/zdt "2020-01-02T00:00Z" nil])
   (constantly :example-keyword)
   ; TODO: regclass?
   ; TODO: regproc?
   (constantly (java.util.UUID/fromString "123e4567-e89b-12d3-a456-426614174000"))
   (constantly (java.net.URI. "https://example.com"))
   ; TODO: transit?

   (constantly (java.time.ZonedDateTime/now))
   (constantly (java.time.OffsetDateTime/now))
   (constantly (java.time.Instant/now))
   (constantly (java.time.LocalDateTime/now))
   (constantly (java.time.LocalDate/now))
   (constantly (java.time.LocalTime/now))
   (constantly #xt/duration "PT1S")
   (constantly #xt/interval "P1YT1S")])

;; TODO: Ensure nothing are not lazy
;; TODO: Something better for reaching max depth

;; Complex data structure functions (these use depth)
(def complex-value-fns
  [;; Recursive List
   (fn [rng depth]
     (if (< depth 3)
       (let [size (rand-between rng 0 4)]
         (into [] (repeatedly size #(generate-random-value rng (inc depth)))))
       []))  ; Empty list at max depth
   
   ;; Recursive Set  
   (fn [rng depth]
     (if (< depth 3)
       (let [size (rand-between rng 0 4)]
         (into #{} (repeatedly size #(generate-hashable-value rng))))
       #{}))  ; Empty set at max depth
   
   ;; Recursive Map
   (fn [rng depth]
     (if (< depth 3)
       (rand-map-a-z rng (inc depth))
       {}))])  ; Empty map at max depth

;; All value functions combined
(def all-value-fns (concat hashable-value-fns complex-value-fns))

(defn generate-hashable-value [^java.util.Random rng]
  (let [fn-idx (rand-between rng 0 (count hashable-value-fns))
        value-fn (nth hashable-value-fns fn-idx)]
    (value-fn)))

(defn generate-random-value [^java.util.Random rng depth]
  (let [fn-idx (rand-between rng 0 (count all-value-fns))
        value-fn (nth all-value-fns fn-idx)]
    (if (>= fn-idx (count hashable-value-fns))
      ;; Complex type - pass rng and depth
      (value-fn rng depth)
      ;; Simple type - call with no args (constantly functions ignore args)
      (value-fn))))

(defn shuffle-rng [^java.util.Random rng ^java.util.Collection coll]
  (let [al (java.util.ArrayList. coll)]
    (java.util.Collections/shuffle al rng)
    (vec (.toArray al))))

(defn rand-map-a-z 
  ([^java.util.Random rng] (rand-map-a-z rng 0))
  ([^java.util.Random rng depth]
   (let [;; All possible keys a-z
         all-keys (map #(keyword (str (char %))) (range (int \a) (inc (int \d))))
         ;; Randomly decide how many keys to include
         num-keys (rand-between rng 0 (count all-keys))
         ;; Randomly select which keys to include
         selected-keys (take num-keys (shuffle-rng rng all-keys))
         ;; For each selected key, generate a random value
         key-value-pairs (map (fn [k]
                               [k (generate-random-value rng depth)])
                             selected-keys)]
     (into {} key-value-pairs))))

(defn random-records [rng max-records]
  (let [num-records (rand-between rng 1 max-records)]
    (doall
      (for [i (shuffle-rng rng (take num-records (range max-records)))]
        (-> (rand-map-a-z rng)
            (assoc :xt/id i))))))

(comment
  (let [seed (rand-int 1000)
        rng (java.util.Random. seed)]
    (println "seed:" seed)
    (random-records rng 10)))

(t/deftest flush-flush-compact
  (dotimes [i 1000]
    (when (= (mod i 100) 0)
      (println "iteration:" i))
    (with-open [node (xtn/start-node {:databases {:xtdb {:log [:in-memory {:instant-src (tu/->mock-clock)}]}}
                                      :compactor {:threads 0}})]
      (let [seed (rand-int Integer/MAX_VALUE)
            rng (java.util.Random. seed)
            records1 (random-records rng 4)
            records2 (random-records rng 4)]
        (t/testing (str "iteration-" i "-seed-" seed)
          (try
            ;(println "records1:" records1)
            ;(println "records2:" records2)

            (xt/execute-tx node [(into [:put-docs :docs] records1)])
            (tu/flush-block! node)

            (xt/execute-tx node [(into [:put-docs :docs] records2)])
            (tu/flush-block! node)

            ;; TODO: How to catch compaction errors?
            (c/compact-all! node #xt/duration "PT1S")

            ;; TODO: Ensure puts didn't error

            (t/is (= ["l00-rc-b00.arrow" "l00-rc-b01.arrow"
                      "l01-r20200106-b01.arrow"
                      "l01-rc-b00.arrow" "l01-rc-b01.arrow"]
                     (->> (.listAllObjects (.getBufferPool (db/primary-db node))
                                           (util/->path "tables/public$docs/meta/"))
                          (mapv (comp str #(.getFileName ^Path %) :key os/<-StoredObject)))))

            (t/is (= 2 (count (xt/q node "FROM xt.txs"))))

            ;;; TODO: Fix
            (let [res (xt/q node "SELECT * FROM docs ORDER BY _id")
                  expected-ids (into #{} (map :xt/id (concat records1 records2)))]
              (t/is (= expected-ids (into #{} (map :xt/id res)))))

            (catch Throwable t
              (println "seed:" seed)
              (throw (ex-info "Error"
                              {:seed seed :records1 records1 :records2 records2}
                              t)))))))))

(t/deftest flush-live
  (dotimes [i 1000]
    (when (= (mod i 100) 0)
      (println "iteration:" i))
    (with-open [node (xtn/start-node {:databases {:xtdb {:log [:in-memory {:instant-src (tu/->mock-clock)}]}}
                                      :compactor {:threads 0}})]
      (let [seed (rand-int Integer/MAX_VALUE)
            rng (java.util.Random. seed)
            records1 (random-records rng 4)
            records2 (random-records rng 4)]
        (t/testing (str "iteration-" i "-seed-" seed)
          (try
            ;(println "records1:" records1)
            ;(println "records2:" records2)

            (xt/execute-tx node [(into [:put-docs :docs] records1)])
            (tu/flush-block! node)

            (xt/execute-tx node [(into [:put-docs :docs] records2)])
            ;(tu/flush-block! node)

            ;; TODO: How to catch compaction errors?
            ;(c/compact-all! node #xt/duration "PT1S")

            ;; TODO: Ensure puts didn't error

            (t/is (= ["l00-rc-b00.arrow"]
                     (->> (.listAllObjects (.getBufferPool (db/primary-db node))
                                           (util/->path "tables/public$docs/meta/"))
                          (mapv (comp str #(.getFileName ^Path %) :key os/<-StoredObject)))))

            (t/is (= 2 (count (xt/q node "FROM xt.txs"))))

            ;;; TODO: Fix
            (let [res (xt/q node "SELECT * FROM docs ORDER BY _id")
                  expected-ids (into #{} (map :xt/id (concat records1 records2)))]
              (t/is (= expected-ids (into #{} (map :xt/id res)))))

            (catch Throwable t
              (println "seed:" seed)
              (throw (ex-info "Error"
                              {:seed seed :records1 records1 :records2 records2}
                              t)))))))))

(t/deftest live-live
  (dotimes [i 1000]
    (when (= (mod i 100) 0)
      (println "iteration:" i))
    (with-open [node (xtn/start-node {:databases {:xtdb {:log [:in-memory {:instant-src (tu/->mock-clock)}]}}
                                      :compactor {:threads 0}})]
      (let [seed (rand-int Integer/MAX_VALUE)
            rng (java.util.Random. seed)
            records1 (random-records rng 4)
            records2 (random-records rng 4)]
        (t/testing (str "iteration-" i "-seed-" seed)
          (try
            ;(println "records1:" records1)
            ;(println "records2:" records2)

            (xt/execute-tx node [(into [:put-docs :docs] records1)])
            ;(tu/flush-block! node)

            (xt/execute-tx node [(into [:put-docs :docs] records2)])
            ;(tu/flush-block! node)

            ;; TODO: How to catch compaction errors?
            ;(c/compact-all! node #xt/duration "PT1S")

            ;; TODO: Ensure puts didn't error

            (t/is (= 2 (count (xt/q node "FROM xt.txs"))))

            ;;; TODO: Fix
            (let [res (xt/q node "SELECT * FROM docs ORDER BY _id")
                  expected-ids (into #{} (map :xt/id (concat records1 records2)))]
              (t/is (= expected-ids (into #{} (map :xt/id res)))))

            (catch Throwable t
              (println "seed:" seed)
              (throw (ex-info "Error"
                              {:seed seed :records1 records1 :records2 records2}
                              t)))))))))

(t/deftest live-erase-live
  (dotimes [i 1000]
    (when (= (mod i 100) 0)
      (println "iteration:" i))
    (with-open [node (xtn/start-node {:databases {:xtdb {:log [:in-memory {:instant-src (tu/->mock-clock)}]}}
                                      :compactor {:threads 0}})]
      (let [seed (rand-int Integer/MAX_VALUE)
            rng (java.util.Random. seed)
            records1 (random-records rng 4)
            records2 (random-records rng 4)]
        (t/testing (str "iteration-" i "-seed-" seed)
          (try
            (println "records1:" records1)
            (println "records2:" records2)

            (xt/execute-tx node [(into [:put-docs :docs] records1)])
            ;(tu/flush-block! node)

            (xt/execute-tx node [[:erase-docs :docs]])

            (xt/execute-tx node [(into [:put-docs :docs] records2)])
            ;(tu/flush-block! node)

            ;; TODO: How to catch compaction errors?
            ;(c/compact-all! node #xt/duration "PT1S")

            ;; TODO: Ensure puts didn't error

            (t/is (= 2 (count (xt/q node "FROM xt.txs"))))

            ;;; TODO: Fix
            (let [res (xt/q node "SELECT * FROM docs ORDER BY _id")
                  expected-ids (into #{} (map :xt/id (concat records1 records2)))]
              (t/is (= expected-ids (into #{} (map :xt/id res)))))

            (catch Throwable t
              (println "seed:" seed)
              (throw (ex-info "Error"
                              {:seed seed :records1 records1 :records2 records2}
                              t)))))))))
