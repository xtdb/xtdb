(ns xtdb.baby-deterministic-test
  (:require [clojure.test :as t]
            [clojure.string :as str]
            [clojure.test.check :as tc]
            [clojure.set :as set]
            [clojure.test.check.generators :as gen]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.db-catalog :as db]
            [xtdb.node.impl] ;;TODO probably move internal methods to main node interface
            [xtdb.object-store :as os]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]
            [xtdb.node :as xtn]
            [clojure.test.check.properties :as prop])
  (:import [java.nio.file Path]))

;; Simple/hashable value generators
(def nil-gen (gen/return nil))
(def bool-gen gen/boolean)
(def byte-gen (gen/fmap byte (gen/choose -128 127)))
(def short-gen (gen/fmap short (gen/choose -32768 32767)))
(def int-gen gen/int)
(def long-gen gen/large-integer)
(def float-gen gen/double) ; TODO: fix
(def double-gen gen/double)
(def string-gen gen/string-alphanumeric)
(def byte-array-gen (gen/fmap byte-array (gen/vector byte-gen 1 10)))
(def keyword-gen gen/keyword)
(def uuid-gen gen/uuid)
(def uri-gen (gen/fmap #(java.net.URI. (str "https://example" % ".com")) 
                       (gen/choose 1 1000)))

;; Date/time generators
(def instant-gen 
  (gen/fmap #(java.time.Instant/ofEpochSecond %) 
            (gen/choose 0 2147483647)))

(def local-date-gen 
  (gen/fmap java.time.LocalDate/ofEpochDay 
            (gen/choose 0 50000)))

(def local-time-gen 
  (gen/fmap java.time.LocalTime/ofSecondOfDay 
            (gen/choose 0 86399)))

(def local-datetime-gen 
  (gen/let [date local-date-gen
            time local-time-gen]
    (java.time.LocalDateTime/of date time)))

(def offset-datetime-gen 
  (gen/let [ldt local-datetime-gen
            offset-hours (gen/choose -12 12)]
    (java.time.OffsetDateTime/of ldt (java.time.ZoneOffset/ofHours offset-hours))))

(def zoned-datetime-gen 
  (gen/let [ldt local-datetime-gen]
    (java.time.ZonedDateTime/of ldt (java.time.ZoneId/of "UTC"))))

(def duration-gen (gen/return #xt/duration "PT1S"))
(def interval-gen (gen/return #xt/interval "P1YT1S"))

;; Simple/hashable value generators
(def simple-gen
  (gen/one-of [nil-gen bool-gen byte-gen short-gen int-gen long-gen 
               float-gen double-gen string-gen byte-array-gen keyword-gen 
               uuid-gen uri-gen instant-gen local-date-gen local-time-gen 
               local-datetime-gen offset-datetime-gen zoned-datetime-gen 
               duration-gen interval-gen]))

(def safe-keyword-gen
  (gen/such-that #(let [n (name %)]
                    (and (not (str/starts-with? n "_"))
                         (not (str/starts-with? n "-"))
                         (not (#{"_id" "_fn"} n))))
                 gen/keyword
                 100))

;; Frequency-based key distribution for realistic overlap
(def key-gen
  (gen/frequency
    [[9 (gen/elements [:a :b :c :d])] ; high overlap
     [1 safe-keyword-gen]]))               ; completely random keys

;; Recursive value generator - test.check handles depth automatically!
(def value-gen
  (gen/recursive-gen
    (fn [inner-gen]
      (gen/one-of 
        [(gen/vector inner-gen 1 4)                    ; Lists
         ;; Map generation using frequency-based keys
         (gen/let [num-keys (gen/choose 1 4)
                   keys (gen/vector-distinct key-gen {:num-elements num-keys})
                   values (gen/vector inner-gen num-keys)]
            (zipmap keys values))]))
    simple-gen))

;; Map generator for records using frequency-based keys
(def record-map-gen
  (gen/let [num-keys (gen/choose 1 4)
            keys (gen/vector-distinct key-gen {:num-elements num-keys})
            values (gen/vector value-gen num-keys)]
    (zipmap keys values)))

;; Multiple records generator
(def records-gen
  (gen/let [num-records (gen/choose 1 5)
            available-ids (gen/shuffle (range 8))  ; All possible IDs
            records-data (gen/vector record-map-gen num-records)]
    (mapv (fn [id data] (assoc data :xt/id id))
          (take num-records available-ids)
          records-data)))

;; Test state generator (for the main test)
(def test-state-gen
  (gen/let [records1 records-gen
            records2 records-gen
            first-flush? gen/boolean
            second-flush? gen/boolean
            compact? gen/boolean
            ;; Generate erased IDs as subset of records1 IDs
            all-ids (gen/return (map :xt/id records1))
            num-to-erase (gen/choose 0 (count all-ids))
            erased-ids (gen/let [shuffled (gen/shuffle all-ids)]
                         (gen/return (into #{} (take num-to-erase shuffled))))]
    {:records1 records1
     :records2 records2
     :first-flush? first-flush?
     :erased-ids erased-ids
     :second-flush? second-flush?
     :compact? compact?}))

(comment
  (gen/sample records-gen 1))

(def live-erase-live-property
  (prop/for-all [{:keys [records1 records2 first-flush? erased-ids second-flush? compact?]}
                 test-state-gen]
    (with-open [node (xtn/start-node {:databases {:xtdb {:log [:in-memory {:instant-src (tu/->mock-clock)}]}}
                                      :compactor {:threads 0}})]
      ;; Execute the test steps
      (xt/execute-tx node [(into [:put-docs :docs] records1)])
      
      (when first-flush?
        (tu/flush-block! node))
      
      (when-not (empty? erased-ids)
        (xt/execute-tx node [(into [:erase-docs :docs] erased-ids)]))
      
      (xt/execute-tx node [(into [:put-docs :docs] records2)])
      
      (when second-flush?
        (tu/flush-block! node))
      
      (when compact?
        (c/compact-all! node #xt/duration "PT1S"))
      
      ;; Your assertions (return true if they pass, false if they fail)
      (and (= (if (not (empty? erased-ids)) 3 2)
              (count (xt/q node "FROM xt.txs")))
           
           (= (->> [(when (or first-flush? second-flush?)
                      ["l00-rc-b00.arrow"])
                    (when (and first-flush? second-flush?)
                      ["l00-rc-b01.arrow"])]
                   flatten
                   (remove nil?)
                   vec)
              (->> (.listAllObjects (.getBufferPool (db/primary-db node))
                                    (util/->path "tables/public$docs/meta/"))
                   (mapv (comp str #(.getFileName ^Path %) :key os/<-StoredObject))))
           
           (let [res (xt/q node "SELECT * FROM docs ORDER BY _id")
                 expected-ids (set/union (set/difference (into #{} (map :xt/id records1))
                                                         (when-not (empty? erased-ids) erased-ids))
                                         (into #{} (map :xt/id records2)))]
             (= expected-ids (into #{} (map :xt/id res))))))))

(comment
  (t/deftest osm-test
    (with-open [node (xtn/start-node {:databases {:xtdb {:log [:in-memory {:instant-src (tu/->mock-clock)}]}}
                                      :compactor {:threads 0}})]
      (xt/execute-tx node [[:put-docs :docs
                            {:a nil :A nil :b nil :xt/id 0}
                            {:a nil :xt/id 1}]])

      (xt/execute-tx node [[:put-docs :docs
                            {:a nil :xt/id 0}]])

      (t/is (= [{:xt/id 0}] (xt/q node "SELECT * FROM docs ORDER BY _id"))))))

(comment
  (tc/quick-check 100 live-erase-live-property))
