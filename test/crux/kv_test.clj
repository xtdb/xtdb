(ns crux.kv-test
  (:require [clojure.test :as t]
            [crux.bootstrap :as b]
            [crux.byte-utils :as bu]
            [crux.fixtures :as f]
            [crux.kv :as kv]
            [crux.memory :as mem]
            [crux.io :as cio])
  (:import [java.io Closeable]))

(t/use-fixtures :each f/with-each-kv-store-implementation f/with-kv-store)

(declare value seek seek-and-iterate)

(t/deftest test-store-and-value []
  (t/testing "store, retrieve and seek value"
    (kv/store f/*kv* [[(bu/long->bytes 1) (.getBytes "Crux")]])
    (t/is (= "Crux" (String. ^bytes (value f/*kv* (bu/long->bytes 1)))))
    (t/is (= [1 "Crux"] (let [[k v] (seek f/*kv* (bu/long->bytes 1))]
                          [(bu/bytes->long k) (String. ^bytes v)]))))

  (t/testing "non existing key"
    (t/is (nil? (value f/*kv* (bu/long->bytes 2))))))

(t/deftest test-can-store-and-delete-all []
  (kv/store f/*kv* (map (fn [i]
                          [(bu/long->bytes i) (bu/long->bytes (inc i))])
                        (range 10)))
  (doseq [i (range 10)]
    (t/is (= (inc i) (bu/bytes->long (value f/*kv* (bu/long->bytes i))))))

  (t/testing "deleting all keys in random order, including non existent keys"
    (kv/delete f/*kv* (for [i (shuffle (range 12))]
                        (bu/long->bytes i)))
    (t/is (zero? (kv/count-keys f/*kv*)))))

(t/deftest test-seek-and-iterate-range []
  (doseq [[^String k v] {"a" 1 "b" 2 "c" 3 "d" 4}]
    (kv/store f/*kv* [[(.getBytes k) (bu/long->bytes v)]]))

  (t/testing "seek range is exclusive"
    (t/is (= [["b" 2] ["c" 3]]
             (for [[^bytes k v] (seek-and-iterate f/*kv*
                                                  #(neg? (bu/compare-bytes % (.getBytes "d")))
                                                  (.getBytes "b"))]
               [(String. k) (bu/bytes->long v)]))))

  (t/testing "seek range after existing keys returns empty"
    (t/is (= [] (seek-and-iterate f/*kv* #(neg? (bu/compare-bytes % (.getBytes "d"))) (.getBytes "d"))))
    (t/is (= [] (seek-and-iterate f/*kv* #(neg? (bu/compare-bytes % (.getBytes "f")%)) (.getBytes "e")))))

  (t/testing "seek range before existing keys returns keys at start"
    (t/is (= [["a" 1]] (for [[^bytes k v] (into [] (seek-and-iterate f/*kv* #(neg? (bu/compare-bytes % (.getBytes "b"))) (.getBytes "0")))]
                         [(String. k) (bu/bytes->long v)])))))

(t/deftest test-seek-between-keys []
  (doseq [[^String k v] {"a" 1 "c" 3}]
    (kv/store f/*kv* [[(.getBytes k) (bu/long->bytes v)]]))

  (t/testing "seek returns next valid key"
    (t/is (= ["c" 3]
             (let [[^bytes k v] (seek f/*kv* (.getBytes "b"))]
               [(String. k) (bu/bytes->long v)])))))

(t/deftest test-seek-and-iterate-prefix []
  (doseq [[^String k v] {"aa" 1 "b" 2 "bb" 3 "bcc" 4 "bd" 5 "dd" 6}]
    (kv/store f/*kv* [[(.getBytes k) (bu/long->bytes v)]]))

  (t/testing "seek within bounded prefix returns all matching keys"
    (t/is (= [["b" 2] ["bb" 3] ["bcc" 4] ["bd" 5]]
             (for [[^bytes k v] (into [] (seek-and-iterate f/*kv* #(bu/bytes=? (.getBytes "b") % (alength (.getBytes "b"))) (.getBytes "b")))]
               [(String. k) (bu/bytes->long v)]))))

  (t/testing "seek within bounded prefix before or after existing keys returns empty"
    (t/is (= [] (into [] (seek-and-iterate f/*kv* (partial bu/bytes=? (.getBytes "0")) (.getBytes "0")))))
    (t/is (= [] (into [] (seek-and-iterate f/*kv* (partial bu/bytes=? (.getBytes "e")) (.getBytes "0")))))))

(t/deftest test-delete-keys []
  (t/testing "store, retrieve and delete value"
    (kv/store f/*kv* [[(bu/long->bytes 1) (.getBytes "Crux")]])
    (t/is (= "Crux" (String. ^bytes (value f/*kv* (bu/long->bytes 1)))))
    (kv/delete f/*kv* [(bu/long->bytes 1)])
    (t/is (nil? (value f/*kv* (bu/long->bytes 1))))
    (t/testing "deleting non existing key is noop"
      (kv/delete f/*kv* [(bu/long->bytes 1)]))))

(t/deftest test-backup-and-restore-db
  (let [backup-dir (cio/create-tmpdir "kv-store-backup")]
    (try
      (kv/store f/*kv* [[(bu/long->bytes 1) (.getBytes "Crux")]])
      (cio/delete-dir backup-dir)
      (kv/backup f/*kv* backup-dir)
      (with-open [restored-kv (b/start-kv-store {:db-dir (str backup-dir)
                                                 :kv-backend f/*kv-backend*})]
        (t/is (= "Crux" (String. ^bytes (value restored-kv (bu/long->bytes 1)))))

        (t/testing "backup and original are different"
          (kv/store f/*kv* [[(bu/long->bytes 1) (.getBytes "Original")]])
          (kv/store restored-kv [[(bu/long->bytes 1) (.getBytes "Backup")]])
          (t/is (= "Original" (String. ^bytes (value f/*kv* (bu/long->bytes 1)))))
          (t/is (= "Backup" (String. ^bytes (value restored-kv (bu/long->bytes 1)))))))
      (finally
        (cio/delete-dir backup-dir)))))

;; TODO: These helpers convert back and forth to bytes, would be good
;; to get rid of this, but that requires changing removing the byte
;; arrays above in the tests. The tested code uses buffers internally.

(defn seek [kvs k]
  (with-open [snapshot (kv/new-snapshot kvs)
              i (kv/new-iterator snapshot)]
    (when-let [k (kv/seek i k)]
      [(mem/->on-heap k) (mem/->on-heap (kv/value i))])))

(defn value [kvs seek-k]
  (let [[k v] (seek kvs seek-k)]
    (when (and k (bu/bytes=? seek-k k))
      (mem/->on-heap v))))

(defn seek-and-iterate [kvs key-pred seek-k]
  (with-open [snapshot (kv/new-snapshot kvs)
              i (kv/new-iterator snapshot)]
    (loop [acc (transient [])
           k (kv/seek i seek-k)]
      (let [k (when k
                (mem/->on-heap k))]
        (if (and k (key-pred k))
          (recur (conj! acc [k (mem/->on-heap (kv/value i))])
                 (kv/next i))
          (persistent! acc))))))

(t/deftest test-performance-on-heap
  (if (and (System/getenv "CRUX_KV_PERFORMANCE")
           (if-let [backend (System/getenv "CRUX_KV_PERFORMANCE_BACKEND")]
             (= backend f/*kv-backend*)
             true))
    (do (println f/*kv-backend* "on-heap")
        (let [n 1000000
              ks (vec (for [n (range n)]
                        (.getBytes (format "%020x" n))))]
          (t/is (= n (count ks)))

          (System/gc)
          (println "Writing")
          (time
           (kv/store f/*kv* (for [k ks]
                              [k k])))

          (System/gc)
          (println "Reading")
          (time
           (do (dotimes [_ 10]
                 (time
                  (with-open [snapshot (kv/new-snapshot f/*kv*)
                              i (kv/new-iterator snapshot)]
                    (dotimes [idx n]
                      (let [idx (- (dec n) idx)
                            k (get ks idx)]
                        (assert (bu/bytes=? k (kv/seek i k)))
                        (assert (bu/bytes=? k (kv/value i))))))))
               (println "Done")))
          (println)))
    (t/is true)))

(t/deftest test-performance-off-heap
  (if (and (System/getenv "CRUX_KV_PERFORMANCE")
           (if-let [backend (System/getenv "CRUX_KV_PERFORMANCE_BACKEND")]
             (= backend f/*kv-backend*)
             true))
    (let [n 1000000
          ks (vec (for [n (range n)]
                    (mem/->off-heap (.getBytes (format "%020x" n)))))]
      (println f/*kv-backend* "off-heap")
      (t/is (= n (count ks)))
      (t/is (mem/off-heap? (first ks)))

      (System/gc)
      (println "Writing")
      (time
       (kv/store f/*kv* (for [k ks]
                          [k k])))

      (System/gc)
      (println "Reading")
      (time
       (do (dotimes [_ 10]
             (time
              ;; TODO: Note, the cached decorator still uses
              ;; bytes, so we grab the underlying kv store.
              (with-open [snapshot (kv/new-snapshot (:kv f/*kv*))
                          i (kv/new-iterator snapshot)]
                (dotimes [idx n]
                  (let [idx (- (dec n) idx)
                        k (get ks idx)]
                    (assert (mem/buffers=? k (kv/seek i k)))
                    (assert (mem/buffers=? k (kv/value i))))))))
           (println "Done")))
      (println))
    (t/is true)))
