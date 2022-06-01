(ns xtdb.kv-test
  (:require [clojure.test :as t]
            [clojure.test.check.clojure-test :as tcct]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [xtdb.checkpoint :as cp]
            [xtdb.codec :as c]
            [xtdb.fixtures :as fix]
            [xtdb.fixtures.kv :as fkv]
            [xtdb.io :as xio]
            [xtdb.kv :as kv]
            [xtdb.memory :as mem])
  (:import java.nio.ByteOrder
           org.agrona.concurrent.UnsafeBuffer
           java.lang.IllegalStateException))

(t/use-fixtures :once fix/with-silent-test-check)
(t/use-fixtures :each fkv/with-each-kv-store*)

;; TODO: These helpers convert back and forth to bytes, would be good
;; to get rid of this, but that requires changing removing the byte
;; arrays above in the tests. The tested code uses buffers internally.

(defn seek [kvs k]
  (with-open [snapshot (kv/new-snapshot kvs)
              i (kv/new-iterator snapshot)]
    (when-let [k (kv/seek i k)]
      [(mem/->on-heap k) (mem/->on-heap (kv/value i))])))

(defn value [kvs seek-k]
  (with-open [snapshot (kv/new-snapshot kvs)]
    (some-> (kv/get-value snapshot seek-k)
            (mem/->on-heap))))

(defn- value-tx [kv-tx seek-k]
  (with-open [snapshot (kv/new-tx-snapshot kv-tx)]
    (some-> (kv/get-value snapshot seek-k)
            (mem/->on-heap))))

(defn- value-snapshot [snapshot seek-k]
  (some-> (kv/get-value snapshot seek-k)
          (mem/->on-heap)))

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

(defn long->bytes ^bytes [^long l]
  (let [ub (UnsafeBuffer. (byte-array Long/BYTES))]
    (.putLong ub 0 l ByteOrder/BIG_ENDIAN)
    (.byteArray ub)))

(defn bytes->long ^long [^bytes data]
  (let [ub (UnsafeBuffer. data)]
    (.getLong ub 0 ByteOrder/BIG_ENDIAN)))

(defn compare-bytes
  (^long [^bytes a ^bytes b]
   (mem/compare-buffers (mem/as-buffer a) (mem/as-buffer b)))
  (^long [^bytes a ^bytes b max-length]
   (mem/compare-buffers (mem/as-buffer a) (mem/as-buffer b) max-length)))

(defn bytes=?
  ([^bytes a ^bytes b]
   (mem/buffers=? (mem/as-buffer a) (mem/as-buffer b)))
  ([^bytes a ^bytes b ^long max-length]
   (mem/buffers=? (mem/as-buffer a) (mem/as-buffer b) max-length)))

(defn- store [kv-store kvs]
  (with-open [kv-tx (kv/begin-kv-tx kv-store)]
    (doseq [[k v] kvs]
      (kv/put-kv kv-tx k v))
    (kv/commit-kv-tx kv-tx)))

(t/deftest test-store-and-value []
  (fkv/with-kv-store [kv-store]
    (t/testing "store, retrieve and seek value"
      (store kv-store [[(long->bytes 1) (.getBytes "XTDB")]])
      (t/is (= "XTDB" (String. ^bytes (value kv-store (long->bytes 1)))))
      (t/is (= [1 "XTDB"] (let [[k v] (seek kv-store (long->bytes 1))]
                            [(bytes->long k) (String. ^bytes v)]))))

    (t/testing "non existing key"
      (t/is (nil? (value kv-store (long->bytes 2)))))))

(t/deftest test-can-store-and-delete-all-116 []
  (fkv/with-kv-store [kv-store]
    (let [number-of-entries 500]
      (store kv-store (map (fn [i]
                                [(long->bytes i) (long->bytes (inc i))])
                              (range number-of-entries)))
      (doseq [i (range number-of-entries)]
        (t/is (= (inc i) (bytes->long (value kv-store (long->bytes i))))))

      (t/testing "deleting all keys in random order, including non existent keys"
        (store kv-store (for [i (shuffle (range (long (* number-of-entries 1.2))))]
                             [(long->bytes i) nil]))
        (doseq [i (range number-of-entries)]
          (t/is (nil? (value kv-store (long->bytes i)))))))))

(t/deftest test-seek-and-iterate-range []
  (fkv/with-kv-store [kv-store]
    (doseq [[^String k v] {"a" 1 "b" 2 "c" 3 "d" 4}]
      (store kv-store [[(.getBytes k) (long->bytes v)]]))

    (t/testing "seek range is exclusive"
      (t/is (= [["b" 2] ["c" 3]]
               (for [[^bytes k v] (seek-and-iterate kv-store
                                                    #(neg? (compare-bytes % (.getBytes "d")))
                                                    (.getBytes "b"))]
                 [(String. k) (bytes->long v)]))))

    (t/testing "seek range after existing keys returns empty"
      (t/is (= [] (seek-and-iterate kv-store #(neg? (compare-bytes % (.getBytes "d"))) (.getBytes "d"))))
      (t/is (= [] (seek-and-iterate kv-store #(neg? (compare-bytes % (.getBytes "f")%)) (.getBytes "e")))))

    (t/testing "seek range before existing keys returns keys at start"
      (t/is (= [["a" 1]] (for [[^bytes k v] (into [] (seek-and-iterate kv-store #(neg? (compare-bytes % (.getBytes "b"))) (.getBytes "0")))]
                           [(String. k) (bytes->long v)]))))))

(t/deftest test-seek-between-keys []
  (fkv/with-kv-store [kv-store]
    (doseq [[^String k v] {"a" 1 "c" 3}]
      (store kv-store [[(.getBytes k) (long->bytes v)]]))

    (t/testing "seek returns next valid key"
      (t/is (= ["c" 3]
               (let [[^bytes k v] (seek kv-store (.getBytes "b"))]
                 [(String. k) (bytes->long v)]))))))

(t/deftest test-seek-and-iterate-prefix []
  (fkv/with-kv-store [kv-store]
    (doseq [[^String k v] {"aa" 1 "b" 2 "bb" 3 "bcc" 4 "bd" 5 "dd" 6}]
      (store kv-store [[(.getBytes k) (long->bytes v)]]))

    (t/testing "seek within bounded prefix returns all matching keys"
      (t/is (= [["b" 2] ["bb" 3] ["bcc" 4] ["bd" 5]]
               (for [[^bytes k v] (into [] (seek-and-iterate kv-store #(bytes=? (.getBytes "b") % (alength (.getBytes "b"))) (.getBytes "b")))]
                 [(String. k) (bytes->long v)]))))

    (t/testing "seek within bounded prefix before or after existing keys returns empty"
      (t/is (= [] (into [] (seek-and-iterate kv-store (partial bytes=? (.getBytes "0")) (.getBytes "0")))))
      (t/is (= [] (into [] (seek-and-iterate kv-store (partial bytes=? (.getBytes "e")) (.getBytes "0"))))))))

(t/deftest test-delete-keys []
  (fkv/with-kv-store [kv-store]
    (t/testing "store, retrieve and delete value"
      (store kv-store [[(long->bytes 1) (.getBytes "XTDB")]])
      (t/is (= "XTDB" (String. ^bytes (value kv-store (long->bytes 1)))))

      (store kv-store [[(long->bytes 1) nil]])
      (t/is (nil? (value kv-store (long->bytes 1))))

      (t/testing "deleting non existing key is noop"
        (store kv-store [[(long->bytes 1) nil]]))))

  (t/testing "store, delete and restore"
    (fkv/with-kv-store [kv-store]
      (store kv-store [[(long->bytes 1) (.getBytes "XTDB")]
                          [(long->bytes 2) (.getBytes "XTDB")]
                          [(long->bytes 1) nil]
                          [(long->bytes 2) nil]
                          [(long->bytes 1) (.getBytes "XTDB")]])

      (t/is (= "XTDB" (String. ^bytes (value kv-store (long->bytes 1)))))
      (t/is (nil? (value kv-store (long->bytes 2)))))))

(t/deftest test-transaction-isolation
  (fkv/with-kv-store [kv-store]
    (with-open [tx1 (kv/begin-kv-tx kv-store)]

      (kv/put-kv tx1 (long->bytes 1) (.getBytes "XTDB"))
      (t/is (= "XTDB" (String. ^bytes (value-tx tx1 (long->bytes 1)))))

      (with-open [tx1-snapshot (kv/new-tx-snapshot tx1)]
        (store kv-store [[(long->bytes 2) (.getBytes "XTDB2")]])
        (t/is (String. ^bytes (value kv-store (long->bytes 2))))
        (t/is (nil? (value-snapshot tx1-snapshot (long->bytes 2))))))))

(t/deftest test-commit-empty-tx
  (fkv/with-kv-store [kv-store]
    (let [tx1 (kv/begin-kv-tx kv-store)]
      (kv/commit-kv-tx tx1))))

(t/deftest test-can-read-writes-in-tx
  (fkv/with-kv-store [kv-store]
    (let [tx1 (kv/begin-kv-tx kv-store)
          tx2 (kv/begin-kv-tx kv-store)]

      (t/testing "read a put"
        (kv/put-kv tx1 (long->bytes 1) (.getBytes "XTDB"))
        (t/is (= "XTDB" (String. ^bytes (value-tx tx1 (long->bytes 1)))))
        (t/is (nil? (value-tx tx2 (long->bytes 1))))
        (t/is (nil? (value kv-store (long->bytes 1)))))

      (t/testing "delete"
        (kv/put-kv tx2 (long->bytes 1) (.getBytes "XTDB"))
        (kv/put-kv tx1 (long->bytes 1) nil)
        (t/is (nil? (value-tx tx1 (long->bytes 1))))
        (t/is (value-tx tx2 (long->bytes 1))))

      (t/testing "update"
        (kv/put-kv tx1 (long->bytes 1) (.getBytes "XTDB2"))
        (kv/put-kv tx1 (long->bytes 1) (.getBytes "XTDB3"))
        (t/is (= "XTDB3" (String. ^bytes (value-tx tx1 (long->bytes 1)))))
        (t/is (= "XTDB" (String. ^bytes (value-tx tx2 (long->bytes 1))))))

      (t/testing "seek"
        (with-open [snapshot (kv/new-tx-snapshot tx1)
                    i (kv/new-iterator snapshot)]
          (t/is (kv/seek i (long->bytes 0)))
          (t/is (kv/seek i (long->bytes 1)))
          (t/is (nil? (kv/seek i (long->bytes 2))))
          (t/is (some? (kv/seek i (long->bytes 0))))
          (when-not (instance? xtdb.mem_kv.MemKv kv-store)
            (t/is (thrown-with-msg? IllegalStateException #"Snapshot open will be affected by put"
                                    (kv/put-kv tx1 (long->bytes 1) nil)))))))))

(t/deftest test-can-commit-txs
  (fkv/with-kv-store [kv-store]
    (let [^java.io.Closeable tx1 (kv/begin-kv-tx kv-store)]
      (kv/put-kv tx1 (long->bytes 1) (.getBytes "XTDB"))
      (t/is (value-tx tx1 (long->bytes 1)))
      (t/is (nil? (value kv-store (long->bytes 1))))

      (kv/commit-kv-tx tx1)

      (t/is (value kv-store (long->bytes 1))))))

(t/deftest test-checkpoint-and-restore-db
  (fkv/with-kv-store [kv-store]
    (when (satisfies? cp/CheckpointSource kv-store)
      (fix/with-tmp-dirs #{backup-dir}
        (store kv-store [[(long->bytes 1) (.getBytes "XTDB")]])
        (xio/delete-dir backup-dir)
        (cp/save-checkpoint kv-store backup-dir)
        (binding [fkv/*kv-opts* (merge fkv/*kv-opts* {:db-dir backup-dir})]
          (fkv/with-kv-store [restored-kv]
            (t/is (= "XTDB" (String. ^bytes (value restored-kv (long->bytes 1)))))

            (t/testing "backup and original are different"
              (store kv-store [[(long->bytes 1) (.getBytes "Original")]])
              (store restored-kv [[(long->bytes 1) (.getBytes "Backup")]])
              (t/is (= "Original" (String. ^bytes (value kv-store (long->bytes 1)))))
              (t/is (= "Backup" (String. ^bytes (value restored-kv (long->bytes 1))))))))))))

(t/deftest test-compact []
  (fkv/with-kv-store [kv-store]
    (t/testing "store, retrieve and delete value"
      (store kv-store [[(.getBytes "key-with-a-long-prefix-1") (.getBytes "XTDB")]])
      (store kv-store [[(.getBytes "key-with-a-long-prefix-2") (.getBytes "is")]])
      (store kv-store [[(.getBytes "key-with-a-long-prefix-3") (.getBytes "awesome")]])
      (t/testing "compacting"
        (kv/compact kv-store))
      (t/is (= "XTDB" (String. ^bytes (value kv-store (.getBytes "key-with-a-long-prefix-1")))))
      (t/is (= "is" (String. ^bytes (value kv-store (.getBytes "key-with-a-long-prefix-2")))))
      (t/is (= "awesome" (String. ^bytes (value kv-store (.getBytes "key-with-a-long-prefix-3"))))))))

(t/deftest test-sanity-check-can-start-with-sync-enabled
  (binding [fkv/*kv-opts* (merge fkv/*kv-opts* {:sync? true})]
    (fkv/with-kv-store [kv-store]
      (store kv-store [[(long->bytes 1) (.getBytes "XTDB")]])
      (t/is (= "XTDB" (String. ^bytes (value kv-store (long->bytes 1))))))))

(t/deftest test-sanity-check-can-fsync
  (fkv/with-kv-store [kv-store]
    (store kv-store [[(long->bytes 1) (.getBytes "XTDB")]])
    (kv/fsync kv-store)
    (t/is (= "XTDB" (String. ^bytes (value kv-store (long->bytes 1)))))))

(t/deftest test-can-get-from-snapshot
  (fkv/with-kv-store [kv-store]
    (store kv-store [[(long->bytes 1) (.getBytes "XTDB")]])
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (t/is (= "XTDB" (String. (mem/->on-heap (kv/get-value snapshot (long->bytes 1))))))
      (t/is (nil? (kv/get-value snapshot (long->bytes 2)))))))

(t/deftest test-can-read-write-concurrently
  (fkv/with-kv-store [kv-store]
    (let [w-fs (for [_ (range 128)]
                 (future
                   (store kv-store [[(long->bytes 1) (.getBytes "XTDB")]])))]
      @(first w-fs)
      (let [r-fs (for [_ (range 128)]
                   (future
                     (String. ^bytes (value kv-store (long->bytes 1)))))]
        (mapv deref w-fs)
        (doseq [r-f r-fs]
          (t/is (= "XTDB" @r-f)))))))

(t/deftest test-prev-and-next []
  (fkv/with-kv-store [kv-store]
    (doseq [[^String k v] {"a" 1 "c" 3}]
      (store kv-store [[(.getBytes k) (long->bytes v)]]))

    (with-open [snapshot (kv/new-snapshot kv-store)
                i (kv/new-iterator snapshot)]
      (t/testing "seek returns next valid key"
        (let [k (kv/seek i (mem/as-buffer (.getBytes "b")))]
          (t/is (= ["c" 3] [(String. (mem/->on-heap k)) (bytes->long (mem/->on-heap (kv/value i)))]))))
      (t/testing "prev, iterators aren't bidirectional"
        (t/is (= "a" (String. (mem/->on-heap (kv/prev i)))))
        (t/is (nil? (kv/prev i)))))))

(tcct/defspec test-basic-generative-store-and-get-value 20
  (prop/for-all [kvs (gen/not-empty (gen/map
                                     gen/simple-type-printable
                                     gen/small-integer {:num-elements 100}))]
                (let [kvs (->> (for [[k v] kvs]
                                 [(c/->value-buffer k)
                                  (c/->value-buffer v)])
                               (into {}))]
                  (fkv/with-kv-store [kv-store]
                    (store kv-store kvs)
                    (with-open [snapshot (kv/new-snapshot kv-store)]
                      (->> (for [[k v] kvs]
                             (= v (kv/get-value snapshot k)))
                           (every? true?)))))))

(tcct/defspec test-generative-kv-store-commands 20
  (prop/for-all [commands (gen/let [ks (gen/not-empty (gen/vector gen/simple-type-printable))]
                            (gen/not-empty (gen/vector
                                            (gen/one-of
                                             [(gen/tuple
                                               (gen/return :get-value)
                                               (gen/elements ks))
                                              (gen/tuple
                                               (gen/return :seek)
                                               (gen/elements ks))
                                              (gen/tuple
                                               (gen/return :seek+value)
                                               (gen/elements ks))
                                              (gen/tuple
                                               (gen/return :seek+nexts)
                                               (gen/elements ks))
                                              (gen/tuple
                                               (gen/return :seek+prevs)
                                               (gen/elements ks))
                                              (gen/tuple
                                               (gen/return :fsync))
                                              (gen/tuple
                                               (gen/return :delete)
                                               (gen/elements ks))
                                              (gen/tuple
                                               (gen/return :store)
                                               (gen/elements ks)
                                               gen/small-integer)]))))]
                (fkv/with-kv-store [kv-store]
                  (let [expected (->> (reductions
                                       (fn [[state] [op k v :as command]]
                                         (case op
                                           :get-value [state (get state (c/->value-buffer k))]
                                           :seek [state (ffirst (subseq state >= (c/->value-buffer k)))]
                                           :seek+value [state (second (first (subseq state >= (c/->value-buffer k))))]
                                           :seek+nexts [state (subseq state >= (c/->value-buffer k))]
                                           :seek+prevs [state (some->> (subseq state >= (c/->value-buffer k))
                                                                       (ffirst)
                                                                       (rsubseq state <= ))]
                                           :fsync [state]
                                           :delete [(dissoc state (c/->value-buffer k))]
                                           :store [(assoc state
                                                          (c/->value-buffer k)
                                                          (c/->value-buffer v))]))
                                       [(sorted-map-by mem/buffer-comparator)]
                                       commands)
                                      (rest)
                                      (map second))]
                    (->> (for [[[op k v :as command] expected] (map vector commands expected)]
                           (= expected
                              (case op
                                :get-value (with-open [snapshot (kv/new-snapshot kv-store)]
                                             (kv/get-value snapshot (c/->value-buffer k)))
                                :seek (with-open [snapshot (kv/new-snapshot kv-store)
                                                  i (kv/new-iterator snapshot)]
                                        (kv/seek i (c/->value-buffer k)))
                                :seek+value (with-open [snapshot (kv/new-snapshot kv-store)
                                                        i (kv/new-iterator snapshot)]
                                              (when (kv/seek i (c/->value-buffer k))
                                                (kv/value i)))
                                :seek+nexts (with-open [snapshot (kv/new-snapshot kv-store)
                                                        i (kv/new-iterator snapshot)]
                                              (when-let [k (kv/seek i (c/->value-buffer k))]
                                                (cons [(mem/copy-to-unpooled-buffer k)
                                                       (mem/copy-to-unpooled-buffer (kv/value i))]
                                                      (->> (repeatedly
                                                            (fn []
                                                              (when-let [k (kv/next i)]
                                                                [(mem/copy-to-unpooled-buffer k)
                                                                 (mem/copy-to-unpooled-buffer (kv/value i))])))
                                                           ;; if there's a bug in next, this might be infinite
                                                           (take (count commands))
                                                           (take-while identity)
                                                           (vec)))))
                                :seek+prevs (with-open [snapshot (kv/new-snapshot kv-store)
                                                        i (kv/new-iterator snapshot)]
                                              (when-let [k (kv/seek i (c/->value-buffer k))]
                                                (cons [(mem/copy-to-unpooled-buffer k)
                                                       (mem/copy-to-unpooled-buffer (kv/value i))]
                                                      (->> (repeatedly
                                                            (fn []
                                                              (when-let [k (kv/prev i)]
                                                                [(mem/copy-to-unpooled-buffer k)
                                                                 (mem/copy-to-unpooled-buffer (kv/value i))])))
                                                           (take (count commands))
                                                           (take-while identity)
                                                           (vec)))))
                                :fsync (kv/fsync kv-store)
                                :delete (store kv-store [[(c/->value-buffer k) nil]])
                                :store (store kv-store
                                                 [[(c/->value-buffer k)
                                                   (c/->value-buffer v)]]))))
                         (every? true?))))))

(t/deftest test-performance-off-heap
  (if (and (Boolean/parseBoolean (System/getenv "XTDB_KV_PERFORMANCE"))
           (if-let [backend (System/getenv "XTDB_KV_PERFORMANCE_BACKEND")]
             (= backend (str (:xtdb/module fkv/*kv-opts*)))
             true))
    (fkv/with-kv-store [kv-store]
      (let [n 1000000
            ks (vec (for [n (range n)]
                      (mem/->off-heap (.getBytes (format "%020x" n)))))]
        (println (:xtdb/module fkv/*kv-opts*) "off-heap")
        (t/is (= n (count ks)))
        (t/is (mem/off-heap? (first ks)))

        (System/gc)
        (println "Writing")
        (time
         (store kv-store (for [k ks]
                              [k k])))

        (System/gc)
        (println "Reading")
        (time
         (do (dotimes [_ 10]
               (time
                (with-open [snapshot (kv/new-snapshot kv-store)
                            i (kv/new-iterator snapshot)]
                  (dotimes [idx n]
                    (let [idx (- (dec n) idx)
                          k (get ks idx)]
                      (assert (mem/buffers=? k (kv/seek i k)))
                      (assert (mem/buffers=? k (kv/value i))))))))
             (println "Done")))
        (println)))
    (t/is true)))
