(ns xtdb.pgwire.copy-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.next.jdbc :as xt-jdbc]
            [xtdb.pgwire :as pgw]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.nio.channels WritableByteChannel]
           [java.util Map]
           [org.postgresql.copy CopyIn]
           [xtdb.arrow Relation ArrowUnloader$Mode]))

(t/use-fixtures :each tu/with-allocator tu/with-mock-clock tu/with-node)

(t/deftest test-copy-in-transit
  (with-open [conn (jdbc/get-connection tu/*node*)]
    (t/testing "json"
      (let [copy-in (xt-jdbc/copy-in conn "COPY foo FROM STDIN WITH (FORMAT 'transit-json')")]
        (let [bytes (serde/write-transit-seq [{:xt/id 1, :v 1} {:xt/id 2, :v 2}] :json)]
          (.writeToCopy copy-in bytes 0 (alength bytes)))
        (let [bytes (serde/write-transit-seq [{:xt/id 3, :v 3} {:xt/id 4, :v 4}] :json)]
          (.writeToCopy copy-in bytes 0 (alength bytes)))

        (t/is (= 4 (.endCopy copy-in))))

      (t/is (= [{:xt/id 1, :v 1} {:xt/id 2, :v 2} {:xt/id 3, :v 3} {:xt/id 4, :v 4}]
               (xt/q conn "SELECT * FROM foo ORDER BY _id"))))

    (t/testing "msgpack"
      (let [copy-in (xt-jdbc/copy-in conn "COPY bar FROM STDIN WITH (FORMAT 'transit-msgpack')")]
        (let [bytes (serde/write-transit-seq [{:xt/id 1, :v 1} {:xt/id 2, :v 2}] :msgpack)]
          (.writeToCopy copy-in bytes 0 (alength bytes)))
        (let [bytes (serde/write-transit-seq [{:xt/id 3, :v 3} {:xt/id 4, :v 4}] :msgpack)]
          (.writeToCopy copy-in bytes 0 (alength bytes)))

        (t/is (= 4 (.endCopy copy-in))))

      (t/is (= [{:xt/id 1, :v 1} {:xt/id 2, :v 2} {:xt/id 3, :v 3} {:xt/id 4, :v 4}]
               (xt/q conn "SELECT * FROM bar ORDER BY _id"))))

    (t/testing "error - no format specified"
      (t/is (anomalous? [:incorrect ::pgw/invalid-copy-format]
                        (err/wrap-anomaly {}
                          (xt-jdbc/copy-in conn "COPY baz FROM STDIN WITH ()")))))

    (t/testing "error"
      (t/is (anomalous? [:incorrect ::err/json-parse "Unexpected close marker '}'"]
                        (err/wrap-anomaly {}
                          (let [copy-in (xt-jdbc/copy-in conn "COPY foo FROM STDIN WITH (FORMAT 'transit-json')")
                                bytes (.getBytes "},,,,")]
                            (.writeToCopy copy-in bytes 0 (alength bytes))

                            (.endCopy copy-in))))))

    (t/testing "life goes on"
      (xt/submit-tx tu/*node* [[:put-docs :baz {:xt/id 1, :v 1}]])

      (t/is (= [{:xt/id 1, :v 1}]
               (xt/q tu/*node* "SELECT * FROM baz ORDER BY _id"))))))

(t/deftest test-copy-in-string-keys-4677
  (with-open [conn (jdbc/get-connection tu/*node*)]
    (t/testing "fails with just xt/id string - missing id"
      (let [copy-in (xt-jdbc/copy-in conn "COPY foo FROM STDIN WITH (FORMAT 'transit-json')")]
        (let [bytes (serde/write-transit-seq [{"xt/id" 1, "v" 1} {"xt/id" 2, "v" 2}] :json)]
          (.writeToCopy copy-in bytes 0 (alength bytes)))

        (let [bytes (serde/write-transit-seq [{"xt/id" 3, "v" 3} {"xt/id" 4, "v" 4}] :json)]
          (.writeToCopy copy-in bytes 0 (alength bytes)))

        (t/is (anomalous? [:incorrect :missing-id]
                          (err/wrap-anomaly {}
                            (.endCopy copy-in))))))

    (t/testing "fails with just xt/id string - missing id"
      (let [copy-in (xt-jdbc/copy-in conn "COPY foo FROM STDIN WITH (FORMAT 'transit-json')")]
        (let [bytes (serde/write-transit-seq [{"_id" 1, "v" 1} {"_id" 2, "v" 2}] :json)]
          (.writeToCopy copy-in bytes 0 (alength bytes)))

        (let [bytes (serde/write-transit-seq [{"_id" 3, "v" 3} {"_id" 4, "v" 4}] :json)]
          (.writeToCopy copy-in bytes 0 (alength bytes)))

        (t/is (= 4 (.endCopy copy-in))))

      (t/is (= [{:xt/id 1, :v 1} {:xt/id 2, :v 2} {:xt/id 3, :v 3} {:xt/id 4, :v 4}]
               (xt/q conn "SELECT * FROM foo ORDER BY _id"))))))

(defn- copy-in->channel ^java.nio.channels.WritableByteChannel [^CopyIn copy-in]
  (reify WritableByteChannel
    (write [_ buf]
      (let [remaining (.remaining buf)
            bytes (byte-array remaining)]
        (.get buf bytes)
        (.writeToCopy copy-in bytes 0 remaining)
        remaining))
    (isOpen [_] true)
    (close [_] nil)))

(t/deftest test-arrow-stream-copy-no-tx
  (with-open [conn (jdbc/get-connection tu/*node*)]
    (t/testing "stream, single batch"
      (let [copy-in (xt-jdbc/copy-in conn "COPY single_batch_stream FROM STDIN WITH (FORMAT 'arrow-stream')")]
        (with-open [rel (Relation/openFromRows tu/*allocator* [{:xt/id 1, :v 1} {:xt/id 2, :v 2}])]
          (let [bytes (.getAsArrowStream rel)]
            (.writeToCopy copy-in bytes 0 (alength bytes))))

        (t/is (= 2 (.endCopy copy-in))))

      (t/is (= [{:xt/id 1, :v 1} {:xt/id 2, :v 2}]
               (xt/q conn "SELECT * FROM single_batch_stream ORDER BY _id"))))

    (t/testing "file, single batch"
      (let [copy-in (xt-jdbc/copy-in conn "COPY single_batch_file FROM STDIN WITH (FORMAT 'arrow-file')")]
        (with-open [rel (Relation/openFromRows tu/*allocator* [{:xt/id 1, :v 1} {:xt/id 2, :v 2}])]
          (let [bytes (.getAsArrowFile rel)]
            (.writeToCopy copy-in bytes 0 (alength bytes))))

        (t/is (= 2 (.endCopy copy-in))))

      (t/is (= [{:xt/id 1, :v 1} {:xt/id 2, :v 2}]
               (xt/q conn "SELECT * FROM single_batch_file ORDER BY _id"))))

    (t/testing "stream, multiple batches"
      (let [copy-in (xt-jdbc/copy-in conn "COPY multi_batch_stream FROM STDIN WITH (FORMAT 'arrow-stream')")
            ch (copy-in->channel copy-in)]
        (util/with-open [rel (Relation. tu/*allocator* #xt/schema [{"_id" :i64} {"v" :i64}])
                         unl (.startUnload rel ch ArrowUnloader$Mode/STREAM)]
          (.writeRows rel (->> [{:xt/id 1, :v 1} {:xt/id 2, :v 2}]
                               (into-array Map)))
          (.writePage unl)
          (.clear rel)

          (.writeRows rel (->> [{:xt/id 3, :v 3} {:xt/id 4, :v 4}]
                               (into-array Map)))
          (.writePage unl)
          (.end unl))

        (t/is (= 4 (.endCopy copy-in))))

      (t/is (= [{:xt/id 1, :v 1, :xt/valid-from #xt/zdt "2020-01-03[UTC]"}
                {:xt/id 2, :v 2, :xt/valid-from #xt/zdt "2020-01-03[UTC]"}
                {:xt/id 3, :v 3, :xt/valid-from #xt/zdt "2020-01-04[UTC]"}
                {:xt/id 4, :v 4, :xt/valid-from #xt/zdt "2020-01-04[UTC]"}]
               (xt/q conn "SELECT *, _valid_from FROM multi_batch_stream ORDER BY _id"))))))

(t/deftest test-arrow-stream-copy-in-tx
  (with-open [conn (jdbc/get-connection tu/*node*)]
    (jdbc/with-transaction [_ conn]
      (t/testing "stream, multiple batches"
        (let [copy-in (xt-jdbc/copy-in conn "COPY multi_batch_stream FROM STDIN WITH (FORMAT 'arrow-stream')")
              ch (copy-in->channel copy-in)]
          (util/with-open [rel (Relation. tu/*allocator* #xt/schema [{"_id" :i64} {"v" :i64}])
                           unl (.startUnload rel ch ArrowUnloader$Mode/STREAM)]
            (.writeRows rel (->> [{:xt/id 1, :v 1} {:xt/id 2, :v 2}]
                                 (into-array Map)))
            (.writePage unl)
            (.clear rel)

            (.writeRows rel (->> [{:xt/id 3, :v 3} {:xt/id 4, :v 4}]
                                 (into-array Map)))
            (.writePage unl)
            (.end unl))

          (t/is (= 4 (.endCopy copy-in))))))

    (t/is (= [{:xt/id 1, :v 1, :xt/valid-from #xt/zdt "2020-01-01[UTC]"}
              {:xt/id 2, :v 2, :xt/valid-from #xt/zdt "2020-01-01[UTC]"}
              {:xt/id 3, :v 3, :xt/valid-from #xt/zdt "2020-01-01[UTC]"}
              {:xt/id 4, :v 4, :xt/valid-from #xt/zdt "2020-01-01[UTC]"}]
             (xt/q conn "SELECT *, _valid_from FROM multi_batch_stream ORDER BY _id"))
          "all at the same valid-time this time")))
