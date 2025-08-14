(ns xtdb.pgwire.copy-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.next.jdbc :as xt-jdbc]
            [xtdb.pgwire :as pgw]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

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
