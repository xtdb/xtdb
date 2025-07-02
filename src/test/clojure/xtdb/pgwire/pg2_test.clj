(ns xtdb.pgwire.pg2-test
  (:require [clojure.test :refer [deftest is testing] :as t]
            [cognitect.transit :as transit]
            [pg.core :as pg]
            [xtdb.api :as xt]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu])
  (:import (java.nio ByteBuffer)
           (java.util Arrays UUID)
           (org.pg.codec CodecParams)
           (org.pg.enums OID)
           (org.pg.error PGError PGErrorResponse)))

(def ^:dynamic ^:private *port* nil)
(def ^:dynamic ^:private ^xtdb.api.DataSource *server* nil)

(defn with-server-and-port [f]
  (let [server (-> tu/*node* :system :xtdb.pgwire/server :read-write)]
    (binding [*server* server
              *port* (:port server)]
      (f))))

(t/use-fixtures :each tu/with-allocator tu/with-mock-clock tu/with-node with-server-and-port)

(defn- pg-config [params]
  (merge {:host "localhost"
          :port *port*
          :user "xtdb"
          :database "xtdb"}
         params))

;; connect to the database
(defn- pg-conn ^org.pg.Connection [params]
  (pg/connect (pg-config params)))

(deftest txs-error-as-json-3866
  (with-open [conn (pg-conn {})]
    (is (thrown-with-msg? PGErrorResponse
                          #"code=08P01, .*, message=Cannot put documents with columns: #\{\"_system_time\"\}"
                          (pg/execute conn "INSERT INTO docs (_id, _system_time) VALUES (1, DATE '2020-01-01')")))
    (testing "xt.txs.error column renders as json without errors"
      (pg/execute conn "SELECT * FROM xt.txs"))))

(deftest test-pg
  (with-open [conn (pg-conn {})]
    (t/is (= [{:v 1}]
             (pg/query conn "SELECT 1 v")))))

(deftest test-unspecified-param-types
  (with-open [conn (pg-conn {})]

    (t/testing "params with unspecified types are assumed to be text"

      (t/is (= [{:v "1"}]
               (pg/execute conn "SELECT $1 v" {:params ["1"]}))
            "given text params query is executable")

      (t/is (= [{:v "1"}]
               (pg/execute conn "SELECT $1 v" {:params ["1"]
                                               :oids [OID/DEFAULT]}))
            "params declared with the default oid (0) by clients are
             treated as unspecified and therefore considered text")

      (t/is (thrown-with-msg?
             PGError #"cannot text-encode, oid: 25, type: java.lang.Long, value: 1"
             (pg/execute conn "SELECT $1 v" {:params [1]}))
            "non text params error"))

    (testing "params with unspecified types in DML error"
      ;;postgres is able to infer the type of the param from context such as the column type
      ;;of base tables referenced in the query and the operations present. However we are currently
      ;;not able to do this, because the EE isn't yet powerful enough to work in reverese and requires
      ;;all param types to be known, but also because we can't guarentee the types of any base tables
      ;;before executing a DML statement, as this isn't fixed until the indexer has indexed the tx.
      ;;
      ;;Therefore we choose to error in this case to avoid any surprising and unexecpted behaviour
      (t/is (thrown?
             PGError
             (pg/execute conn "INSERT INTO foo(_id, v) VALUES (1, $1)" {:params ["1"]}))
            "dml with unspecified params error")

      ;; without this we get a wrong parameter count error from pg2, which seems more like a bug in pg2
      (pg/close-cached-statements conn)

      (t/is (thrown-with-msg?
             PGErrorResponse
             #"Missing types for args - client must specify types for all non-null params"
             (pg/execute conn "INSERT INTO foo(_id, v) VALUES (1, $1)" {:params ["1"]
                                                                        :oids [OID/DEFAULT]}))
            "params declared with the default oid (0) by clients are
             treated as unspecified and therefore also error")

      (t/testing "... unless it's null"
        (pg/execute conn "INSERT INTO foo(_id, v) VALUES (2, $1)" {:params [nil], :oids [OID/DEFAULT]})

        (t/is (= [{:_id 2, :v nil}]
                 (pg/query conn "SELECT * FROM foo WHERE _id = 2")))))))

(deftest test-postgres-native-params
  (with-open [conn (pg-conn {})]

    (t/is (= [{:v 1}]
             (pg/execute conn "SELECT $1 v" {:params [1]
                                             :oids [OID/INT8]}))
          "Users can execute queries with explicit param types")

    (t/testing "Users can execute dml with explicit param types"

      (pg/execute conn
                  "INSERT INTO foo(_id, v) VALUES (1, $1)"
                  {:params [1]
                   :oids [OID/INT8]})

      (t/is (= [{:_id 1, :v 1}]
               (pg/query conn "SELECT * FROM foo"))))))

(deftest test-java-time-instant-ts
  ;;NOTE https://github.com/igrishaev/pg2/issues/46
  (with-open [conn (pg-conn {})]
    (t/is (= [{:v #xt/date-time "2030-01-04T12:44:55"}]
             (pg/execute conn "SELECT $1 v" {:params [#xt/instant "2030-01-04T12:44:55Z"]
                                             :oids [OID/TIMESTAMP]}))
          "when reading param, zone is ignored and instant is treated as a timestamp")))

(deftest test-java-time-instant-tstz
  ;;NOTE https://github.com/igrishaev/pg2/issues/46
  (with-open [conn (pg-conn {})]
    (t/is (= [{:v #xt/offset-date-time "2030-01-04T12:44:55Z"}]
             (pg/execute conn "SELECT $1 v" {:params [#xt/instant "2030-01-04T12:44:55Z"]
                                             :oids [OID/TIMESTAMPTZ]}))
          "when reading param, zone is honored (UTC) and instant is treated as a timestamptz")))

(deftest test-java-uuid
  (let [uuid (UUID/randomUUID)]
    (with-open [conn (pg-conn {})]
      (t/is (= [{:v #uuid "7dd2ed62-bb05-43c8-b289-5503d9b19ee6"}]
               (pg/execute conn "SELECT $1 v" {:params [#uuid "7dd2ed62-bb05-43c8-b289-5503d9b19ee6"]
                                               :oids [OID/UUID]})))
      (t/is (= [{:v uuid}]
               (pg/execute conn "SELECT $1 v" {:params [uuid]
                                               :oids [OID/UUID]})))
      (testing "insert uuid as id and select by it"
        (pg/execute conn "INSERT INTO foouuid (_id, v) values ($1, $2)" {:params [uuid "foo"]
                                                                         :oids [OID/UUID OID/VARCHAR]})
        (t/is (= [{:_id uuid, :v "foo"}]
                 (pg/query conn (str "SELECT * FROM foouuid WHERE _id = UUID '" uuid "'"))))
        (t/is (= [{:xt/id uuid, :v "foo"}]
                 (xt/q tu/*node* ["SELECT * FROM foouuid WHERE _id = ?", uuid]))))
      (testing "cast text to UUID"
        (t/is (= [{:v uuid}]
                 (pg/execute conn (str "SELECT '" uuid "'::uuid v"))))))))

(deftest test-cast-text-data-type
  (with-open [conn (pg-conn {})]
    (t/is (= [{:v "101"}]
             (pg/execute conn "SELECT 101::text v")))))

(deftest test-pg-boolean-param
  (doseq [binary? [true false]
          v [true false]]

    (t/testing (format "binary?: %s, value?: %s" binary? v)
      (with-open [conn (pg-conn {:binary-encode? binary? :binary-decode? binary?})]
        (t/is (= [{:v v}]
                 (pg/execute conn "SELECT ? v" {:oids [OID/BOOL]
                                                :params [v]})))))))

(t/deftest test-pg2-begin-4182
  (with-open [conn (pg-conn {})]
    (pg/begin conn)
    (pg/execute conn "INSERT INTO foo RECORDS {_id: 1}")
    (pg/commit conn)

    (pg/with-transaction [tx conn]
      (pg/execute conn "INSERT INTO foo RECORDS {_id: 2}"))

    (t/is (= (pg/execute conn "SELECT * FROM foo ORDER BY _id")
             [{:_id 1} {:_id 2}]))))

(defn remaining-bytes ^bytes [^ByteBuffer buf]
  (let [res (byte-array (.remaining buf))]
    (.get buf res)
    res))

(def pg2-transit-processor
  (reify org.pg.processor.IProcessor
    (^String encodeTxt [_this ^Object obj ^CodecParams _codecParams]
      (String. (serde/write-transit obj :json)))
    (^Object decodeTxt [_this ^String s ^CodecParams _codecParams]
      (serde/read-transit (.getBytes s) :json))
    (^ByteBuffer encodeBin [_this ^Object obj ^CodecParams _codecParams]
      (ByteBuffer/wrap (serde/write-transit obj :json)))
    (^Object decodeBin [_this ^ByteBuffer buf ^CodecParams _codecParams]
      (serde/read-transit (remaining-bytes buf) :json))))

(deftest test-pg2-transit-params
  (let [m-in {:a 1
              :i (transit/tagged-value "xtdb/interval" "PT5S")}
        expected-m-out {"a" 1
                        "i" #xt/interval "PT5S"}
        insert-and-query (fn [conn m]
                           (pg/execute conn
                             "INSERT INTO foo (_id, v) VALUES (1, $1)"
                             {:params [m], :oids [(int 16384)]})
                           (pg/execute conn "SELECT v FROM foo"))]

    (testing "explicit transit serialization"
      (with-open [conn (pg-conn {:pg-params {"fallback_output_format" "transit"}})]
        (t/is (= (-> (insert-and-query conn (String. (serde/write-transit m-in :json)))
                     (update-in [0 :v] #(serde/read-transit (.getBytes ^String %) :json)))
                 [{:v expected-m-out}]))))

    (testing "custom type setup for transit serialization"
      (with-open [conn (pg-conn {:type-map {:pg_catalog/transit pg2-transit-processor}
                                 :pg-params {"fallback_output_format" "transit"}})]
        (t/is (= (insert-and-query conn m-in)
                 [{:v expected-m-out}]))))

    (testing "custom type setup for transit serialization, binary mode"
      (with-open [conn (pg-conn {:binary-encode? true
                                 :binary-decode? true
                                 :type-map {:pg_catalog/transit pg2-transit-processor}
                                 :pg-params {"fallback_output_format" "transit"}})]
        (t/is (= (insert-and-query conn m-in)
                 [{:v expected-m-out}]))))))

(deftest test-startup-params
  ;;TODO test should really explicitly set the default clock of the node and/or pgwire server
  ;;to something other than the param value in the test. But this currently isn't possible.
  (t/testing "TimeZone"
    (with-open [conn (pg-conn {:pg-params {"TimeZone" "Pacific/Tarawa"}})]
      (t/is (= [{:timezone "Pacific/Tarawa"}]
               (pg/execute conn "SHOW TIME ZONE"))
            "Exact case"))

    (with-open [conn (pg-conn {:pg-params {"TiMEzONe" "Pacific/Tarawa"}})]

      (t/is (= [{:timezone "Pacific/Tarawa"}]
               (pg/execute conn "SHOW TIME ZONE"))
            "arbitrary case"))))

(deftest error-propagation
  (let [warns (atom [])]
    (with-open [conn (pg-conn {:fn-notice (fn [notice] (swap! warns conj (:message notice)))})]
      (pg/execute conn "SELECT 2 AS b FROM docs GROUP BY b")
      ;; the fn-notice runs in a separate executor pool
      (Thread/sleep 100)
      (t/is (= #{"Table not found: docs" "Column not found: b"}
               (set @warns))))))

(deftest test-time
  (with-open [conn (pg-conn {})]
    (t/is (= [{:v "20:40:31.932254"}]
             (pg/execute conn "SELECT TIME '20:40:31.932254' v"))
          "time is returned as json")))

(deftest test-scalar-types
  (let [^bytes ba (byte-array [0 -16])]
    (with-open [conn (pg-conn {})]
      (t/is (Arrays/equals ba ^bytes (:v (first (pg/execute conn "SELECT $1 v" {:params [ba]
                                                                                :oids [OID/BYTEA]}))))
            "reading varbinary parameter"))))

(deftest pg-sleep-test
  (with-open [conn (pg-conn {})]
    (let [start (System/currentTimeMillis)]
      (pg/execute conn "SELECT pg_sleep(0.1)")
      (t/is (< 100 (- (System/currentTimeMillis) start)))
      (pg/execute conn "SELECT pg_sleep(0.1+0.1)")
      (t/is (< 300 (- (System/currentTimeMillis) start))))))

(deftest pg-sleep-for-test
  (with-open [conn (pg-conn {})]
    (let [start (System/currentTimeMillis)]
      (pg/execute conn "SELECT pg_sleep_for('0.100 second')")
      (t/is (< 100 (- (System/currentTimeMillis) start)))
      (pg/execute conn "SELECT pg_sleep_for('0.01 minute')")
      (t/is (< 160 (- (System/currentTimeMillis) start))))))

(t/deftest test-explain-query-with-params
  (with-open [conn (pg-conn {})]
    (let [[{:keys [plan]}] (pg/execute conn "EXPLAIN SELECT $1" {:params [""]})]
      (t/is (some? plan)))))

(t/deftest test-sql-with-leading-whitespace
  (with-open [conn (pg-conn {})]
    (pg/execute conn "     INSERT INTO test RECORDS {_id: 0, value: 'hi'}")))

(deftest keyword-roundtripping-4237
  (xt/execute-tx tu/*node* [["INSERT INTO docs RECORDS ?" {:_id 1, :foo :bar}]])

  (with-open [pg-conn (pg-conn {})]
    (t/is (= [{:_id 1, :foo "bar"}] (pg/execute pg-conn "FROM docs"))
          "if the driver doesn't know about keywords it might fall back to text")))

(deftest test-transit-id-formats
  (let [insert-and-query (fn [table doc]
                           (with-open [conn (pg-conn {})]
                             (pg/execute conn
                                         (str "INSERT INTO " table " RECORDS $1")
                                         {:params [(-> doc (serde/write-transit :json) (String.))]
                                          :oids [(int 16384)]})

                             (pg/execute conn (str "SELECT * FROM " table))))]

    (t/is (= [{:_id 1, :value 1}] (insert-and-query "table_a" {"_id" 1, "value" 1})))
    (t/is (= [{:_id 1, :value 1}] (insert-and-query "table_b" {"_id" 1, :value 1})))
    (t/is (= [{:_id 1, :value 1}] (insert-and-query "table_c" {:xt/id 1, "value" 1})))
    (t/is (thrown-with-msg? org.pg.error.PGErrorResponse #"missing-id" (insert-and-query "table_d" {"xt/id" 1, "value" 1})))
    ;; Asserts that :xt/id is currently rejected as a document ID — may be allowed in future #4415
    (t/is (thrown-with-msg? org.pg.error.PGErrorResponse #"missing-id" (insert-and-query "table_e" {:xt/id 1, :value 1})))
    (t/is (thrown-with-msg? org.pg.error.PGErrorResponse #"missing-id" (insert-and-query "table_f" {:xt/db 1, :value 1})))
    (t/is (= [{:_id 1, :value 1}] (insert-and-query "table_g" {:_id 1, :value 1})))))

;; - pgwire
;; - dml execute
