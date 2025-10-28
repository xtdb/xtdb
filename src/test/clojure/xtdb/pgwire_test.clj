(ns xtdb.pgwire-test
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing] :as t]
            [clojure.tools.logging :as log]
            [honey.sql :as hsql]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as result-set]
            [xtdb.api :as xt]
            [xtdb.basis :as basis]
            [xtdb.db-catalog :as db]
            [xtdb.logging :as logging]
            [xtdb.next.jdbc :as xt-jdbc]
            [xtdb.node :as xtn]
            [xtdb.pgwire :as pgw]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import (java.io InputStream)
           (java.lang Thread$State)
           (java.net Socket)
           (java.nio ByteBuffer)
           (java.sql Array Connection PreparedStatement ResultSet SQLWarning Statement Timestamp Types)
           (java.time Clock Instant LocalDate LocalDateTime OffsetDateTime ZoneId ZoneOffset ZonedDateTime)
           (java.util Arrays Calendar List TimeZone)
           (java.util.concurrent CountDownLatch TimeUnit)
           (org.postgresql.util PGobject PSQLException)
           (xtdb JsonSerde)
           (xtdb.api DataSource$ConnectionBuilder)
           xtdb.api.log.Log$Message$FlushBlock
           xtdb.pgwire.Server))

(set! *warn-on-reflection* false) ; gagh! lazy. don't do this.
(set! *unchecked-math* false)

(def ^:dynamic *port* nil)
(def ^:dynamic ^xtdb.api.DataSource *server* nil)

(defn- in-system-tz ^java.time.ZonedDateTime [^ZonedDateTime zdt]
  (.withZoneSameInstant zdt (ZoneId/systemDefault)))

(defn read-ex [^PSQLException e]
  (let [sem (.getServerErrorMessage e)]
    (->> {:sql-state (.getSQLState e)
          :message (.getMessage sem)
          :detail (some-> (.getDetail sem)
                          not-empty
                          (serde/read-transit :json))
          :routine (some-> (.getRoutine sem) not-empty)}
         (into {} (filter (comp some? val))))))

(defmacro reading-ex [& body]
  `(try
     ~@body
     (catch PSQLException e#
       (read-ex e#))))

(t/use-fixtures :once

  #_ ; HACK commented out while we're not bringing in the Flight JDBC driver
  (fn [f]
    ;; HACK see https://issues.apache.org/jira/browse/ARROW-18296
    ;; this ensures the FSQL driver is at the end of the DriverManager list
    (when-let [fsql-driver (->> (enumeration-seq (DriverManager/getDrivers))
                                (filter #(instance? ArrowFlightJdbcDriver %))
                                first)]
      (DriverManager/deregisterDriver fsql-driver)
      (DriverManager/registerDriver fsql-driver))
    (f)))

(defn with-server-and-port [f]
  (let [server (-> tu/*node* :system ::pgw/server :read-write)]
    (binding [*server* server
              *port* (:port server)]
      (f))))

(t/use-fixtures :each tu/with-allocator tu/with-mock-clock tu/with-node with-server-and-port)

(defn serve
  (^xtdb.pgwire.Server [] (serve {}))
  (^xtdb.pgwire.Server [opts] (pgw/serve tu/*node* (merge {:num-threads 1
                                                           :allocator tu/*allocator*
                                                           :drain-wait 250}
                                                          opts))))

(defn jdbc-conn
  (^Connection [] (jdbc-conn nil))
  (^Connection [opts]
   (-> ^DataSource$ConnectionBuilder
       (reduce (fn [^DataSource$ConnectionBuilder cb [k v]]
                 (.option cb k v))

               (-> (.createConnectionBuilder *server*)
                   (.user "xtdb")
                   (.port *port*)
                   (.database (:dbname opts "xtdb")))

               (filter (comp string? key) opts))

       (.build))))

(defn with-playground [f]
  (util/with-open [server (pgw/open-playground)]
    (binding [*port* (:port server)
              *server* server]
      (f))))

(defn- exec [^Connection conn ^String sql]
  (.execute (.createStatement conn) sql))

(deftest connect-with-next-jdbc-test
  (with-open [_ (jdbc-conn)])
  ;; connect a second time to make sure we are releasing server resources properly!
  (with-open [_ (jdbc-conn)]))

(defn- try-sslmode [sslmode]
  (try
    (with-open [_ (jdbc-conn {"sslmode" sslmode})])
    :ok
    (catch PSQLException e
      (if (= "The server does not support SSL." (.getMessage e))
        :unsupported
        (throw e)))))

(defn rs->maps [^ResultSet rs]
  (let [md (.getMetaData rs)]
    (-> (loop [res []]
          (if (.next rs)
            (recur (->>
                    (for [idx (range 1 (inc (.getColumnCount md)))
                          :let [obj (.getObject rs idx)]]
                      {(.getColumnName md idx) (cond-> obj
                                                 (instance? Array obj) (-> .getArray vec))})
                    (into {})
                    (conj res)))
            res))
        (vec))))

(defn result-metadata [stmt-or-result-set]
  (let [md (.getMetaData stmt-or-result-set)]
    (-> (for [idx (range 1 (inc (.getColumnCount md)))]
          {(.getColumnName md idx) (.getColumnTypeName md idx)})
        (vec))))

(defn param-metadata [stmt]
  (let [md (.getParameterMetaData stmt)]
    (-> (for [idx (range 1 (inc (.getParameterCount md)))]
          (.getParameterTypeName md idx))
        (vec))))

(defn- try-gssencmode [gssencmode]
  (try
    (with-open [_ (jdbc-conn {"gssEncMode" gssencmode})])
    :ok
    (catch PSQLException e
      (if (= "The server does not support GSS Encoding." (.getMessage e))
        :unsupported
        (throw e)))))

(t/deftest test-parse-client-creds
  (t/testing "test correct encoding"
    (t/is (= {:client-id "test-client" :client-secret "test-secret"}
             (pgw/parse-client-creds "test-client:test-secret"))))

  (t/testing "test invalid inputs"
    (t/is (anomalous? [:incorrect :xtdb/invalid-client-credentials #"Client credentials were not provided"]
                      (pgw/parse-client-creds nil)))
    (t/is (anomalous? [:incorrect :xtdb/invalid-client-credentials #"Client credentials were empty"]
                      (pgw/parse-client-creds "")))
    (t/is (anomalous? [:incorrect :xtdb/invalid-client-credentials #"Client credentials must be provided in the format 'client-id:client-secret'"]
                      (pgw/parse-client-creds "no-colon")))
    (t/is (anomalous? [:incorrect :xtdb/invalid-client-credentials #"Client credentials must be provided in the format 'client-id:client-secret'"]
                      (pgw/parse-client-creds "client:secret:extra")))))

(deftest gssenc-test
  (t/is (= :ok (try-gssencmode "disable")))
  (t/is (= :ok (try-gssencmode "prefer")))
  (t/is (= :unsupported (try-gssencmode "require"))))

(deftest query-test
  (with-open [conn (jdbc-conn)
              stmt (.createStatement conn)
              rs (.executeQuery stmt "SELECT a.a FROM (VALUES ('hello, world')) a (a)")]
    (is (= true (.next rs)))
    (is (= false (.next rs)))))

(deftest simple-query-test
  (with-open [conn (jdbc-conn {"preferQueryMode" "simple"})
              stmt (.createStatement conn)
              rs (.executeQuery stmt "SELECT a.a FROM (VALUES ('hello, world')) a (a)")]
    (is (= [{"a" "hello, world"}]
           (rs->maps rs)))))

(deftest prepared-query-test
  (with-open [conn (jdbc-conn {"prepareThreshold" 1})
              stmt (.prepareStatement conn "SELECT a.a FROM (VALUES ('hello, world')) a (a)")
              stmt2 (.prepareStatement conn "SELECT a.a FROM (VALUES ('hello, world2')) a (a)")]

    (with-open [rs (.executeQuery stmt)]
      (is (= true (.next rs)))
      (is (= false (.next rs))))

    (with-open [rs (.executeQuery stmt)]
      (is (= true (.next rs)))
      (is (= false (.next rs))))

    ;; exec queries a few times to trigger .execute prepared statements in jdbc

    (dotimes [_ 5]
      (with-open [rs (.executeQuery stmt2)]
        (is (= true (.next rs)))
        (is (= "hello, world2" (str (.getObject rs 1))))
        (is (= false (.next rs)))))

    (dotimes [_ 5]
      (with-open [rs (.executeQuery stmt)]
        (is (= true (.next rs)))
        (is (= "hello, world" (str (.getObject rs 1))))
        (is (= false (.next rs)))))))

(t/deftest prepared-stmt-sees-new-cols-4570
  (with-open [conn (jdbc-conn {"prepareThreshold" 1})]
    (jdbc/execute! conn ["INSERT INTO docs RECORDS {_id: 1}"])

    (t/is (= [{:_id 1, :_column_2 nil}]
             (jdbc/execute! conn ["SELECT _id, my_col FROM docs"])))

    (with-open [stmt (.prepareStatement conn "FROM docs SELECT my_col ORDER BY _id")
                star-stmt (.prepareStatement conn "FROM docs ORDER BY _id")]
      (with-open [rs (.executeQuery stmt)
                  rs-star (.executeQuery star-stmt)]
        (t/is (= [{:_column_1 nil}] (resultset-seq rs)))
        (t/is (= [{:_id 1}] (resultset-seq rs-star))))

      (jdbc/execute! conn ["INSERT INTO docs RECORDS {_id: 2, my_col: 'foo'}"])

      (with-open [rs (.executeQuery stmt)
                  rs-star (.executeQuery star-stmt)]
        (t/is (= [{:my_col nil} {:my_col "foo"}] (resultset-seq rs)))
        (t/is (= [{:_id 1, :my_col nil} {:_id 2, :my_col "foo"}] (resultset-seq rs-star)))))))

(deftest parameterized-query-test
  (with-open [conn (jdbc-conn)
              stmt (doto (.prepareStatement conn "SELECT a.a FROM (VALUES (?)) a (a)")
                     (.setObject 1 "hello, world"))
              rs (.executeQuery stmt)]

    (t/is (= [{:a "hello, world"}] (resultset-seq rs)))))

(def json-representation-examples
  "A map of entries describing sql value domains
  and properties of their json representation.

  :sql the SQL expression that produces the value

  :clj (optional) a clj value that we expect from clojure.data.json/read-str
  :clj-pred (optional) a fn that returns true if the parsed arg (via data.json/read-str) is what we expect"
  (letfn [(string [s]
            {:sql (str "'" s "'"), :clj s})

          (integer [i]
            {:sql (str i), :clj-pred #(= (bigint %) (bigint i))})

          (decimal [n & {:keys [add-zero]}]
            (let [d1 (bigdec n)
                  d2 (if add-zero (.setScale d1 1) d1)]
              {:sql (.toPlainString d2)
               :clj-pred #(= (bigdec %) (bigdec n))}))]

    ;;JSON NULL is a bit strange to test in that it as an element in an array
    ;;is perhaps one of the only ways its likely to appear in XTDB data.
    ;;as we return a SQL null at the top level and in maps omit the entry entirely
    ;;as ABSENT = NULL
    [{:sql "ARRAY[NULL]", :clj [nil]}

     {:sql "true", :clj true}

     (string "hello, world")
     (string "")
     (string "42")
     (string "2022-01-03")

     ;; numbers

     (integer 0)
     (integer -0)
     (integer 42)
     (integer Long/MAX_VALUE)

     (integer Long/MIN_VALUE)

     (decimal 0.0)
     (decimal -0.0)
     (decimal 3.14)
     (decimal 42.0)

     ;; does not work no exact decimal support currently
     (decimal Double/MIN_VALUE)
     (decimal Double/MAX_VALUE :add-zero true)

     ;; dates / times

     {:sql "DATE '2021-12-24'", :clj "2021-12-24"}
     {:sql "TIME '03:04:11'", :clj "03:04:11"}
     {:sql "TIMESTAMP '2021-03-04 03:04:11'", :clj "2021-03-04T03:04:11"}
     {:sql "TIMESTAMP '2021-03-04 03:04:11+02:00'", :clj "2021-03-04T03:04:11+02:00"}
     {:sql "TIMESTAMP '2021-12-24 11:23:44.003'", :clj "2021-12-24T11:23:44.003"}

     {:sql "INTERVAL '1' YEAR", :clj "P12M"}
     {:sql "INTERVAL '1' MONTH", :clj "P1M"}
     {:sql "INTERVAL 'P1DT1H1M1.123456S'" :clj "P1DT1H1M1.123456S"}

     {:sql "DATE '2021-12-24' - DATE '2021-12-23'", :clj 1}

     ;; arrays

     {:sql "ARRAY []", :clj []}
     {:sql "ARRAY [ARRAY ['42'], 42, '42']", :clj [["42"] 42 "42"]}]))

(deftest json-representation-test
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["SET FALLBACK_OUTPUT_FORMAT = 'json'"])
    (doseq [{:keys [sql, clj, clj-pred]} json-representation-examples]
      (testing (str "SQL expression " sql " should parse to " clj)
        (with-open [stmt (.prepareStatement conn (format "SELECT a FROM (VALUES (%s), (ARRAY [])) a (a)" sql))]
          ;; empty array to force polymoprhic/json return
          (with-open [rs (.executeQuery stmt)]
            ;; one row in result set
            (.next rs)

            (testing "json parses to expected clj value"
              (let [clj-value (.getObject rs 1)]
                (when clj
                  (is (= clj clj-value) "parsed value should = :clj"))
                (when clj-pred
                  (is (clj-pred clj-value) "parsed value should pass :clj-pred"))))))))))

(defn check-server-resources-freed [server]
  (testing "accept socket"
    (is (.isClosed (:accept-socket server))))

  (testing "accept thread"
    (is (= Thread$State/TERMINATED (.getState (:accept-thread server)))))

  (testing "thread pool shutdown"
    (is (.isShutdown (:thread-pool server)))
    (is (.isTerminated (:thread-pool server)))))

(deftest server-resources-freed-on-close-test
  (doseq [close-method [#(.close %)]]
    (with-open [server (pgw/serve tu/*node* {})]
      (close-method server)
      (check-server-resources-freed server))))

(defn q [conn sql]
  (jdbc/execute! conn sql {:builder-fn xt-jdbc/builder-fn}))

(defn q-seq [conn sql]
  (->> (jdbc/execute! conn sql {:builder-fn result-set/as-arrays})
       (rest)))

(defn ping [conn]
  (-> (q conn ["select 'ping' ping"])
      first
      :ping))

(deftest accept-thread-interrupt-closes-thread-test
  (with-open [server (serve {:accept-so-timeout 10})]
    (.interrupt (:accept-thread server))
    (.join (:accept-thread server) 1000)

    (is (= Thread$State/TERMINATED (.getState (:accept-thread server))))))

(deftest accept-thread-interrupt-allows-server-shutdown-test
  (with-open [server (serve {:accept-so-timeout 10})]
    (.interrupt (:accept-thread server))
    (.join (:accept-thread server) 1000)

    (.close server)
    (check-server-resources-freed server)))

(deftest accept-thread-socket-close-stops-thread-test
  (with-open [server (serve)]
    (.close (:accept-socket server))
    (.join (:accept-thread server) 1000)
    (is (= Thread$State/TERMINATED (.getState (:accept-thread server))))))

(deftest accept-thread-socket-close-allows-cleanup-test
  (with-open [server (serve)]
    (.close (:accept-socket server))
    (.join (:accept-thread server) 1000)
    (.close server)
    (check-server-resources-freed server)))

(defn- get-connections [server]
  (vals (:connections @(:server-state server))))

(defn- get-last-conn
  (^xtdb.pgwire.Connection [] (get-last-conn *server*))
  (^xtdb.pgwire.Connection [server] (last (sort-by :cid (get-connections server)))))

(defn- wait-for-close [server-conn ms]
  (deref (:close-promise @(:conn-state server-conn)) ms false))

(defn check-conn-resources-freed [{{:keys [^Socket socket]} :frontend}]
  (t/is (.isClosed socket)))

(deftest conn-force-closed-by-server-frees-resources-test
  (with-open [_ (jdbc-conn)]
    (let [srv-conn (get-last-conn)]
      (.close srv-conn)
      (check-conn-resources-freed srv-conn))))

(deftest conn-closed-by-client-frees-resources-test
  (with-open [client-conn (jdbc-conn)
              server-conn (get-last-conn)]
    (.close client-conn)
    (is (wait-for-close server-conn 500))
    (check-conn-resources-freed server-conn)))

(deftest server-close-closes-idle-conns-test
  (with-open [^Server server (serve {:drain-wait 0})]
    (binding [*server* server, *port* (:port server)]
      (with-open [_client-conn (jdbc-conn)
                  server-conn (get-last-conn server)]
        (.close server)
        (is (wait-for-close server-conn 500))
        (check-conn-resources-freed server-conn)))))

(deftest canned-response-test
  ;; quick test for now to confirm canned response mechanism at least doesn't crash!
  ;; this may later be replaced by client driver tests (e.g test sqlalchemy connect & query)
  (with-redefs [pgw/canned-responses [{:q "hello!"
                                       :cols [{:col-name "greet", :pg-type :json}]
                                       :rows (fn [_] [["\"hey!\""]])}]]
    (with-open [conn (jdbc-conn)]
      (is (= [{:greet "hey!"}] (q conn ["hello!"]))))))

(deftest concurrent-conns-test
  (with-open [server (serve {:num-threads 2})]
    (let [results (atom [])
          spawn (fn spawn []
                  (future
                    (with-open [conn (jdbc-conn)]
                      (swap! results conj (ping conn)))))
          futs (vec (repeatedly 10 spawn))]

      (is (every? #(not= :timeout (deref % 500 :timeout)) futs))
      (is (= 10 (count @results)))

      (.close server)
      (check-server-resources-freed server))))

(deftest concurrent-conns-close-midway-test
  (with-open [server (serve {:num-threads 2 :accept-so-timeout 10})]
    (binding [*server* server, *port* (:port server)]
      (logging/with-log-level 'xtdb.pgwire :off
        (let [spawn (fn spawn [i]
                      (future
                        (try
                          (with-open [conn (jdbc-conn {"loginTimeout" 1, "socketTimeout" 1})]
                            (loop [query-til (+ (System/currentTimeMillis)
                                                (* i 1000))]
                              (ping conn)
                              (when (< (System/currentTimeMillis) query-til)
                                (recur query-til))))
                          ;; we expect an ex here, whether or not draining
                          (catch PSQLException _))))

              futs (mapv spawn (range 10))]

          (is (some #(not= :timeout (deref % 1000 :timeout)) futs))

          (.close server)

          (is (every? #(not= :timeout (deref % 1000 :timeout)) futs))

          (check-server-resources-freed server))))))

;; the goal of this test is to cause a bunch of ping queries to block on parse
;; until the server is draining
;; and observe that connection continue until the multi-message extended interaction is done
;; (when we introduce read transactions I will probably extend this to short-lived transactions)
(deftest close-drains-active-extended-queries-before-stopping-test
  (util/with-open [server (serve {:num-threads 10})]
    (let [parse-sql @#'pgw/parse-sql
          {:keys [!closing?]} server
          latch (CountDownLatch. 10)]
      ;; redefine parse to block when we ping
      (with-redefs [pgw/parse-sql
                    (fn [query]
                      (if-not (str/starts-with? query "select 'ping'")
                        (parse-sql query)
                        (do
                          (.countDown latch)
                          ;; delay until we see a draining state
                          (loop [wait-until (+ (System/currentTimeMillis) 5000)]
                            (when (and (< (System/currentTimeMillis) wait-until)
                                       (not @!closing?))
                              (recur wait-until)))
                          (parse-sql query))))]
        (let [spawn (fn spawn [] (future (with-open [conn (jdbc-conn)] (ping conn))))
              futs (vec (repeatedly 10 spawn))]

          (is (.await latch 1 TimeUnit/SECONDS))

          (util/close server)

          (is (every? #(= "ping" (deref % 1000 :timeout)) futs))

          (check-server-resources-freed server))))))

;;TODO no current support for cancelling queries

(defn test-cancel [sql]
  (with-open [conn (jdbc-conn)
              stmt (jdbc/prepare conn [sql])]
    (future
      (Thread/sleep 2000)
      (log/debug "cancelling...")
      (.cancel stmt))
    (let [t0 (System/currentTimeMillis)
          ex (is (thrown? PSQLException (.executeQuery stmt)))
          exec-time (- (System/currentTimeMillis) t0)]
      (is (= "57014" (.getSQLState ex)))
      (is (-> ex .getServerErrorMessage .getMessage (.contains "cancel")))
      (is (< 1500 exec-time 2500)))
    (t/is (.isValid conn 3000))))

(deftest cancel_sleep
  (test-cancel "SELECT pg_sleep(10.0)"))

(deftest cancel_big_GENERATE_SERIES
  (test-cancel "FROM GENERATE_SERIES(1,10000000) AS system(_id) ORDER BY _id"))

(deftest server_close_interrupts_ongoing_query
  (with-open [^Server server (serve)]
    (binding [*server* server, *port* (:port server)]
      (with-open [client-conn (jdbc-conn)
                  stmt (jdbc/prepare client-conn ["SELECT pg_sleep(10.0)"])
                  server-conn (get-last-conn server)]
        (future
          (Thread/sleep 2000)
          (log/debug "closing server...")
          (.close server))
        (let [t0 (System/currentTimeMillis)
              ex (is (thrown? PSQLException (.executeQuery stmt)))
              exec-time (- (System/currentTimeMillis) t0)]
          (is (= "08006" (.getSQLState ex)))
          (is (< 1500 exec-time 2500)))
        (is (wait-for-close server-conn 500))
        (check-conn-resources-freed server-conn)
        (check-server-resources-freed server)))))

(deftest jdbc-prepared-query-close-test
  (with-open [conn (jdbc-conn {"prepareThreshold" 1
                               "preparedStatementCacheQueries" 0
                               "preparedStatementCacheMiB" 0})]
    ;; the Driver now creates a couple of statements.
    ;; we do close this, but the PG driver appears to retain it
    (t/is (= #{"S_1" "S_2"} (set (keys (:prepared-statements @(:conn-state (get-last-conn)))))))

    (dotimes [i 3]
      (with-open [stmt (.prepareStatement conn (format "SELECT a.a FROM (VALUES (%s)) a (a)" i))]
        (.executeQuery stmt)))

    (t/testing "no portal should remain, they are closed by sync, explicit close or rebinding the unnamed portal"
      (t/is (empty? (:portals @(:conn-state (get-last-conn))))))

    (t/testing "the last statement should still exist as they last the duration of the session and are only closed by
                an explicit close message, which the pg driver sends between execs"
      ;; S_5 because initial qs, then i == 3
      (t/is (= #{"S_1" "S_2" "S_5"} (set (keys (:prepared-statements @(:conn-state (get-last-conn))))))))))

(defn psql-available?
  "Returns true if psql is available in $PATH"
  []
  (try (= 0 (:exit (sh/sh "which" "psql"))) (catch Throwable _ false)))

(defn psql-session
  "Takes a function of two args (send, read).

  Send puts a string in to psql stdin, reads the next string from psql stdout. You can use (read :stderr) if you wish to read from stderr instead."
  [f]
  ;; there are other ways to do this, but its a straightforward factoring that removes some boilerplate for now.
  (let [^List argv ["psql" "-h" "localhost" "-p" (str *port*) "--csv" "xtdb"]
        pb (ProcessBuilder. argv)
        p (.start pb)
        stdout (.getInputStream p)
        stderr (.getErrorStream p)
        stdin (.getOutputStream p)

        send
        (fn [^String s]
          (.write stdin (.getBytes s "utf-8"))
          (.flush stdin))

        read
        (fn read
          ([] (read :stdout))
          ([stream]
           (let [^InputStream in (case stream :stderr stderr :stdout stdout stream)]
             (loop [wait-until (+ (System/currentTimeMillis) 1000)]
               (or (when (pos? (.available in))
                     (let [barr (byte-array (.available in))]
                       (.read in barr)
                       (let [read-str (String. barr)]
                         (case stream
                           :stderr (str/split-lines read-str)
                           :stdout (when-not (str/starts-with? read-str "Null display is")
                                     (csv/read-csv read-str))))))

                   (when (< wait-until (System/currentTimeMillis))
                     :timeout)

                   (recur wait-until))))))]
    (try
      (f send read)
      (finally
        (when (.isAlive p)
          (.destroy p))

        (is (.waitFor p 1000 TimeUnit/MILLISECONDS))
        (is (#{143, 0} (.exitValue p)))

        (util/try-close stdin)
        (util/try-close stdin)
        (util/try-close stderr)))))

;; define psql tests if psql is available on path
;; (will probably move to a selector)
(when (psql-available?)
  (deftest psql-connect-test
    (let [{:keys [exit, out]} (sh/sh "psql" "-h" "localhost" "-p" (str *port*) "xtdb" "-c" "select 'ping' ping")]
      (is (= 0 exit))
      (is (str/includes? out " ping\n(1 row)")))))

(deftest ssl-test
  (t/testing "no SSL config supplied"
    (t/are [sslmode expect] (= expect (try-sslmode sslmode))
      "disable" :ok
      "allow" :ok
      "prefer" :ok

      "require" :unsupported
      "verify-ca" :unsupported
      "verify-full" :unsupported))

  (with-open [node (xtn/start-node {:server {:ssl {:keystore (io/file (io/resource "xtdb/pgwire/xtdb.jks"))
                                                   :keystore-password "password123"}}})]
    (binding [*port* (.getServerPort node)]
      (with-open [conn (jdbc-conn {"sslmode" "require"})]
        (jdbc/execute! conn ["INSERT INTO foo (_id) VALUES (1)"])
        (t/is (= [{:xt/id 1}]
                 (q conn ["SELECT * FROM foo"]))))

      (when (psql-available?)
        (let [{:keys [exit, out]} (sh/sh "psql" "-h" "localhost" "-p" (str *port*) "-c" "\\conninfo" "xtdb")]
          (is (= 0 exit))
          (is (str/includes? out "You are connected"))
          (is (str/includes? out "SSL connection (protocol: TLSv1.3")))))))

(when (psql-available?)
  (deftest psql-interactive-test
    (psql-session
     (fn [send read]
       (testing "ping"
         (send "select 'ping';\n")
         (is (= [["_column_1"] ["ping"]] (read))))

       (testing "numeric printing"
         (send "select a.a from (values (42)) a (a);\n")
         (is (= [["a"] ["42"]] (read))))

       (testing "expecting column name"
         (send "select a.flibble from (values (42)) a (flibble);\n")
         (is (= [["flibble"] ["42"]] (read))))

       (testing "mixed type col"
         (send "select a.a from (values (42), ('hello!'), (array [1,2,3])) a (a);\n")
         (is (= [["a"] ["42"] ["\"hello!\""] ["[1,2,3]"]] (read))))

       (testing "error during plan"
         (send "slect baz.a from baz;\n")
         (is (-> (second (read :stderr))
               (str/includes? "line 1:0 mismatched input 'slect' expecting")))

         (testing "query error allows session to continue"
           (send "select 'ping';\n")
           (is (= [["_column_1"] ["ping"]] (read)))))

       (testing "error during query execution"
         (send "select (1 / 0) from (values (42)) a (a);\n")
         (is (= ["ERROR:  data exception - division by zero"
                 "DETAIL:  {\"category\":\"cognitect.anomalies\\/incorrect\",\"code\":\"xtdb.expression\\/division-by-zero\",\"message\":\"data exception - division by zero\"}"]
                (read :stderr)))

         (with-redefs [pgw/cmd-exec-query (fn [& _]  (throw (Exception. "unexpected")))]
           (send "select 1;\n")
           (is (= ["ERROR:  unexpected"
                   "DETAIL:  {\"category\":\"cognitect.anomalies\\/fault\",\"code\":\"xtdb.error\\/unknown\",\"message\":\"unexpected\"}"]
                  (read :stderr))))

         (with-redefs [pgw/cmd-exec-query (fn [& _]  (throw (ex-info "with pg code" {::pgw/severity :error, ::pgw/error-code "XXXXX"})))]
           (send "select 1;\n")
           (is (= ["ERROR:  with pg code"]
                  (read :stderr))))

         (testing "query error allows session to continue"
           (send "select 'ping';\n")
           (is (= [["_column_1"] ["ping"]] (read))))))))

  (deftest psql-error-no-hanging-3930
    (psql-session
     (fn [send read]
       (testing "error query"
         (send "INSERT INTO foo (id, a) VALUES (1, 2);\n")
         (is (= ["ERROR:  Illegal argument: 'missing-id'"
                 "DETAIL:  {\"doc\":{\"id\":1,\"a\":2},\"category\":\"cognitect.anomalies\\/incorrect\",\"code\":\"missing-id\",\"message\":\"Illegal argument: 'missing-id'\"}"]
                (read :stderr)))
         ;; to drain the standard stream
         (read))

       (testing "ping"
         (send "select 'ping';\n")
         (is (= [["_column_1"] ["ping"]] (read))))))))

(deftest map-read-test
  (with-open [conn (jdbc-conn)]
    (xt/execute-tx tu/*node* [[:put-docs :a {:xt/id "map-test", :a {:b 42}}]])

    (let [rs (q conn ["select a.a from a a"])]
      (is (= [{:a {:b 42}}] rs)))))

(deftest open-close-transaction-does-not-crash-test
  (with-open [conn (jdbc-conn)]
    (jdbc/with-transaction [db conn]
      (is (= "ping" (ping db))))))

;; for now, behaviour will change later I am sure
(deftest different-transaction-isolation-levels-accepted-and-ignored-test
  (with-open [conn (jdbc-conn)]
    (doseq [level [:read-committed
                   :read-uncommitted
                   :repeatable-read
                   :serializable]]
      (testing (format "can open and close transaction (%s)" level)
        (jdbc/with-transaction [db conn {:isolation level}]
          (is (= "ping" (ping db)))))
      (testing (format "readonly accepted (%s)" level)
        (jdbc/with-transaction [db conn {:isolation level, :read-only true}]
          (is (= "ping" (ping db)))))
      (testing (format "rollback only accepted (%s)" level)
        (jdbc/with-transaction [db conn {:isolation level, :rollback-only true}]
          (is (= "ping" (ping db))))))))

;; right now all isolation levels have the same defined behaviour
(deftest transaction-by-default-pins-the-snapshot-to-last-tx-test
  (let [insert #(xt/execute-tx tu/*node* [[:put-docs %1 %2]])]
    (insert :a {:xt/id :fred, :name "Fred"})

    (with-open [conn (jdbc-conn)]
      (jdbc/with-transaction [db conn]
        (is (= [{:name "Fred"}] (q db ["select a.name from a"])))
        (insert :a {:xt/id :bob, :name "Bob"})
        (is (= [{:name "Fred"}] (q db ["select a.name from a"]))))

      (is (= #{{:name "Fred"}, {:name "Bob"}} (set (q conn ["select a.name from a"])))))))

(deftest db-queryable-after-transaction-error-test
  (with-open [conn (jdbc-conn)]
    (try
      (jdbc/with-transaction [db conn]
        (is (= [] (q db ["SELECT * WHERE FALSE"])))
        (throw (Exception. "Oh no!")))
      (catch Throwable _))
    (is (= [] (q conn ["SELECT * WHERE FALSE"])))))

(deftest transactions-infer-access-mode-by-default-test
  (with-open [conn (jdbc-conn)]
    (jdbc/with-transaction [db conn]
      (q db ["insert into foo(_id) values (42)"]))
    (is (= [{:xt/id 42}] (q conn ["SELECT * FROM foo"])))))

(defn- session-variables [server-conn ks]
  (-> server-conn :conn-state deref :session (select-keys ks)))

(deftest session-access-mode-default-test
  (with-open [_ (jdbc-conn)]
    (is (= {:access-mode :read-only} (session-variables (get-last-conn) [:access-mode])))))

(defn tx! [conn & sql]
  (jdbc/with-transaction [tx conn]
    (run! #(q tx %) sql)))

(deftest dml-test
  (with-open [conn (jdbc-conn)]
    (testing "mixing a read causes rollback"
      (is (thrown-with-msg? PSQLException #"Queries are unsupported in a DML transaction"
                            (jdbc/with-transaction [tx conn]
                              (q tx ["INSERT INTO foo(_id, a) values(42, 42)"])
                              (q conn ["SELECT a FROM foo"])))))

    (testing "insert it"
      (jdbc/with-transaction [tx conn]
        (q tx ["INSERT INTO foo(_id, a) values(42, 42)"]))
      (testing "read after write"
        (is (= [{:a 42}] (q conn ["SELECT a FROM foo"])))))

    (testing "update it"
      (tx! conn ["UPDATE foo SET a = a + 1 WHERE _id = 42"])
      (is (= [{:a 43}] (q conn ["SELECT a FROM foo"]))))

    (testing "delete it"
      (tx! conn ["DELETE FROM foo WHERE _id = 42"])
      (is (= [] (q conn ["SELECT a FROM foo"]))))))

(when (psql-available?)
  (deftest psql-dml-test
    (psql-session
     (fn [send read]
       (testing "begin"
         (send "BEGIN;\n")
         (is (= [["BEGIN"]] (read))))

       (testing "insert"
         (send "INSERT INTO foo (_id, a) values (42, 42);\n")
         (is (= [["INSERT 0 0"]] (read))))

       (testing "insert 2"
         (send "INSERT INTO foo (_id, a) values (366, 366);\n")
         (is (= [["INSERT 0 0"]] (read))))

       (testing "commit"
         (send "COMMIT;\n")
         (is (= [["COMMIT"]] (read))))

       (testing "read your own writes"
         (send "SELECT a FROM foo;\n")
         (is (= [["a"] ["366"] ["42"]] (read))))

       (testing "delete"
         (send "BEGIN READ WRITE;\n")

         (read)
         (send "DELETE FROM foo;\n")
         (is (= [["DELETE 0"]] (read))))))))

(when (psql-available?)
  (deftest psql-dml-at-prompt-test
    (psql-session
     (fn [send read]
       (send "INSERT INTO foo(_id, a) VALUES (42, 42);\n")
       (is (= [["INSERT 0 0"]] (read)))))))

(deftest dml-param-test
  (with-open [conn (jdbc-conn)]
    (tx! conn ["INSERT INTO foo (_id, a) VALUES (?, ?)" 42 "hello, world"])
    (is (= [{:a "hello, world"}] (q conn ["SELECT a FROM foo where _id = 42"])))))

(deftest test-current-time
  (let [custom-clock (Clock/fixed (Instant/parse "2000-08-16T11:08:03Z") (ZoneId/of "Z"))]
    (swap! (:server-state *server*) assoc :clock custom-clock)

    (with-open [conn (jdbc-conn)]

      (letfn [(current-time [conn]
                (:ct (jdbc/execute-one! conn ["SELECT CURRENT_TIMESTAMP ct"])))]

        (t/is (= (-> #xt/zdt "2000-08-16T12:08:03+01:00" in-system-tz) (current-time conn)))

        (testing "current ts instant is pinned during a tx, regardless of what happens to the session clock"

          (let [{:keys [conn-state]} (get-last-conn)]
            (swap! conn-state assoc-in [:session :clock] (Clock/fixed Instant/EPOCH ZoneOffset/UTC)))

          (jdbc/with-transaction [tx conn]
            (let [epoch #xt/zdt "1970-01-01Z"]
              (t/is (= epoch (current-time tx)))
              (Thread/sleep 10)
              (t/is (= epoch (current-time tx)))

              (testing "inside a transaction the instant is fixed, regardless of the session clock"
                (let [{:keys [conn-state]} (get-last-conn)]
                  (swap! conn-state assoc-in [:session :clock] custom-clock))
                (t/is (= epoch (current-time tx)))))))))))

(deftest test-timezone
  (with-open [conn (jdbc-conn)]
    (letfn [(q-tz [conn]
              (:timezone (jdbc/execute-one! conn ["SHOW TIME ZONE"])))]
      (let [default-tz (str (.getZone (Clock/systemDefaultZone)))]

        (t/testing "expect server timezone"
          (t/is (= default-tz (q-tz conn))))

        (t/testing "utc"
          (jdbc/execute! conn ["SET TIME ZONE '+00:00'"])
          (t/is (= "Z" (q-tz conn))))

        (t/testing "param"
          (jdbc/execute! conn ["SET TIME ZONE ?" "Europe/London"])
          (t/is (= "Europe/London" (q-tz conn))))

        (t/testing "random tz"
          (exec conn "SET TIME ZONE '+07:44'")
          (t/is (= "+07:44" (q-tz conn))))

        (t/testing "tz is session scoped"
          (with-open [conn2 (jdbc-conn)]

            (t/is (= default-tz (q-tz conn2)))
            (t/is (= "+07:44" (q-tz conn)))

            (exec conn2 "SET TIME ZONE '-00:01'")

            (t/is (= "-00:01" (q-tz conn2)))
            (t/is (= "+07:44" (q-tz conn)))))

        (jdbc/with-transaction [tx conn]

          (t/testing "in a transaction, inherits session tz"
            (t/is (= "+07:44" (q-tz tx))))

          (t/testing "tz can be modified in a transaction"
            (exec tx "SET TIME ZONE 'Asia/Tokyo'")
            (t/is (= "Asia/Tokyo" (q-tz tx)))))

        (testing "SET is a session operator, so the tz still applied to the session"
          (t/is (= "Asia/Tokyo" (q-tz conn))))))))

(deftest pg-begin-unsupported-syntax-error-test
  (with-open [conn (jdbc-conn {"autocommit" "false"})]
    (is (thrown-with-msg? PSQLException #"line 1:6 mismatched input 'not'" (q conn ["BEGIN not valid sql!"])))
    (is (thrown-with-msg? PSQLException #"line 1:6 extraneous input 'SERIALIZABLE'" (q conn ["BEGIN SERIALIZABLE"])))))

(deftest begin-with-access-mode-test
  (with-open [conn (jdbc-conn {"autocommit" "false"})]
    (testing "DML enabled with BEGIN READ WRITE"
      (q conn ["BEGIN READ WRITE"])
      (q conn ["INSERT INTO foo (_id) VALUES (42)"])
      (q conn ["COMMIT"])
      (is (= [{:xt/id 42}] (q conn ["SELECT _id from foo"]))))

    (testing "no DML in BEGIN READ ONLY"
      (q conn ["BEGIN READ ONLY"])
      (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (q conn ["INSERT INTO foo (_id) VALUES (43)"])))
      (q conn ["ROLLBACK"]))))

(deftest start-transaction-test
  (with-open [conn (jdbc-conn {"autocommit" "false"})]
    (let [sql #(q conn [%])]

      (sql "START TRANSACTION READ ONLY")
      (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction"
                            (sql "INSERT INTO foo (_id) VALUES (42)")))
      (sql "ROLLBACK")

      (t/testing "we can now infer the transaction access-mode #3624"
        (sql "START TRANSACTION")
        (sql "INSERT INTO foo (_id) VALUES (42)")
        (sql "COMMIT")
        (is (= [{:xt/id 42}] (q conn ["SELECT _id from foo"]))))

      (t/testing "we can't mix modes"
        (t/testing "w -> r"
          (sql "START TRANSACTION")
          (sql "INSERT INTO foo (_id) VALUES (42)")
          (is (thrown-with-msg? PSQLException #"Queries are unsupported in a DML transaction"
                                (sql "SELECT * FROM foo")))
          (sql "ROLLBACK"))

        (t/testing "r -> w"
          (sql "START TRANSACTION")
          (is (= [{:xt/id 42}] (q conn ["SELECT _id from foo"])))
          (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction"
                                (sql "INSERT INTO foo (_id) VALUES (42)")))
          (sql "ROLLBACK")))

      (t/testing "explicit READ WRITE"
        (sql "START TRANSACTION READ WRITE")
        (sql "INSERT INTO foo (_id) VALUES (43)")
        (sql "COMMIT")
        (is (= #{{:xt/id 42} {:xt/id 43}} (set (q conn ["SELECT _id from foo"]))))))))

(deftest set-session-characteristics-test
  (with-open [conn (jdbc-conn {"autocommit" "false"})]
    (let [sql #(q conn [%])]
      (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE")
      (sql "START TRANSACTION")
      (sql "INSERT INTO foo (_id) VALUES (42)")
      (sql "COMMIT")

      (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
      (is (= [{:xt/id 42}] (q conn ["SELECT _id from foo"])))

      (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE")
      (sql "START TRANSACTION")
      (sql "INSERT INTO foo (_id) VALUES (43)")
      (sql "COMMIT")

      (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
      (is (= #{{:xt/id 42}, {:xt/id 43}} (set (q conn ["SELECT _id from foo"])))))))

(deftest set-valid-time-defaults-test
  (with-open [conn (jdbc-conn)]
    (let [sql #(q conn [%])]
      (sql "START TRANSACTION READ WRITE")
      (sql "INSERT INTO foo (_id, version) VALUES ('foo', 0)")
      (sql "COMMIT")

      (is (= [{:version 0,
               :xt/valid-from #xt/zdt "2020-01-01Z[UTC]"}]
             (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

      (sql "START TRANSACTION READ WRITE")
      (sql "UPDATE foo SET version = 1 WHERE _id = 'foo'")
      (sql "COMMIT")

      (is (= [{:version 1,
               :xt/valid-from #xt/zdt "2020-01-02Z[UTC]"}]
             (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

      (is (= [{:version 0,
               :xt/valid-from #xt/zdt "2020-01-01Z[UTC]",
               :xt/valid-to #xt/zdt "2020-01-02Z[UTC]"}
              {:version 1,
               :xt/valid-from #xt/zdt "2020-01-02Z[UTC]"}]
             (q conn ["SETTING DEFAULT VALID_TIME ALL
                       SELECT version, _valid_from, _valid_to FROM foo ORDER BY version"])))

      (is (= [{:version 1,
               :xt/valid-from #xt/zdt "2020-01-02Z[UTC]"}]
             (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

      (sql "START TRANSACTION READ WRITE")
      (sql "UPDATE foo FOR ALL VALID_TIME SET version = 2 WHERE _id = 'foo'")
      (sql "COMMIT")

      (is (= [{:version 2,
               :xt/valid-from #xt/zdt "2020-01-02Z[UTC]"}]
             (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

      (is (= [{:version 2,
               :xt/valid-from #xt/zdt "2020-01-01Z[UTC]",
               :xt/valid-to #xt/zdt "2020-01-02Z[UTC]"}
              {:version 2,
               :xt/valid-from #xt/zdt "2020-01-02Z[UTC]"}]
             (q conn ["SETTING DEFAULT VALID_TIME ALL
                       SELECT version, _valid_from, _valid_to FROM foo"])))

      (is (= [{:version 2,
               :xt/valid-from #xt/zdt "2020-01-02Z[UTC]"}]
             (q conn ["SETTING DEFAULT VALID_TIME AS OF NOW SELECT version, _valid_from, _valid_to FROM foo"]))))))

(t/deftest test-setting-basis-current-time-3505
  (with-open [conn (jdbc-conn)]
    (is (= [{:ts (-> #xt/zdt "2020-01-01Z[UTC]" in-system-tz)}]
           (q conn ["SETTING CLOCK_TIME = '2020-01-01Z'
                     SELECT CURRENT_TIMESTAMP AS ts"])))

    (q conn ["INSERT INTO foo (_id, version) VALUES ('foo', 0)"])

    (is (= [{:version 0, :xt/valid-from #xt/zdt "2020-01-01Z[UTC]"}]
           (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

    (t/is (= {:sql-state "0B000",
              :message "system-time (2024-01-01T00:00:00Z) is after the latest completed tx (#xt/tx-key {:tx-id 0, :system-time #xt/instant \"2020-01-01T00:00:00Z\"})",
              :detail #xt/illegal-arg [:xtdb/unindexed-tx
                                       "system-time (2024-01-01T00:00:00Z) is after the latest completed tx (#xt/tx-key {:tx-id 0, :system-time #xt/instant \"2020-01-01T00:00:00Z\"})"
                                       {:latest-completed-tx #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-01T00:00:00Z"},
                                        :system-time #xt/instant "2024-01-01T00:00:00Z"}]}
             (reading-ex
               (q conn ["SETTING SNAPSHOT_TOKEN = ?
                         SELECT CURRENT_TIMESTAMP FROM foo"
                        (basis/->time-basis-str {"xtdb" [#xt/zdt "2024-01-01Z[UTC]"]})]))))

    (q conn ["UPDATE foo SET version = 1 WHERE _id = 'foo'"])

    (is (= [{:version 1
             :xt/valid-from #xt/zdt "2020-01-02Z[UTC]"}]
           (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

    (is (= [{:version 0
             :xt/valid-from #xt/zdt "2020-01-01Z[UTC]"
             :ts (-> #xt/zdt "2024-01-01Z[UTC]" in-system-tz)}]
           (q conn ["SETTING SNAPSHOT_TOKEN = ?,
                             CLOCK_TIME = '2024-01-01Z'
                     SELECT version, _valid_from, _valid_to, CURRENT_TIMESTAMP ts FROM foo"
                    (basis/->time-basis-str {"xtdb" [#xt/zdt "2020-01-01Z[UTC]"]})]))
        "both snapshot and current time")

    (q conn ["SET TIME ZONE 'UTC'"])

    (is (= [{:version 0
             :xt/valid-from #xt/zdt "2020-01-01Z[UTC]"
             :ts #xt/zdt "2024-01-01Z[UTC]"}]
           (q conn ["SETTING SNAPSHOT_TOKEN = ?, CLOCK_TIME = ?
                     SELECT version, _valid_from, _valid_to, CURRENT_TIMESTAMP ts FROM foo"
                    (basis/->time-basis-str {"xtdb" [#xt/zdt "2020-01-01Z[UTC]"]})
                    #xt/zdt "2024-01-01Z[UTC]"]))
        "both snapshot and current time, params")

    (is (= [{:version 0
             :xt/valid-from #xt/zdt "2020-01-01Z[UTC]"
             :ts #xt/zdt "2024-01-01Z[UTC]"}]
           (q conn ["SETTING SNAPSHOT_TOKEN = ?, CLOCK_TIME = ?
                     SELECT version, _valid_from, _valid_to, CURRENT_TIMESTAMP ts FROM foo WHERE _id = ?"
                    (basis/->time-basis-str {"xtdb" [#xt/zdt "2020-01-01Z[UTC]"]})
                    #xt/zdt "2024-01-01Z[UTC]", "foo"]))
        "gets correct param order")

    (q conn ["UPDATE foo SET version = 2 WHERE _id = 'foo'"])

    (is (= [{:version 2}]
           (q conn ["SELECT version FROM foo"])))

    (t/testing "for system-time cannot override snapshot"
      (exec conn "SET TIME ZONE 'UTC'")
      (is (= [{:version 0}]
             (q conn ["SETTING SNAPSHOT_TOKEN = ?
                       SELECT version FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '2020-01-02Z'"
                      (basis/->time-basis-str {"xtdb" [#xt/zdt "2020-01-01Z[UTC]"]})]))
          "timestamp-tz"))

    (is (= [{:version 1}]
           (q conn ["SELECT version FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '2020-01-02Z'"]))
        "version would have been 1 if snapshot was not set")))

(t/deftest test-setting-import-system-time-3616
  (with-open [conn (jdbc-conn)]
    (let [sql #(q conn [%])]
      (t/testing "as part of START TRANSACTION"
        (sql "SET TIME ZONE 'Europe/London'")
        (sql "START TRANSACTION READ WRITE WITH (SYSTEM_TIME DATE '2021-08-01')")
        (sql "INSERT INTO foo (_id, version) VALUES ('foo', 0)")
        (sql "COMMIT")

        (is (= [{:version 0,
                 :xt/system-from #xt/zdt "2021-07-31T23:00:00Z[UTC]"}]
               (q conn ["SELECT version, _system_from FROM foo"]))))

      (t/testing "with BEGIN"
        (sql "BEGIN READ WRITE WITH (SYSTEM_TIME = '2021-08-03T00:00:00')")
        (sql "INSERT INTO foo (_id, version) VALUES ('foo', 1)")
        (sql "COMMIT")

        (is (= [{:version 0,
                 :xt/system-from #xt/zdt "2021-07-31T23:00:00Z[UTC]"}
                {:version 1,
                 :xt/system-from #xt/zdt "2021-08-02T23:00:00Z[UTC]"}]
               (q conn ["SELECT version, _system_from FROM foo FOR ALL VALID_TIME ORDER BY version"]))))

      (t/testing "past system time"
        (q conn ["BEGIN READ WRITE WITH (SYSTEM_TIME ?)" "2021-08-02Z"])
        (sql "INSERT INTO foo (_id, version) VALUES ('foo', 2)")
        (t/is (thrown-with-msg? PSQLException #"specified system-time older than current tx"
                                (sql "COMMIT")))))))

;; this demonstrates that session / set variables do not change the next statement
;; its undefined - but we can say what it is _not_.
(deftest implicit-transaction-stop-gap-test
  (with-open [conn (jdbc-conn {"autocommit" "false"})]
    (let [sql #(q conn [%])]

      (testing "read only"
        (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        (sql "BEGIN")
        (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (sql "INSERT INTO foo (_id) values (43)")))
        (sql "ROLLBACK")
        (is (= [] (sql "select * from foo"))))

      (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE")

      (testing "session access mode inherited"
        (sql "BEGIN")
        (sql "INSERT INTO foo (_id) values (42)")
        (sql "COMMIT")
        (testing "despite read write setting read remains available outside tx"
          (is (= [{:xt/id 42}] (sql "select _id from foo")))))

      (testing "override session to start a read only transaction"
        (sql "BEGIN READ ONLY")
        (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (sql "INSERT INTO foo (_id) values (43)")))
        (sql "ROLLBACK")
        (testing "despite read write setting read remains available outside tx"
          (is (= [{:xt/id 42}] (sql "select _id from foo"))))))))

(deftest analyzer-error-returned-test
  (testing "Query"
    (with-open [conn (jdbc-conn)]
      (is (thrown-with-msg? PSQLException #"mismatched input 'SLECT' expecting" (q conn ["SLECT 1 FROM foo"])))))

  (testing "DML"
    (with-open [conn (jdbc-conn)]
      (q conn ["BEGIN READ WRITE"])
      (q conn ["INSERT INTO foo (a) values (42)"])
      (is (thrown-with-msg? PSQLException #"missing-id" (q conn ["COMMIT"]))))))

(when (psql-available?)
  (deftest psql-analyzer-error-test
    (psql-session
     (fn [send read]
       (send "SLECT 1 FROM foo;\n")
       (let [s (read :stderr)]
         (is (not= :timeout s))
         (is (str/includes? (->> s (map str/join) (str/join "\n"))
                            "line 1:0 mismatched input 'SLECT' expecting")))

       (send "BEGIN READ WRITE;\n")
       (read)

       ;; no id
       (send "INSERT INTO foo (x) values (42);\n")
       (read)

       (send "COMMIT;\n")
       (let [error (read :stderr)]
         (is (not= :timeout error))
         (is (= "ERROR:  Illegal argument: 'missing-id'"
                (first error))))

       (send "ROLLBACK;\n")
       (let [s (read)]
         (is (not= :timeout s))
         (is (= [["ROLLBACK"]] s)))))))

(deftest runtime-error-query-test
  (logging/with-log-level 'xtdb.pgwire :off
    (with-open [conn (jdbc-conn)]
      (is (thrown-with-msg? PSQLException #"Data Exception - trim error."
                            (q conn ["SELECT TRIM(LEADING 'abc' FROM a.a) FROM (VALUES ('')) a (a)"]))))))

(deftest runtime-error-commit-test
  (t/testing "in tx"
    (with-open [conn (jdbc-conn)]
      (q conn ["START TRANSACTION READ WRITE"])
      (q conn ["INSERT INTO foo (_id) VALUES (TRIM(LEADING 'abc' FROM ''))"])
      (t/is (thrown-with-msg? PSQLException #"Data Exception - trim error." (q conn ["COMMIT"])))))

  (t/testing "autocommit #3563"
    (with-open [conn (jdbc-conn)]
      (t/is (thrown-with-msg? PSQLException #"Data Exception - trim error."
                              (q conn ["INSERT INTO foo (_id) VALUES (TRIM(LEADING 'abc' FROM ''))"])))

      (t/is (thrown-with-msg? PSQLException #"ERROR: Parameter error: 0 provided, 2 expected"
                              (q conn ["INSERT INTO tbl1 (_id, foo) VALUES ($1, $2)"]))))))

(deftest test-column-order
  (with-open [conn (jdbc-conn)]

    (let [sql #(q-seq conn [%])]
      (q conn ["INSERT INTO foo(_id, col0, col1) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')"])

      (is (= [[20 "b" 2] [10 "a" 1] [30 "c" 3]]
             (sql "SELECT col0, col1, _id FROM foo")))

      (is (= [[2 20 "b"] [1 10 "a"] [3 30 "c"]]
             (sql "SELECT * FROM foo")))

      (t/is (= [[1 2 3 4 5 6 7 8 9]]
               (sql "SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9"))
            "Column order is preserved with over 8 columns, see: clojure map implicit ordering"))))

;; https://github.com/pgjdbc/pgjdbc/blob/8afde800bce64e9b22a7da10ca6c515017cf7db1/pgjdbc/src/main/java/org/postgresql/jdbc/PgConnection.java#L403
;; List of types/oids that support binary format

(deftest test-postgres-types
  (with-open [conn (jdbc-conn {"prepareThreshold" 1 "binaryTransfer" false})]
    (q-seq conn ["INSERT INTO foo(_id, int8, int4, int2, float8 , var_char, bool) VALUES (?, ?, ?, ?, ?, ?, ?)"
                 #uuid "9e8b41a0-723f-4e6b-babb-c4e6afd17ef2" Long/MIN_VALUE Integer/MAX_VALUE Short/MIN_VALUE Double/MAX_VALUE "aa" true]))
  ;; no float4 for text format due to a bug in pgjdbc where it sends it as a float8 causing a union type.
  (with-open [conn (jdbc-conn {"prepareThreshold" 1 "binaryTransfer" true})]
    (q-seq conn ["INSERT INTO foo(_id, int8, int4, int2, float8, float4, var_char, bool) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                 #uuid "7dd2ed62-bb05-43c8-b289-5503d9b19ee6" Long/MAX_VALUE Integer/MIN_VALUE Short/MAX_VALUE Double/MIN_VALUE Float/MAX_VALUE "bb" false])
    ;;binary format is only requested for server side preparedStatements in pgjdbc
    ;;threshold one should mean we test the text and binary format for each type
    (let [sql #(q conn [%])
          res [{:float8 Double/MIN_VALUE,
                :float4 Float/MAX_VALUE,
                :int2 Short/MAX_VALUE,
                :var-char "bb",
                :int4 Integer/MIN_VALUE,
                :int8 Long/MAX_VALUE,
                :xt/id #uuid "7dd2ed62-bb05-43c8-b289-5503d9b19ee6",
                :bool false}
               {:float8 Double/MAX_VALUE,
                :int2 Short/MIN_VALUE,
                :var-char "aa",
                :int4 Integer/MAX_VALUE,
                :int8 Long/MIN_VALUE,
                :xt/id #uuid "9e8b41a0-723f-4e6b-babb-c4e6afd17ef2",
                :bool true}]]


      (is (=
           res
           (sql "SELECT * FROM foo"))
          "text")

      (is (=
           res
           (sql "SELECT * FROM foo"))
          "binary"))))

(defn- as-json-param [v]
  (-> (constantly v)
      (with-meta {'next.jdbc.prepare/set-parameter
                  (fn [v, ^PreparedStatement stmt, ^long idx]
                    (.setObject stmt idx (doto (PGobject.)
                                           (.setType "json")
                                           (.setValue (JsonSerde/encode (v))))))})))

(deftest test-json-type
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO foo (_id, json, scalar) VALUES (0, ?, ?)"
                         (as-json-param {:a 1 :b 2})
                         (as-json-param 42)])

    (jdbc/execute! conn ["INSERT INTO foo (_id, json) VALUES (1, ?)"
                         (as-json-param {:a 2, :c {:d 3}})])

    (t/is (= [{:xt/id 1, :json {:a 2, :c {:d 3}}}
              {:xt/id 0, :json {:a 1, :b 2}, :scalar 42}]
             (q conn ["SELECT * FROM foo"])))

    (t/is (= [{:xt/id 1, :a 2, :c {:d 3}, :d 3}
              {:xt/id 0, :a 1, :b 2}]
             (q conn ["SELECT _id, (json).a, (json).b, (json).c, (json).c.d FROM foo"])))

    (t/is (= [{:json {:a 2, :c {:d 3}}}]
             (q conn ["SELECT ? json"
                      (as-json-param {:a 2, :c {:d 3}})])))))

(deftest test-odbc-queries
  (with-open [conn (jdbc-conn)]

    ;; ODBC issues this query by default
    (is (= []
           (q-seq conn ["select oid, typbasetype from pg_type where typname = 'lo'"])))))

(t/deftest test-pg-port
  (util/with-open [node (xtn/start-node {::pgw/server {}})]
    (binding [*port* (.getServerPort node)]
      (with-open [conn (jdbc-conn)]
        (t/is (= "ping" (ping conn)))))))

(t/deftest test-assert-3445
  (with-open [conn (jdbc-conn)]
    (jdbc/with-transaction [tx conn]
      (jdbc/execute! tx ["INSERT INTO foo (_id) VALUES (1)"]))

    (jdbc/with-transaction [tx conn]
      (jdbc/execute! tx ["ASSERT 1 = (SELECT COUNT(*) FROM foo)"])
      (jdbc/execute! tx ["INSERT INTO foo (_id) VALUES (2)"]))

    (t/is (thrown-with-msg? Exception
                            #"ERROR: Assert failed"
                            (jdbc/with-transaction [tx conn]
                              (jdbc/execute! tx ["ASSERT 1 = (SELECT COUNT(*) FROM foo)"])
                              (jdbc/execute! tx ["INSERT INTO foo (_id) VALUES (2)"]))))

    (t/is (= [{:row-count 2}]
             (q conn ["SELECT COUNT(*) row_count FROM foo"])))

    (t/is (= [{:xt/id 2, :committed false,
               :error #xt/error [:conflict :xtdb/assert-failed "Assert failed"
                                 {:arg-idx 0, :sql "ASSERT 1 = (SELECT COUNT(*) FROM foo)", :tx-op-idx 0
                                  :tx-key #xt/tx-key {:tx-id 2, :system-time #xt/instant "2020-01-03T00:00:00Z"}}]}
              {:xt/id 1, :committed true}
              {:xt/id 0, :committed true}]
             (q conn ["SELECT * EXCLUDE system_time FROM xt.txs"])))))

(deftest test-untyped-null-type
  (with-open [conn (jdbc-conn {"prepareThreshold" -1})
              stmt (.prepareStatement conn "SELECT ?")]

    (t/testing "untyped null param"

      (.setObject stmt 1 nil)

      (t/is (= ["text"] (param-metadata stmt)))

      (with-open [rs (.executeQuery stmt)]

        (t/is (= [{"_column_1" "text"}] (result-metadata stmt) (result-metadata rs))
              "untyped nulls are of type text in result sets")
        (t/is (= [{"_column_1" nil}] (rs->maps rs)))))

    (t/testing "updating param to non null value"

      (.setObject stmt 1 4)

      (t/is (= ["int8"] (param-metadata stmt)))

      (with-open [rs (.executeQuery stmt)]
        (t/is (= [{"_column_1" "int8"}] (result-metadata stmt) (result-metadata rs)))
        (t/is (= [{"_column_1" 4}] (rs->maps rs)))))))

(deftest test-typed-null-type
  (with-open [conn (jdbc-conn {"prepareThreshold" -1})
              stmt (.prepareStatement conn "SELECT ?")]

    (t/is (= ["text"] (param-metadata stmt)))

    (t/testing "typed null param"

      (.setObject stmt 1 nil Types/INTEGER)

      (t/is (= ["int4"] (param-metadata stmt)))
      (t/is (= [{"_column_1" "int4"}] (result-metadata stmt)))

      (with-open [rs (.executeQuery stmt)]
        (t/is (= [{"_column_1" "int4"}] (result-metadata stmt) (result-metadata rs)))
        (t/is (= [{:_column_1 nil}] (resultset-seq rs)))))))

(deftest test-prepared-statements-with-unknown-params
  (with-open [conn (jdbc-conn {"prepareThreshold" -1})
              stmt (.prepareStatement conn "SELECT ?")]

    (t/testing "unknown param types are assumed to be of type text"

      (t/is (= ["text"] (param-metadata stmt))))

    (t/testing "Once a param value is supplied statement updates correctly and can be executed"

      (.setObject stmt 1 #uuid "7dd2ed62-bb05-43c8-b289-5503d9b19ee6")

      (t/is (= ["uuid"] (param-metadata stmt)))

      (with-open [rs (.executeQuery stmt)]

        (t/is (= [{"_column_1" "uuid"}] (result-metadata stmt) (result-metadata rs)))
        (t/is (= [{"_column_1" #uuid "7dd2ed62-bb05-43c8-b289-5503d9b19ee6"}] (rs->maps rs)))))))

(t/deftest test-transit-params-4455
  (with-open [conn (jdbc-conn {"prepareThreshold" 0})]
    (with-open [stmt (.prepareStatement conn "SELECT ? vec")]
      (.setObject stmt 1 [{:a 1, :b 2}, {:a 3, :b 4}])
      (t/is (= ["transit"] (param-metadata stmt)))
      (t/is (= [{"vec" "transit"}] (result-metadata stmt)))

      (t/is (= [{:vec [{"a" 1, "b" 2} {"a" 3, "b" 4}]}]
               (vec (resultset-seq (.executeQuery stmt)))))

      (.setObject stmt 1 [{:a "foo"} {:a "bar"}])
      (t/is (= [{:vec [{"a" "foo"} {"a" "bar"}]}]
               (vec (resultset-seq (.executeQuery stmt))))
            "re-run with different type for `a` (although overall param still transit)"))

    (t/is (= [{:vec [{:a 1, :b 2} {:a 3, :b 4}]}]
             (jdbc/execute! conn ["SELECT ? vec" [{:a 1, :b 2} {:a 3, :b 4}]]
                            {:builder-fn xt-jdbc/builder-fn}))
          "transit param in query")

    (t/is (= [{:a 1}]
             (jdbc/execute! conn ["SELECT (?[1]).a a" [{:a 1, :b 2} {:a 3, :b 4}]]
                            {:builder-fn xt-jdbc/builder-fn}))
          "transit param in query")

    (t/is (= [{:a "foo"}]
             (jdbc/execute! conn ["SELECT (?[1]).a a" [{:a "foo"} {:a "bar"}]]
                            {:builder-fn xt-jdbc/builder-fn}))
          "re-run with different type (still transit though)")))

(deftest test-prepared-statments
  (with-open [conn (jdbc-conn {"prepareThreshold" -1})]
    (.execute (.prepareStatement conn "INSERT INTO foo(_id, a, b) VALUES (1, 'one', 2)"))
    (with-open [stmt (.prepareStatement conn "SELECT foo.*, ? FROM foo")]
      (t/testing "server side prepared statments where param types are known"
        (.setBoolean stmt 1 true)

        (with-open [rs (.executeQuery stmt)]
          (t/is (= [{"_id" "int8"} {"a" "text"} {"b" "int8"} {"_column_2" "bool"}]
                   (result-metadata stmt)
                   (result-metadata rs)))

          (t/is (= [{:_id 1, :a "one", :b 2, :_column_2 true}]
                   (resultset-seq rs)))))

      (t/testing "param types can be rebound for the same prepared statment"
        (.setDouble stmt 1 44.4)

        (with-open [rs (.executeQuery stmt)]
          (t/is (= [{"_id" "int8"} {"a" "text"} {"b" "int8"} {"_column_2" "float8"}]
                   (result-metadata stmt)
                   (result-metadata rs)))

          (t/is (= [{:_id 1, :a "one", :b 2, :_column_2 44.4}]
                   (resultset-seq rs)))))

      (t/testing "relevant schema change reported to pgwire client"

        (t/testing "outside of a transaction, we return an error that pgwire will retry"
          (jdbc/execute! conn ["INSERT INTO foo(_id, a, b) VALUES (2, 1, 1)"])

          (t/is (= [{:_id 2, :a 1, :b 1, :_column_2 44.4}
                    {:_id 1, :a "one", :b 2, :_column_2 44.4}]
                   (jdbc/execute! stmt))))

        (t/testing "pgjdbc won't re-prep during a transaction"
          (jdbc/execute! conn ["INSERT INTO foo(_id, a, b) VALUES (3, 1, '1')"])

          (jdbc/execute! conn ["BEGIN"])

          (try
            (t/is (= {:sql-state "0A000",
                      :message "cached plan must not change result type",
                      :routine "RevalidateCachedQuery"}
                     (-> (reading-ex (jdbc/execute! stmt))
                         (dissoc :detail))))
            (finally
              (jdbc/execute! conn ["ROLLBACK"]))))))))

(deftest test-nulls-in-monomorphic-types
  (with-open [conn (jdbc-conn {"prepareThreshold" -1})]
    (.execute (.prepareStatement conn "INSERT INTO foo(_id, a) VALUES (1, NULL), (2, 22.2)"))
    (with-open [stmt (.prepareStatement conn "SELECT a FROM foo ORDER BY _id")]

      (with-open [rs (.executeQuery stmt)]

        (t/is (= [{"a" "float8"}]
                 (result-metadata stmt)
                 (result-metadata rs)))

        (t/is (= [{"a" nil} {"a" 22.2}]
                 (rs->maps rs)))))))

(deftest test-nulls-in-polymorphic-types
  (with-open [conn (jdbc-conn {"prepareThreshold" -1})]
    (.execute (.prepareStatement conn "INSERT INTO foo(_id, a) VALUES (1, 'one'), (2, 1), (3, NULL)"))
    (with-open [stmt (.prepareStatement conn "SELECT a FROM foo ORDER BY _id")
                rs (.executeQuery stmt)]
      (t/is (= [{"a" "transit"}]
               (result-metadata stmt)
               (result-metadata rs)))

      (let [[x y z :as res] (mapv #(get % "a") (rs->maps rs))]
        (t/is (= 3 (count res)))

        (t/is (= "one" x))
        (t/is (= 1 y))
        (t/is (nil? z))))))

(deftest test-datetime-types
  (doseq [{:keys [^Class type val pg-type]} [{:type LocalDate :val #xt/date "2018-07-25" :pg-type "date"}
                                             {:type LocalDate :val #xt/date "1239-01-24" :pg-type "date"}
                                             {:type LocalDateTime :val #xt/date-time "2024-07-03T19:01:34.123456" :pg-type "timestamp"}
                                             {:type LocalDateTime :val #xt/date-time "1742-07-03T04:22:59" :pg-type "timestamp"}]
          binary? [true false]]

    (t/testing (format "binary?: %s, type: %s, pg-type: %s, val: %s" binary? type pg-type val)

      (with-open [conn (jdbc-conn {"prepareThreshold" -1 "binaryTransfer" binary?})
                  stmt (.prepareStatement conn "SELECT ? AS val")]

        (.execute (.createStatement conn) "SET TIME ZONE '+05:00'")

        (.setObject stmt 1 val)

        (with-open [rs (.executeQuery stmt)]

          (t/is (= [{"val" pg-type}]
                   (result-metadata stmt)
                   (result-metadata rs)))

          (.next rs)
          (t/is (= val (.getObject rs 1 type))))))))

(deftest test-timestamptz
  (let [type OffsetDateTime
        pg-type "timestamptz"
        val #xt/offset-date-time "2024-07-03T19:01:34-07:00"
        val2 #xt/offset-date-time "2024-07-03T19:01:34.695959+03:00"]
    (doseq [binary? [true false]]

      (with-open [conn (jdbc-conn {"prepareThreshold" -1 "binaryTransfer" binary?})
                  stmt (.prepareStatement conn "SELECT val FROM (VALUES ?, ?, TIMESTAMP '2099-04-15T20:40:31[Asia/Tokyo]') AS foo(val)")]

        (.execute (.createStatement conn) "SET TIME ZONE '+04:00'")

        (.setObject stmt 1 val)
        (.setObject stmt 2 val2)

        (with-open [rs (.executeQuery stmt)]

          (t/is (= [{"val" pg-type}]
                   (result-metadata stmt)
                   (result-metadata rs)))

          (.next rs)
          (t/is (.isEqual val (.getObject rs 1 type)))
          (.next rs)
          (t/is (.isEqual val2 (.getObject rs 1 type)))
          (.next rs)
          (t/is (.isEqual
                 #xt/offset-date-time "2099-04-15T11:40:31Z"
                 (.getObject rs 1 type))
                "ZonedDateTimes can be read even if not written"))))))

(deftest test-tstz-range-3652
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 1, v: 1}"])
    (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 1, v: 2}"])

    (t/is (= [{:xt/id 1, :v 2,
               :xt/valid-time #xt/tstz-range [#xt/zdt "2020-01-02T00:00Z" nil]}
              {:xt/id 1, :v 1,
               :xt/valid-time #xt/tstz-range [#xt/zdt "2020-01-01T00:00Z" #xt/zdt "2020-01-02T00:00Z"]}]
             (->> (jdbc/execute! conn ["SELECT *, _valid_time FROM foo FOR ALL VALID_TIME"]
                                 {:builder-fn xt-jdbc/builder-fn})))))

  (when (psql-available?)
    (psql-session
     (fn [send read]
       (send "INSERT INTO bar RECORDS {_id: 1, v: 1};\n")
       (read)
       (send "INSERT INTO bar RECORDS {_id: 1, v: 2};\n")
       (read)

       (send "SELECT *, _valid_time FROM bar FOR ALL VALID_TIME;\n")

       (t/is (= [["_id" "v" "_valid_time"]
                 ["1" "2" "[2020-01-04 00:00:00+00:00,)"]
                 ["1" "1" "[2020-01-03 00:00:00+00:00,2020-01-04 00:00:00+00:00)"]]
                (read)))))))

(when (psql-available?)
  (deftest test-datetime-formatting
    ;; no way to set current-time yet, so setting it via custom clock
    (let [custom-clock (Clock/fixed (Instant/parse "2022-08-16T11:08:03.123456789Z") (ZoneOffset/ofHoursMinutes 4 44))]

      (swap! (:server-state *server*) assoc :clock custom-clock)

      (psql-session
       (fn [send read]

         (t/testing "timestamps are correctly output in text format"
           ;;note nanosecond timestamp is truncated to microsecond
           (send "SET TIME ZONE '+03:21';\n")
           (read)
           (send "SHOW timezone;\n")
           (t/is (= [["timezone"] ["+03:21"]] (read)))

           (send "SELECT
                    TIMESTAMP '3000-04-15T20:40:31+01:00[Europe/London]' zdt,
                    CURRENT_DATE cd,
                    CURRENT_TIMESTAMP cts, CURRENT_TIMESTAMP(4) cts4,
                    LOCALTIMESTAMP lts, LOCALTIMESTAMP(9) lts9;\n")

           (t/is (=
                  [["zdt" "cd" "cts" "cts4" "lts" "lts9"]
                   ["3000-04-15 20:40:31+01:00"
                    "2022-08-16"
                    "2022-08-16 14:29:03.123456+03:21"
                    "2022-08-16 14:29:03.1234+03:21"
                    "2022-08-16 14:29:03.123456"
                    "2022-08-16 14:29:03.123456"]]
                  (read)))

           (send "SET TIME ZONE 'GMT';\n")
           (read)
           (send "SHOW timezone;\n")
           (t/is (= [["timezone"] ["GMT"]] (read))))

         (send "SELECT
                    TIMESTAMP '3000-04-15T20:40:31+01:00[Europe/London]' zdt,
                    CURRENT_DATE cd,
                    CURRENT_TIMESTAMP cts, CURRENT_TIMESTAMP(4) cts4,
                    LOCALTIMESTAMP lts, LOCALTIMESTAMP(9) lts9;\n")

         (t/is (=
                [["zdt" "cd" "cts" "cts4" "lts" "lts9"]
                 ["3000-04-15 20:40:31+01:00"
                  "2022-08-16"
                  "2022-08-16 11:08:03.123456+00:00"
                  "2022-08-16 11:08:03.1234+00:00"
                  "2022-08-16 11:08:03.123456"
                  "2022-08-16 11:08:03.123456"]]
                (read))))))))

(deftest test-prepare-select
  (with-open [conn (jdbc-conn {"prepareThreshold" -1})]
    ;; real Postgres does this too - it's like it defaults the type of $1 on `PREPARE`
    ;; so we need the cast
    (jdbc/execute! conn ["PREPARE foo AS SELECT $1::bigint forty_two"])
    (t/is (= {:forty_two 42}
             (jdbc/execute-one! conn ["EXECUTE foo (42)"])))

    (t/is (= {:forty_two 42}
             (jdbc/execute-one! conn ["EXECUTE foo (?)" 42]))))

  (when (psql-available?)
    (psql-session
     (fn [send read]
       (send "PREPARE foo AS SELECT $1::bigint forty_two;\n")
       (read)
       (send "EXECUTE foo (42);\n")
       (t/is (= [["forty_two"] ["42"]] (read)))))))

(t/deftest test-prepare-insert
  (with-open [conn (jdbc-conn {"prepareThreshold" -1})]
    (jdbc/execute! conn ["PREPARE foo AS INSERT INTO foo (_id, a) VALUES ($1, $2)"])
    (jdbc/execute! conn ["EXECUTE foo (1, 'one')"])
    (jdbc/execute! conn ["EXECUTE foo (?, ?)" 2 "two"])

    (t/is (= [{:xt/id 1, :a "one"}, {:xt/id 2, :a "two"}]
             (q conn ["SELECT * FROM foo ORDER BY _id"]))))

  (when (psql-available?)
    (psql-session
     (fn [send read]
       (send "PREPARE foo AS INSERT INTO foo (_id, a) VALUES ($1, $2);\n")
       (t/is (= [["PREPARE"]] (read)))
       (send "EXECUTE foo (3, 'three');\n")
       (t/is (= [["INSERT 0 0"]] (read)))
       (send "SELECT * FROM foo ORDER BY _id;\n")
       (t/is (= [["_id" "a"] ["1" "one"] ["2" "two"] ["3" "three"]]
                (read)))))))

(t/deftest test-combines-dml
  (with-open [conn (jdbc-conn)]
    (jdbc/with-transaction [tx conn]
      (jdbc/execute! tx ["INSERT INTO foo RECORDS ?" {:xt/id 1, :a "one"}])
      (jdbc/execute! tx ["INSERT INTO foo RECORDS ?" {:xt/id 2, :a "two"}])
      (jdbc/execute! tx ["INSERT INTO foo RECORDS {_id: ?, a: ?}" 3, "three"])
      (jdbc/execute! tx ["INSERT INTO foo RECORDS ?" {:xt/id 4, :a "four"}])

      ;; HACK.
      (t/is (= [[:sql "INSERT INTO foo RECORDS $1"
                 [{:_id 1, :a "one"}]
                 [{:_id 2, :a "two"}]]
                [:sql "INSERT INTO foo RECORDS {_id: $1, a: $2}" [3 "three"]]
                [:sql "INSERT INTO foo RECORDS $1" [{:_id 4, :a "four"}]]]
               (-> @(:server-state *server*)
                   (get-in [:connections 1 :conn-state])
                   deref
                   (get-in [:transaction :dml-buf])))))

    (t/is (= [{:xt/id 1, :a "one"}
              {:xt/id 2, :a "two"}
              {:xt/id 3, :a "three"}
              {:xt/id 4, :a "four"}]
             (q conn ["SELECT * FROM foo ORDER BY _id"])))))

(deftest test-java-sql-timestamp
  (testing "java.sql.Timestamp"
    (with-open [conn (jdbc-conn {"prepareThreshold" -1})
                stmt (.prepareStatement conn "SELECT ? AS v")]

      (.setTimestamp
       stmt
       1
       (Timestamp/from #xt/instant "2030-01-04T12:44:55Z")
       (Calendar/getInstance (TimeZone/getTimeZone "GMT")))

      (with-open [rs (.executeQuery stmt)]

        ;;treated as text because pgjdbc sets the param oid to 0/unspecified
        ;;will error in DML
        ;;https://github.com/pgjdbc/pgjdbc/blob/84bdec6015472f309de7becf41df7fd8e423d0ac/pgjdbc/src/main/java/org/postgresql/jdbc/PgPreparedStatement.java#L1454
        (t/is (= [{"v" "text"}]
                 (result-metadata stmt)
                 (result-metadata rs)))

        (t/is (= [{"v" "2030-01-04 12:44:55+00"}] (rs->maps rs)))))))

(deftest test-declared-param-types-are-honored
  (t/testing "Declared param type VARCHAR is honored, even though it maps to utf8 which XTDB maps to text"
    ;;case applies anytime we have multiple pg types that map to the same xt type, as we choose a single pg type
    ;;for the return type.
    (with-open [conn (jdbc-conn {"prepareThreshold" -1})
                stmt (.prepareStatement conn "SELECT ? x, ? y")]

      (.setString stmt 1 "foo")
      (.setObject stmt 2 "bar" Types/OTHER)

      (t/is (= ["varchar" "text"]
               (param-metadata stmt)))

      (t/is (= [{"x" "text"} {"y" "text"}]
               (result-metadata stmt)))

      (t/is (=
             [{"x" "foo", "y" "bar"}]
             (rs->maps (.executeQuery stmt)))))))

(deftest test-show-await-token
  (with-open [conn (jdbc-conn)]
    (t/is (= [{}] (q conn ["SHOW AWAIT_TOKEN"])))

    (jdbc/execute! conn ["INSERT INTO foo (_id) VALUES (1)"])

    (t/is (= [{:await-token (basis/->tx-basis-str {"xtdb" [0]})}]
             (q conn ["SHOW AWAIT_TOKEN"])))

    (t/is (= [{:db-name "xtdb", :part 0, :tx-id 0, :system-time #xt/zdt "2020-01-01Z[UTC]"}]
             (q conn ["SHOW LATEST_COMPLETED_TXS"])))

    (jdbc/execute! conn ["INSERT INTO foo (_id) VALUES (2)"])

    (t/is (= [{:await-token (basis/->tx-basis-str {"xtdb" [1]})}]
             (q conn ["SHOW AWAIT_TOKEN"])))

    (jdbc/execute! conn ["SET AWAIT_TOKEN = ?" (basis/->tx-basis-str {"xtdb" [0]})])

    (t/is (= [{:await-token (basis/->tx-basis-str {"xtdb" [0]})}]
             (q conn ["SHOW AWAIT_TOKEN"])))

    (t/is (= [{:db-name "xtdb", :part 0, :tx-id 1, :system-time #xt/zdt "2020-01-02Z[UTC]"}]
             (q conn ["SHOW LATEST_COMPLETED_TXS"])))

    (jdbc/execute! conn ["INSERT INTO foo (_id) VALUES (2)"])

    (t/is (= [{:await-token (basis/->tx-basis-str {"xtdb" [2]})}]
             (q conn ["SHOW AWAIT_TOKEN"])))))

(t/deftest show-await-token-param-fail-4504
  (with-open [conn (jdbc-conn {"prepareThreshold" -1})]
    (jdbc/execute! conn ["INSERT INTO foo (_id) VALUES (1)"])

    (t/is (= [{:await-token (basis/->tx-basis-str {"xtdb" [0]})}]
             (q conn ["SHOW AWAIT_TOKEN"]))))

  (when (psql-available?)
    (psql-session
     (fn [send read]
       (send "INSERT INTO foo (_id) VALUES (1);\n")
       (read)
       (send "SHOW AWAIT_TOKEN;\n")
       (t/is (= [["await_token"] [(basis/->tx-basis-str {"xtdb" [1]})]] (read)))))))

(t/deftest test-show-latest-submitted-tx
  (with-open [conn (jdbc-conn)]
    (t/is (= [] (q conn ["SHOW LATEST_SUBMITTED_TX"])))

    (jdbc/execute! conn ["INSERT INTO users RECORDS ?" {:xt/id "jms", :given-name "James"}])

    (t/is (= [{:tx-id 0, :system-time #xt/zdt "2020-01-01T00:00Z[UTC]", :committed true,
               :await-token (basis/->tx-basis-str {"xtdb" [0]})}]
             (q conn ["SHOW LATEST_SUBMITTED_TX"])))

    (t/is (= [{:db-name "xtdb", :part 0, :msg-id 0}]
             (q conn ["SHOW LATEST_SUBMITTED_MSG_IDS"])))

    (t/is (thrown? PSQLException (jdbc/execute! conn ["ASSERT FALSE"])))

    (t/is (= [{:tx-id 1, :system-time #xt/zdt "2020-01-02T00:00Z[UTC]", :committed false,
               :error #xt/error [:conflict :xtdb/assert-failed "Assert failed"
                                 {:arg-idx 0, :sql "ASSERT FALSE", :tx-op-idx 0,
                                  :tx-key #xt/tx-key {:tx-id 1, :system-time #xt/instant "2020-01-02T00:00:00Z"}}]
               :await-token (basis/->tx-basis-str {"xtdb" [1]})}]
             (q conn ["SHOW LATEST_SUBMITTED_TX"])))))

(t/deftest test-show-session-variable-3804
  (with-open [conn (jdbc-conn)]
    (t/is (= [{:datestyle "iso8601"}]
             (q conn ["SHOW DateStyle"])))

    (t/is (= [{:intervalstyle "ISO_8601"}]
             (q conn ["SHOW IntervalStyle"])))

    (t/is (= [{:search-path "public"}]
             (q conn ["SHOW search_path"]))
          "#3782")))

(t/deftest test-psql-bind-3572
  #_ ; FIXME #3622
  (when (psql-available?)
    (psql-session
     (fn [send read]
       (send "INSERT INTO tbl1 (_id, foo) VALUES ($1, $2) \\bind 'a' 'b' \\g")
       (read)
       (send "SELECT * FROM tbl1 WHERE _id = $1 \\bind 'a' \\g")
       (t/is (= [["_id" "foo"] ["a" "b"]] (read)))))))

(deftest test-resolve-result-format
  (letfn [(resolve-result-format [fmt type-count]
            (->> (pgw/with-result-formats (repeat type-count {}) fmt)
                 (mapv :result-format)))]

    (let [field-count 2]
      (t/is (= [:text :text]
               (resolve-result-format [] field-count))
            "no formats provided, all fields text")

      (t/is (= [:text :text]
               (resolve-result-format [:text] field-count))
            "single format provided (text), applies to all fields")

      (t/is (= [:binary :binary]
               (resolve-result-format [:binary] field-count))
            "single format provided (binary), applies to all fields")

      (t/is (= [:text :binary]
               (resolve-result-format [:text :binary] field-count))
            "format provided for each field, applies to each by index"))

    (t/is (anomalous? [:incorrect ::pgw/invalid-result-format nil
                       {:result-format [:text :binary], :type-count 3}]
                      (resolve-result-format [:text :binary] 3))
          "if more than 1 format is provided and it doesn't match the field count this is invalid")))

(deftest test-pgjdbc-boolean-param
  (doseq [binary? [true false]
          v [true false]]
    ;;pgjdbc doesn't actually send or recieve boolean in binary format
    ;;but it claims to and might someday
    (t/testing (format "binary?: %s, value?: %s" binary? v)

      (with-open [conn (jdbc-conn {"prepareThreshold" -1 "binaryTransfer" binary?})
                  stmt (.prepareStatement conn "SELECT ? AS v")]

        (.setBoolean stmt 1 v)

        (with-open [rs (.executeQuery stmt)]

          (t/is (= [{"v" "bool"}]
                   (result-metadata stmt)
                   (result-metadata rs)))

          (.next rs)
          (t/is (= v (.getBoolean rs 1))))))))

(deftest test-transit-param
  (with-open [node (xtn/start-node)]
    (t/testing "pgwire metadata query"
      (t/is (= [{:oid 16384, :typname "transit"}]
               (xt/q node ["
SELECT t.oid, t.typname
FROM pg_catalog.pg_type t
  JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
WHERE t.typname = ? AND (n.nspname = ? OR ? AND n.nspname = ANY (current_schemas(true)))
ORDER BY t.oid DESC LIMIT 1"
                           "transit" nil true])))))

  (with-open [conn (jdbc-conn {"prepareThreshold" -1})]
    (jdbc/execute! conn ["INSERT INTO foo (_id, v) VALUES (1, ?)" {:a 1, :b 2}])

    (with-open [stmt (.prepareStatement conn "SELECT v FROM foo")
                rs (.executeQuery stmt)]

      (t/is (= [{"v" "transit"}]
               (result-metadata stmt)
               (result-metadata rs)))

      (.next rs)
      (t/is (= {"a" 1, "b" 2}
               (.getObject rs 1))))

    (t/testing "qualified names"
      (jdbc/execute! conn ["INSERT INTO users RECORDS ?"
                           {:xt/id "jms", :user/first-name "James"}])

      (jdbc/execute! conn ["INSERT INTO users RECORDS ?"
                           {:_id "jdt", :user$first_name "Jeremy"}])

      (t/is (= #{{:_id "jdt", :user$first_name "Jeremy"}
                 {:_id "jms", :user$first_name "James"}}
               (set (jdbc/execute! conn (hsql/format {:select [:_id :user$first_name]
                                                      :from :users}))))
            "no transformers"))))

(t/deftest test-pgjdbc-type-query
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["BEGIN READ WRITE"])
    (try
      (jdbc/execute! conn ["INSERT INTO foo RECORDS ?" {:xt/id "foo"}])
      (jdbc/execute! conn ["COMMIT"])
      (catch Throwable t
        (jdbc/execute! conn ["ROLLBACK"])
        (throw t)))))

(deftest test-nested-transit
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 1, nest: {ts: TIMESTAMP '2020-01-01Z'}}"])

    (with-open [stmt (.prepareStatement conn "SELECT * FROM foo")]
      (t/is (= [{"_id" "int8"} {"nest" "transit"}] (result-metadata stmt))))

    (t/is (= [{:xt/id 1,
               :nest {:ts #xt/zoned-date-time "2020-01-01T00:00Z"}}]

             (q conn ["SELECT * FROM foo"])))))

(deftest insert-select-test-3684
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO docs (_id) VALUES (1), (2)"])
    (jdbc/execute! conn ["INSERT INTO docs (SELECT *, 'hi' AS foo FROM docs WHERE _id = 1)"])

    (t/is (= #{{:xt/id 1, :foo "hi"} {:xt/id 2}}
             (set (q conn ["SELECT * FROM docs"]))))))

(defn- stmt->warnings [^Statement stmt]
  (loop [warn (.getWarnings stmt) res []]
    (if-not warn
      res
      (recur (.getNextWarning warn) (conj res warn)))))

(deftest error-propagation
  (with-open [conn (jdbc-conn)
              stmt (.prepareStatement conn "SELECT 2 AS b FROM docs GROUP BY b")]
    (.execute stmt)

    (t/is (= #{"Table not found: docs" "Column not found: b"}
             (set (map #(.getMessage ^SQLWarning %) (stmt->warnings stmt)))))))

(deftest test-ignore-returning-keys-3668
  (with-open [conn (jdbc-conn)
              stmt (.prepareStatement conn "INSERT INTO people (_id, name) VALUES (6, 'fred')" Statement/RETURN_GENERATED_KEYS)]
    (t/is (false?  (.execute stmt)))
    (let [rs (.getGeneratedKeys stmt)]
      (t/is (= [] (rs->maps rs))))))

(deftest test-interval-encoding-3697
  (with-open [conn (jdbc-conn)]
    (t/is (= [{:i #xt/interval "P1DT1H1M1.111111S"}]
             (jdbc/execute! conn ["SELECT INTERVAL 'P1DT1H1M1.111111S' AS i"])))

    (t/is (= [{:i #xt/interval "P12M"}]
             (jdbc/execute! conn ["SELECT INTERVAL 'P12MT0S' AS i"])))

    (t/is (= [{:i #xt/interval "P-22M"}]
             (jdbc/execute! conn ["SELECT INTERVAL 'P-22MT0S' AS i"])))

    (t/testing "mdn implicitly truncated and returned with micro precision"
      (t/is (= [{:i #xt/interval "PT10M10.123456S"}]
               (jdbc/execute! conn ["SELECT INTERVAL '10:10.123456789' MINUTE TO SECOND(9) i"]))))))

(deftest test-playground
  (with-open [srv (pgw/open-playground)]
    (let [{:keys [port]} srv]
      (letfn [(pg-conn [db]
                {:jdbcUrl (format "jdbc:xtdb://localhost:%d/%s" port db)
                 :options "-c fallback_output_format=transit"})]
        (with-open [conn1a (jdbc/get-connection (pg-conn "foo1"))
                    conn1b (jdbc/get-connection (pg-conn "foo1"))
                    conn2 (jdbc/get-connection (pg-conn "foo2"))]

          (jdbc/execute! conn1a ["INSERT INTO foo RECORDS {_id: 1}"])
          (jdbc/execute! conn1b ["INSERT INTO bar RECORDS {_id: 1}"])
          (jdbc/execute! conn2 ["INSERT INTO foo RECORDS {_id: 2}"])

          (t/is (= [{:xt/id 1}] (q conn1a ["SELECT * FROM bar"])))
          (t/is (= [{:xt/id 1}] (q conn1b ["SELECT * FROM foo"])))
          (t/is (= [{:xt/id 2}] (q conn2 ["SELECT * FROM foo"]))))))))

(deftest test-closes-connection-to-unknown-db-3863
  (with-open [xtdb-conn (jdbc/get-connection (format "jdbc:xtdb://localhost:%d/xtdb" *port*))]
    (t/is (= [{:one 1}] (q xtdb-conn ["SELECT 1 one"]))))

  (t/is (thrown-with-msg?
         PSQLException
         #"FATAL: database 'nope' does not exist"
         (jdbc/get-connection (format "jdbc:xtdb://localhost:%d/nope" *port*)))))

(deftest set-role
  (with-open [conn (jdbc-conn {})]
    (testing "SET ROLE identifier"
      (jdbc/execute! conn ["SET ROLE anything"]))
    (testing "SET ROLE NONE"
      (jdbc/execute! conn ["SET ROLE NONE"]))
    (testing "ROLE can be used as identifier"
      (jdbc/execute! conn ["SELECT 1 AS ROLE"]))))

(deftest test-monormorphic-coercion-3693
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO docs(_id, i, f) VALUES (1, 1::SMALLINT, 1.0::FLOAT)"])
    (jdbc/execute! conn ["INSERT INTO docs(_id, i, f) VALUES (2, 2::INT, 2.0::DOUBLE PRECISION)"])

    (with-open [stmt (.prepareStatement conn "SELECT * FROM docs")]

      (t/is (= [{"_id" "int8"} {"f" "float8"} {"i" "int8"}] (result-metadata stmt)))

      (with-open [rs (.executeQuery stmt)]

        (t/is (= [{"_id" 2, "f" 2.0, "i" 2} {"_id" 1, "f" 1.0, "i" 1}] (rs->maps rs)))))))

(deftest test-missing-id-pg-wire-3768
  (with-open [conn (jdbc-conn)
              stmt (.prepareStatement conn "INSERT INTO foo (notid) VALUES (1)")]
    (t/is (is (thrown-with-msg? PSQLException #"Illegal argument: 'missing-id'" (.execute stmt))))
    (t/is (= [] (jdbc/execute! conn ["SELECT * FROM foo"])))))

(deftest test-primitive-array
  (t/testing "array as subselect"
    (with-open [conn (jdbc-conn)]
      (jdbc/execute! conn ["INSERT INTO docs(_id) VALUES (1)"])
      (jdbc/execute! conn ["INSERT INTO docs(_id) VALUES (2)"])

      (with-open [stmt (.prepareStatement conn "SELECT ARRAY(SELECT _id FROM docs ORDER BY _id) AS ids")]
        (with-open [rs (.executeQuery stmt)]
          (t/is (= [{"ids" "_int8"}]
                   (result-metadata stmt)
                   (result-metadata rs)))

          (t/is (= [{"ids" [1 2]}] (rs->maps rs)))))))

  (t/testing "array in select"
    (with-open [conn (jdbc-conn)]
      (with-open [stmt (.prepareStatement conn "SELECT [1, 2, 3] AS arr")]

        (t/is (= [{"arr" "_int8"}] (result-metadata stmt)))

        (with-open [rs (.executeQuery stmt)]
          (t/is (= [{"arr" [1 2 3]}] (rs->maps rs)))))))

  (t/testing "array as parameter"
    (with-open [conn (jdbc-conn)
                ps (jdbc/prepare conn ["INSERT INTO docs (_id, nums) VALUES (?, ?)"])]
      (.setInt ps 1 1)
      (.setArray ps 2 (.createArrayOf conn "int8" (into-array [1, 2, 3])))
      (.executeUpdate ps)

      (with-open [stmt (.prepareStatement conn "SELECT _id, nums FROM docs WHERE _id = 1")
                  rs (.executeQuery stmt)]
        (t/is (= [{"_id" "int8"}, {"nums" "_int8"}] (result-metadata stmt)))
        (t/is (= [{"_id" 1, "nums" [1, 2, 3]}] (rs->maps rs)))))))

(deftest test-text-array
  (t/testing "array as subselect"
    (with-open [conn (jdbc-conn)]
      (jdbc/execute! conn ["INSERT INTO docs(_id, name) VALUES (1, 'a')"])
      (jdbc/execute! conn ["INSERT INTO docs(_id, name) VALUES (2, 'b')"])

      (with-open [stmt (.prepareStatement conn "SELECT ARRAY(SELECT name FROM docs ORDER BY name) AS names")]
        (with-open [rs (.executeQuery stmt)]
          (t/is (= [{"names" "_text"}]
                   (result-metadata stmt)
                   (result-metadata rs)))

          (t/is (= [{"names" ["a" "b"]}] (rs->maps rs)))))))

  (t/testing "array in select"
    (with-open [conn (jdbc-conn)
                stmt (.prepareStatement conn "SELECT ['a', 'b', 'c'] AS arr")]
      (t/is (= [{"arr" "_text"}] (result-metadata stmt)))

      (with-open [rs (.executeQuery stmt)]
        (t/is (= [{"arr" ["a" "b" "c"]}] (rs->maps rs))))))

  (t/testing "text array as parameter"
    (with-open [conn (jdbc-conn)
                ;; intentionally using different table name to avoid union column type [:union [:utf8 [:list :utf8]]]
                ;; which would render this to fallback type - JSON
                ps (jdbc/prepare conn ["INSERT INTO docz (_id, name) VALUES (?, ?)"])]
      (.setInt ps 1 1)
      (.setArray ps 2 (.createArrayOf conn "text" (into-array ["aa", "bbb", "cccc"])))
      (.executeUpdate ps)

      (with-open [stmt (.prepareStatement conn "SELECT _id, name FROM docz WHERE _id = 1")
                  rs (.executeQuery stmt)]
        (t/is (= [{"_id" "int4"}, {"name" "_text"}] (result-metadata stmt)))
        (t/is (= [{"_id" 1, "name" ["aa", "bbb", "cccc"]}] (rs->maps rs)))))))

(deftest test-elixir-client-complex-array
  (with-open [conn (jdbc-conn)
              stmt (.prepareStatement conn
                                      "SELECT ARRAY (SELECT a.atttypid
                                                     FROM pg_attribute AS a
                                                     WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
                                                     ORDER BY a.attnum) as arr
                                       FROM pg_type AS t
                                         LEFT JOIN pg_type AS d ON t.typbasetype = d.oid
                                         LEFT JOIN pg_range AS r ON r.rngtypid = t.oid
                                                                 OR r.rngmultitypid = t.oid
                                                                 OR (t.typbasetype <> 0 AND r.rngtypid = t.typbasetype)
                                       WHERE (t.typrelid = 0)
                                         AND (t.typelem = 0
                                              OR NOT EXISTS (SELECT 1
                                                             FROM pg_catalog.pg_type s
                                                             WHERE s.typrelid <> 0 AND s.oid = t.typelem))")
              rs (.executeQuery stmt)]
    (t/is (= [{"arr" "_int4"}]
             (result-metadata stmt)
             (result-metadata rs)))

    (doseq [res (rs->maps rs)
            :let [arr (get res "arr")]]
      (t/is (= [] arr)))))

(deftest test-array-field
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO docs(_id, arr) VALUES (8, ARRAY[1, 2, 3])"])
    (jdbc/execute! conn ["INSERT INTO docs(_id, arr) VALUES (9, ARRAY[4, 5, 6])"])

    (with-open [stmt (.prepareStatement conn "SELECT * FROM docs order by _id")]

      (t/is (= [{"_id" "int8"} {"arr" "_int8"}] (result-metadata stmt)))

      (with-open [rs (.executeQuery stmt)]
        (t/is (= [{"_id" 8, "arr" [1 2 3]} {"_id" 9, "arr" [4 5 6]}] (rs->maps rs)))))))

(deftest pg-authentication
  (with-open [node (xtn/start-node {:authn [:user-table {:rules [{:user "xtdb", :method :password, :address "127.0.0.1"}]}]})]
    (binding [*port* (.getServerPort node)]
      (t/is (thrown-with-msg? PSQLException #"ERROR: no authentication record found for user: fin"
                              (with-open [_ (jdbc-conn {"user" "fin", "password" "foobar"})]))
            "users without record are blocked")

      ;; user xtdb should be fine
      (with-open [_ (jdbc-conn {"user" "xtdb", "password" "xtdb"})])
      (t/is (thrown-with-msg? PSQLException #"ERROR: password authentication failed for user: xtdb"
                              (with-open [_ (jdbc-conn {"user" "xtdb", "password" "foobar"})]))
            "user with a wrong password gets blocked")))

  (t/testing "users with a trusted record are allowed"
    (with-open [node (xtn/start-node {:authn [:user-table {:rules [{:user "fin", :method :trust, :address "127.0.0.1"}]}]})]
      (binding [*port* (.getServerPort node)]
        (with-open [_ (jdbc-conn {"user" "fin"})]))))

  (with-open [node (xtn/start-node {:authn [:user-table {:rules [{:user "xtdb", :method :password, :address "127.0.0.1"}
                                                                 {:user "fin", :method :password, :address "127.0.0.1"}]}]})]

    (binding [*port* (.getServerPort node)]
      (t/is (thrown-with-msg? PSQLException #"ERROR: password authentication failed for user: fin"
                              (with-open [_ (jdbc-conn {"user" "fin", "password" "foobar"})]))
            "users with a authentication record but not in the database")

      (with-open [conn (jdbc-conn {"user" "xtdb" "password" "xtdb"})]
        (jdbc/execute! conn ["CREATE USER fin WITH PASSWORD 'foobar'"])
        (with-open [stmt (.createStatement conn)
                    rs (.executeQuery stmt "SELECT username FROM pg_user")]
          (is (= (set [{"username" "xtdb"} {"username" "fin"}])
                 (set (rs->maps rs))))))

      (with-open [_ (jdbc-conn {"user" "fin" "password" "foobar"})]))))

(t/deftest test-keywordize-nested-values-3910
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO foomap RECORDS {_id: 1, a: {b: 42}}"])
    (t/is (= [{:a {:b 42}}]
             (q conn ["SELECT a FROM foomap where _id = 1"])))
    (jdbc/execute! conn ["INSERT INTO foomap RECORDS {_id: 2, a: {c$d$e: 43}, bs: [{z: {w: [12, 34, {r: 'abc'}]}, y: 4}, {z: 33, y: 44}]}"])
    (t/is (= [{:xt/id 2, :a {:c.d/e 43}, :bs [{:z {:w [12, 34, {:r "abc"}]}, :y 4}, {:z 33, :y 44}]}]
             (q conn ["SELECT * FROM foomap where _id = 2"])))))

(t/deftest jdbc-batch-3599
  (with-open [conn (jdbc-conn)
              ps (jdbc/prepare conn ["INSERT INTO foo RECORDS ?, ?"])]
    (jdbc/execute-batch! ps
                         [[{:xt/id 1, :v 1}
                           {:xt/id 2, :v 1}]
                          [{:xt/id 1, :v 2}
                           {:xt/id 3, :v 1}]])))

(t/deftest test-multiple-tzs-3723
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO docs (_id, foo) VALUES (1, TIMESTAMP '2023-03-15 12:00:00+01:00')"])
    (jdbc/execute! conn ["INSERT INTO docs (_id, foo) VALUES (2, TIMESTAMP '2023-03-15 12:00:00+03:00')"])

    (t/is (= [{:xt/id 1, :foo #xt/zdt "2023-03-15T12:00:00+01:00"}
              {:xt/id 2, :foo #xt/zdt "2023-03-15T12:00:00+03:00"}]
             (jdbc/execute! conn ["SELECT _id, foo FROM docs ORDER BY _id"]
                            {:builder-fn xt-jdbc/builder-fn})))

    (when (psql-available?)
      (psql-session
       (fn [send read]
         (send "SELECT _id, foo FROM docs ORDER BY _id;\n")
         (t/is (= [["_id" "foo"]
                   ["1" "2023-03-15 12:00:00+01:00"]
                   ["2" "2023-03-15 12:00:00+03:00"]]
                  (read))))))))

(deftest test-max-rows-3902
  (with-open [conn (jdbc-conn)
              insert-stmt (.prepareStatement conn "INSERT INTO docs (_id) VALUES (?)")
              select-stmt (.prepareStatement conn "SELECT * FROM docs ORDER BY _id")]
    (doseq [i (range 10)]
      (.setInt insert-stmt 1 i)
      (.execute insert-stmt))

    (.setMaxRows select-stmt 3)
    (with-open [rs (.executeQuery select-stmt)]
      (t/is (= [{"_id" 0} {"_id" 1} {"_id" 2}] (rs->maps rs))
            "single batch limiting"))

    (with-open [multi-batch-stmt (.prepareStatement conn "(SELECT * FROM docs ORDER BY _id LIMIT 3)
                                                          UNION ALL
                                                          (SELECT * FROM docs ORDER BY _id LIMIT 3)")]
      (.setMaxRows multi-batch-stmt 4)
      (with-open [rs (.executeQuery multi-batch-stmt)]
        (t/is (= [{"_id" 0} {"_id" 1} {"_id" 2} {"_id" 0}] (rs->maps rs))
              "multi batch limiting")))))

(deftest test-scalar-types
  (let [^bytes ba (byte-array [0 -16])]
    (with-open [conn (jdbc-conn)]
      (t/is (Arrays/equals ba ^bytes (:v (first (jdbc/execute! conn ["SELECT X('00f0') AS v"]))))
            "reading varbinary result")

      (t/testing "casting to varbinary"
        (t/is (Arrays/equals ba ^bytes (:v (first (jdbc/execute! conn ["SELECT '00f0'::varbinary AS v"])))))
        (t/is (Arrays/equals ba ^bytes (:v (first (jdbc/execute! conn ["SELECT '00f0'::bytea AS v"]))))))

      (t/testing "nested varbinary"
        (t/is (= (tu/->clj [{:v {:ba (byte-array [0x00 0xf0])}}])
                 (tu/->clj (q conn ["SELECT OBJECT(ba: X('00f0')) AS v"])))
              "reading nested varbinary result")))

    (with-open [conn (jdbc-conn {"options" "-c fallback_output_format=transit"})]
      (let [res (:v (first  (q conn ["SELECT OBJECT(ba: X('00f0')) AS v"])))]
        (t/is (Arrays/equals ba ^bytes (:ba res))))))

  (t/testing "uri literals"
    (with-open [conn (jdbc-conn)]
      (t/is (= #xt/uri "http://xtdb.com" (:v (first (jdbc/execute! conn ["SELECT URI 'http://xtdb.com' AS v"])))))

      (t/is (= [{:v {:uri #xt/uri "http://xtdb.com"}}] (q conn ["SELECT OBJECT(uri: URI 'http://xtdb.com') AS v"]))
            "nested uri"))

    (with-open [conn (jdbc-conn {"options" "-c fallback_output_format=transit"})]
      ;; TODO transit has it's own implementation of URI
      (t/is (= "http://xtdb.com" (str (:v (first  (q conn ["SELECT URI 'http://xtdb.com' AS v"]))))))))

  (t/testing "text literals"
    (with-open [conn (jdbc-conn)]
      (t/is (= [{:v "hello\n world!"}] (q conn ["SELECT E'hello\n world!' AS v"]))
            "c-style escape characters")
      (t/is (= [{:v "dollar $quoted$ string"}] (q conn ["SELECT $$dollar $quoted$ string$$ AS v"]))
            "dollar quoted string"))))


(t/deftest test-patch
  (with-open [conn (jdbc-conn)]
    (letfn [(q* [sql]
              (->> (q conn [sql])
                   (map (juxt #(select-keys % [:xt/id :a :b :c :tmp]) :xt/valid-from :xt/valid-to))))]
      (exec conn "SET TIME ZONE 'UTC'")

      (q conn ["INSERT INTO foo RECORDS {_id: 1, a: 1, b: 2}"])
      (q conn ["PATCH INTO foo RECORDS {_id: 1, c: 3}, {_id: 2, a: 4, b: 5}"])

      (t/is (= [[{:xt/id 1, :a 1, :b 2} #xt/zdt "2020-01-01Z[UTC]" #xt/zdt "2020-01-02Z[UTC]"]
                [{:xt/id 1, :a 1, :b 2, :c 3} #xt/zdt "2020-01-02Z[UTC]" nil]
                [{:xt/id 2, :a 4, :b 5} #xt/zdt "2020-01-02Z[UTC]" nil]]
               (q* "SELECT *, _valid_from, _valid_to FROM foo FOR ALL VALID_TIME ORDER BY _id, _valid_from")))

      (t/testing "for portion of valid_time"
        (q conn ["INSERT INTO bar RECORDS {_id: 1, a: 1, b: 2}"])
        (q conn ["PATCH INTO bar FOR VALID_TIME FROM DATE '2020-01-05' TO DATE '2020-01-07' RECORDS {_id: 1, tmp: 'hi!'}"])
        (q conn ["PATCH INTO bar FOR VALID_TIME FROM ? RECORDS {_id: 2, a: 6, b: 8}" #xt/zdt "2020-01-08Z"])

        (let [expected [[{:xt/id 1, :a 1, :b 2} #xt/zdt "2020-01-03Z[UTC]" #xt/zdt "2020-01-05Z[UTC]"]
                        [{:xt/id 1, :a 1, :b 2, :tmp "hi!"} #xt/zdt "2020-01-05Z[UTC]" #xt/zdt "2020-01-07Z[UTC]"]
                        [{:xt/id 1, :a 1, :b 2} #xt/zdt "2020-01-07Z[UTC]" nil]
                        [{:xt/id 2, :a 6, :b 8} #xt/zdt "2020-01-08Z[UTC]" nil]]]
          (t/is (= expected
                   (q* "SELECT *, _valid_from, _valid_to FROM bar FOR ALL VALID_TIME ORDER BY _id, _valid_from")))

          (t/testing "parse error doesn't halt ingestion"
            (t/is (thrown-with-msg? PSQLException
                                    #"internal error conforming query plan"
                                    (q conn ["PATCH INTO bar FOR VALID_TIME FROM '2020-01-05' TO DATE '2020-01-07' RECORDS {_id: 1, tmp: 'hi!'}"])))

            (t/is (= expected
                     (q* "SELECT *, _valid_from, _valid_to FROM bar FOR ALL VALID_TIME ORDER BY _id, _valid_from"))
                  "node continues"))))

      (t/testing "out-of-order updates"
        (q conn ["PATCH INTO baz FOR VALID_TIME FROM TIMESTAMP '2020-01-01Z' RECORDS {_id: 1, version: 1}"])
        (q conn ["PATCH INTO baz FOR VALID_TIME FROM TIMESTAMP '2022-01-01Z' RECORDS {_id: 1, version: 2, patched: 2022}"]) ;
        (q conn ["PATCH INTO baz FOR VALID_TIME FROM TIMESTAMP '2021-01-01Z' RECORDS {_id: 1, patched: 2021}"])

        (t/is (= [{:xt/id 1, :version 1,
                   :xt/valid-from #xt/zdt "2020-01-01Z[UTC]", :xt/valid-to #xt/zdt "2021-01-01Z[UTC]"}
                  {:xt/id 1, :patched 2021, :version 1,
                   :xt/valid-from #xt/zdt "2021-01-01Z[UTC]", :xt/valid-to #xt/zdt "2022-01-01Z[UTC]"}
                  {:xt/id 1, :patched 2021, :version 2,
                   :xt/valid-from #xt/zdt "2022-01-01Z[UTC]"}]
                 (q conn ["SELECT *, _valid_from, _valid_to FROM baz FOR ALL VALID_TIME ORDER BY _valid_from"])))))))

(t/deftest set-standard-conforming-strings-on-3972
  ;; this just has to no-op to appease the Ruby Sequel driver
  (with-open [conn (jdbc-conn)]
    (q conn ["SET STANDARD_CONFORMING_STRINGS = ON"])
    (t/is (= [{:standard-conforming-strings true}] (q conn ["SHOW STANDARD_CONFORMING_STRINGS"])))
    (t/is (= [{:world "hello"}] (q conn ["SELECT 'hello' AS world"])))))

(when (psql-available?)
  (t/deftest psql-queries-shouldnt-create-txs-4024
    (psql-session
     (fn [send read]
       (send "SELECT COUNT(*) tx_count FROM xt.txs;\n")
       (t/is (= [["tx_count"] ["0"]] (read)))
       (send "SELECT COUNT(*) tx_count FROM xt.txs;\n")
       (t/is (= [["tx_count"] ["0"]] (read)))

       (send "INSERT INTO foo RECORDS {_id: 1};\n")

       (read)

       (send "SELECT COUNT(*) tx_count FROM xt.txs;\n")
       (t/is (= [["tx_count"] ["1"]] (read)))
       (send "SELECT COUNT(*) tx_count FROM xt.txs;\n")
       (t/is (= [["tx_count"] ["1"]] (read)))))))

(t/deftest date-error-propagating-to-client-4021
  (with-open [conn (jdbc-conn)]
    (t/is (thrown-with-msg? PSQLException
                            #"Invalid value for MonthOfYear \(valid values 1 - 12\): 13"
                            (q conn ["SELECT DATE '2025-13-01'"])))

    ;; also #4027
    (t/is (thrown-with-msg? PSQLException
                            #"Unsupported cast:"
                            (jdbc/execute! conn ["SELECT CAST(DATE '2020-01-05' AS TIME) AS TS"])))

    (t/is (thrown-with-msg? PSQLException
                            #"Cannot parse timestamp: 2020-01-00"
                            (jdbc/execute! conn ["SETTING SNAPSHOT_TOKEN = TIMESTAMP '2020-01-00' SELECT 1"])))))

(t/deftest can-handle-errors-containing-instants-4025
  (with-open [conn (jdbc-conn)]
    (exec conn "SET TIME ZONE 'UTC'")

    (jdbc/execute! conn ["BEGIN READ WRITE WITH (SYSTEM_TIME = DATE '2020-01-01')"])
    (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 1}"])
    (jdbc/execute! conn ["COMMIT"])

    (t/testing "earlier sys-time"
      (jdbc/execute! conn ["BEGIN READ WRITE WITH (SYSTEM_TIME = DATE '2019-01-01')"])
      (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 2}"])
      (t/is (thrown? PSQLException (jdbc/execute! conn ["COMMIT"]))))

    (t/is (= [{:xt/id 0,
               :committed true,
               :system-time (time/->zdt #inst "2020-01-01")}
              {:xt/id 1,
               :committed false,
               :error #xt/illegal-arg [:invalid-system-time
                                       "specified system-time older than current tx"
                                       {:tx-key #xt/tx-key {:tx-id 1, :system-time #xt/instant "2019-01-01T00:00:00Z"}
                                        :latest-completed-tx #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-01T00:00:00Z"}}],
               :system-time (time/->zdt #inst "2020-01-02")}]

             (jdbc/execute! conn ["SELECT * FROM xt.txs ORDER BY _id"]
                            {:builder-fn xt-jdbc/builder-fn})))))

(t/deftest test-runtime-errors-pgwire-3994+3978
  (with-open [conn (jdbc-conn)]
    (t/is (thrown-with-msg? PSQLException #"ERROR: For input string: \"abc\""
                            (jdbc/execute! conn ["SELECT 'abc'::int;"])))
    (t/is (thrown-with-msg? PSQLException #"ERROR: data exception - division by zero"
                            (jdbc/execute! conn ["SELECT 1 / 0;"])))
    (t/is (thrown-with-msg? PSQLException #"ERROR: Negative substring length"
                            (jdbc/execute! conn ["SELECT SUBSTRING('asf' FROM 0 FOR -1);"])))))

(def display-tables-query
  "
SELECT n.nspname as \"Schema\",
  c.relname as \"Name\",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",
  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r','p','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;")

(t/deftest test-display-tables-query
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 1}"])
    (jdbc/execute! conn ["INSERT INTO bar RECORDS {_id: 2}"])

    (t/is (= [["public" "bar"]
              ["public" "foo"]
              ["xt" "txs"]]
           (map (juxt :Schema :Name)
                (q conn [display-tables-query]))))))

(t/deftest test-display-tables-psql
  (when (psql-available?)
    (psql-session
     (fn [send read]
       (send "INSERT INTO foo RECORDS {_id: 1};\n")
       (t/is (= [["INSERT 0 0"]] (read)))

       (send "\\d\n")
       (t/is (= [["Schema" "Name" "Type" "Owner"]
                 ["public" "foo" "table" "xtdb"]
                 ["xt" "txs" "table" "xtdb"]]
                (read)))))))

(t/deftest select-snapshot-token
  (with-open [conn (jdbc-conn)]
    (t/is (= [{:ts nil}] (jdbc/execute! conn ["SELECT SNAPSHOT_TOKEN ts"]))
          "before any transactions")

    (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1, :x 3}]])

    (t/is (= [{:ts (basis/->time-basis-str {"xtdb" [#xt/zdt "2020-01-01Z[UTC]"]})}]
             (jdbc/execute! conn ["SELECT SNAPSHOT_TOKEN ts"])))

    (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 2, :x 5}]])

    (t/is (= [{:snapshot_token (basis/->time-basis-str {"xtdb" [#xt/zdt "2020-01-02Z[UTC]"]})}]
             (jdbc/execute! conn ["SHOW SNAPSHOT_TOKEN"])))

    (let [sql "SELECT SNAPSHOT_TOKEN token, * FROM docs ORDER BY _id"]
      (t/is (= [{:token (basis/->time-basis-str {"xtdb" [#xt/zdt "2020-01-02Z[UTC]"]}), :_id 1, :x 3}
                {:token (basis/->time-basis-str {"xtdb" [#xt/zdt "2020-01-02Z[UTC]"]}), :_id 2, :x 5}]
               (jdbc/execute! conn [sql])))

      (t/is (= [{:token (basis/->time-basis-str {"xtdb" [#xt/zdt "2020-01-01Z[UTC]"]}), :_id 1, :x 3}]
               (jdbc/execute! conn [(str "SETTING SNAPSHOT_TOKEN = ? " sql)
                                    (basis/->time-basis-str {"xtdb" [#xt/zdt "2020-01-01Z"]})])))

      (t/testing "in tx"
        (jdbc/execute! conn ["BEGIN READ ONLY WITH (SNAPSHOT_TOKEN = ?, CLOCK_TIME = ?)"
                             (basis/->time-basis-str {"xtdb" [#xt/zdt "2020-01-01Z"]})
                             "2020-01-04Z"])
        (try
          (t/is (= [{:token (basis/->time-basis-str {"xtdb" [#xt/zdt "2020-01-01Z[UTC]"]}),
                     :_id 1, :x 3}]
                   (jdbc/execute! conn [sql])))

          (t/is (= [{:token (basis/->time-basis-str {"xtdb" [#xt/zdt "2020-01-01Z[UTC]"]}),
                     :_id 1, :x 3}]
                   (jdbc/execute! conn [sql]))
                "once more for luck")

          (t/is (= {:clock_time (-> #xt/zdt "2020-01-04Z[UTC]" in-system-tz)}
                   (jdbc/execute-one! conn ["SHOW CLOCK_TIME"])))

          (finally
            (jdbc/execute! conn ["ROLLBACK"])))))))

(t/deftest show-clock-time
  (with-open [conn (jdbc-conn)]
    (t/testing "defaults to now"
      (let [before (Instant/now)
            ct (-> (jdbc/execute-one! conn ["SHOW CLOCK_TIME"])
                   :clock_time
                   (time/->instant))
            after (Instant/now)]
        (t/is (.isBefore before ct))
        (t/is (.isAfter after ct))))

    (t/testing "setting on tx"
      (jdbc/execute! conn ["BEGIN READ ONLY WITH (CLOCK_TIME = TIMESTAMP '2024-01-01Z')"])
      (try
        (t/is (= [{:clock_time (-> #xt/zdt "2024-01-01Z[UTC]" in-system-tz)}]
                 (jdbc/execute! conn ["SHOW CLOCK_TIME"])))

        (finally
          (jdbc/execute! conn ["ROLLBACK"]))))))

(t/deftest test-explain
  ;; this one might be a little brittle, let's revisit if it fails a lot.
  (let [sql "EXPLAIN SELECT _id FROM foo"]
    (with-open [conn (jdbc-conn)]
      (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 1}"])
      (t/is (= [{:depth "->", :op :project,
                 :explain {"append?" false, "project" "[{_id foo.1/_id}]"}}
                {:depth "  ->", :op :rename, :explain {"prefix" "foo.1"}}
                {:depth "    ->", :op :scan,
                 :explain {"predicates" [], "columns" ["_id"], "table" "xtdb.public.foo"}}]
               (jdbc/execute! conn [sql]))))

    (when (psql-available?)
      (psql-session
       (fn [send read]
         (send "INSERT INTO foo RECORDS {_id: 1};\n")
         (t/is (= [["INSERT 0 0"]] (read)))
         (send (str sql ";\n"))
         (t/is (= [["depth" "op" "explain"]
                   ["->" "project"
                    "{\"project\":\"[{_id foo.1\\/_id}]\",\"append?\":false}"]
                   ["  ->" "rename"
                    "{\"prefix\":\"foo.1\"}"]
                   ["    ->" "scan"
                    "{\"columns\":[\"_id\"],\"table\":\"xtdb.public.foo\",\"predicates\":[]}"]]

                  (read))))))))

(t/deftest test-ro-server-4043
  (with-open [node (xtn/start-node {:server {:read-only-port 0, :port 0}})]
    (binding [*port* (.getServerPort node)]
      (with-open [conn (jdbc-conn)]
        (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 1}"])
        (t/is (= [{:_id 1}] (jdbc/execute! conn ["SELECT * FROM foo"])))))

    (binding [*port* (.getServerReadOnlyPort node)]
      (with-open [ro-conn (jdbc-conn)]
        (t/is (thrown-with-msg? PSQLException #"READ ONLY server"
               (jdbc/execute! ro-conn ["INSERT INTO foo RECORDS {_id: 2}"])))

        (t/is (= [{:_id 1}] (jdbc/execute! ro-conn ["SELECT * FROM foo"])))))))

(t/deftest test-return-nano-ts-as-micro-ts
  ;; we now have more precision for text than we do for binary
  (doseq [binary? [false true]
          {:keys [^Class type binary-val text-val input]} [{:input "'2024-01-01T00:00:01.123456789'::TIMESTAMP(9)"
                                                            :type LocalDateTime
                                                            :binary-val #xt/date-time "2024-01-01T00:00:01.123456"
                                                            :text-val #xt/date-time "2024-01-01T00:00:01.123456789"}
                                                           {:input "'2024-01-01T00:00:01.123456789Z'::TIMESTAMP(9) WITH TIME ZONE"
                                                            :type OffsetDateTime,
                                                            :binary-val (-> #xt/zdt "2024-01-01T00:00:01.123456Z" in-system-tz (.toOffsetDateTime))
                                                            :text-val (-> #xt/zdt "2024-01-01T00:00:01.123456789Z" in-system-tz (.toOffsetDateTime))}]
          :let [q (format "SELECT %s v" input)]]

    (t/testing (format "pgjdbc - binary?: %s, type: %s, pg-type: %s, val: %s" binary? type val input)
      (with-open [conn (jdbc-conn {"prepareThreshold" -1 "binaryTransfer" binary?})
                  stmt (.prepareStatement conn q)]
        (with-open [rs (.executeQuery stmt)]
          (.next rs)
          (t/is (= (if binary? binary-val text-val)
                   (.getObject rs 1 type))))))))

(defn- compare-decimals-with-scale [^BigDecimal d1 ^BigDecimal d2]
  (and (= (.scale d1) (.scale d2))
       (= (.unscaledValue d1) (.unscaledValue d2))))

(deftest test-pgwire-decimal-support
  (doseq [binary? [false true]]
    (with-open [conn (jdbc-conn {"prepareThreshold" -1 "binaryTransfer" binary?})
                ps (jdbc/prepare conn ["INSERT INTO table RECORDS {_id: ?, data: ?}"])]
      (jdbc/execute-batch! ps [[1 0.1M] [2 24580955505371094.000001M]])
      (jdbc/execute! conn ["INSERT INTO table RECORDS {_id: ?, data: ?}"
                           3 61954235709850086879078532699846656405640394575840079131296.39935M])

      (let [expected-res [{:_id 1, :data 0.1M}
                          {:_id 2, :data 24580955505371094.000001M}
                          {:_id 3, :data 61954235709850086879078532699846656405640394575840079131296.39935M}]
            res (jdbc/execute! conn ["SELECT * FROM table ORDER BY _id"])]
        (t/is (= expected-res res))
        (t/is (true? (->>
                      (map vector expected-res res)
                      (every? #(compare-decimals-with-scale (:data (first %)) (:data (second %))))))
              "correct scale")))))

(t/deftest test-pgwire-decimals-different-scales
  (with-open [conn (jdbc-conn)]
    (with-open [stmt (jdbc/prepare conn ["INSERT INTO decimals RECORDS ?"])]
      (jdbc/execute-batch! stmt [[{:xt/id 1, :dec 1.01M}]
                                 [{:xt/id 2, :dec 1.012M}]])
      (t/is (= [{:_id 1, :dec 1.01M} {:_id 2, :dec 1.012M}]
               (jdbc/execute! conn ["SELECT * FROM decimals ORDER BY _id"]))))

    (with-open [stmt (jdbc/prepare conn ["INSERT INTO decimals2 SELECT ? _id, ? AS `dec`"])]
      (jdbc/execute-batch! stmt [[1 1.01M]
                                 [2 1.012M]])

      (t/is (= [{:_id 1, :dec 1.01M} {:_id 2, :dec 1.012M}]
               (jdbc/execute! conn ["SELECT * FROM decimals2 ORDER BY _id"]))))))

(deftest test-pgwire-numeric-type-annotation
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO table RECORDS {_id: 1, data: ?::decimal}" 1.111111111111M])
    (jdbc/execute! conn ["INSERT INTO table RECORDS {_id: 2, data: ?::decimal}" 1.1M])
    ;; but explicit precision and scale overrides the default
    (jdbc/execute! conn ["INSERT INTO table RECORDS {_id: 3, data: ?::decimal(3,2)}" 1.11111M])

    (let [expected-res [{:_id 1, :data 1.111111111111M}
                        {:_id 2, :data 1.1M}
                        {:_id 3, :data 1.11M}]
          res (jdbc/execute! conn ["SELECT * FROM table ORDER BY _id"])]
      (t/is (= expected-res res))
      (t/is (true? (->>
                    (map vector expected-res res)
                    (every? #(compare-decimals-with-scale (:data (first %)) (:data (second %))))))
            "correct scale"))))

(deftest keyword-roundtripping-4237
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO docs RECORDS ?" {:xt/id 1, :foo :bar}])

    (with-open [ps (jdbc/prepare conn ["FROM docs"])]

      (t/is (= [{"_id" "int8"} {"foo" "keyword"}]
               (result-metadata ps)))

      (t/is (= [{:_id 1, :foo :bar}] (jdbc/execute! ps))))

    (t/is (= [{:_id 1, :foo :bar}]
             (jdbc/execute! conn ["FROM docs"])))

    (t/is (= [{:xt/id 1, :foo :bar}]
             (jdbc/execute! conn ["FROM docs"]
                            {:builder-fn xt-jdbc/builder-fn})))))

(deftest better-assert-error-code-and-message-3878
  (with-open [conn (jdbc-conn {"autocommit" "false"})]
    (testing "outside transaction"
      (t/is (= {:sql-state "P0004",
                :message "boom",
                :detail #xt/error [:conflict :xtdb/assert-failed "boom"
                                   {:arg-idx 0, :sql "ASSERT 2 < 1, 'boom'", :tx-op-idx 0,
                                    :tx-key #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-01T00:00:00Z"}}]}
               (reading-ex
                 (q conn ["ASSERT 2 < 1, 'boom'"])))))

    (testing "inside transaction"
      (q conn ["BEGIN READ WRITE"])
      (q conn ["ASSERT 2 < 1, 'boom'"])
      (t/is (= {:sql-state "P0004",
                :message "boom",
                :detail #xt/error [:conflict :xtdb/assert-failed "boom"
                                   {:arg-idx 0, :sql "ASSERT 2 < 1, 'boom'", :tx-op-idx 0,
                                    :tx-key #xt/tx-key {:tx-id 1, :system-time #xt/instant "2020-01-02T00:00:00Z"}}]}
               (reading-ex
                 (q conn ["COMMIT"]))))
      (q conn ["ROLLBACK"]))

    (testing "no error message"
      (q conn ["BEGIN READ WRITE"])
      (q conn ["ASSERT 2 < 1"])
      (t/is (thrown-with-msg? PSQLException
                              #"ERROR: Assert failed"
                              (q conn ["COMMIT"]))))))

(t/deftest test-datestyle
  (with-open [conn (jdbc-conn)]
    (t/is (= {:ts #xt/zoned-date-time "2020-01-01Z"} (jdbc/execute-one! conn ["SELECT TIMESTAMP '2020-01-01Z' ts"])))

    (jdbc/execute! conn ["SET datestyle = 'iso8601'"])
    (t/is (= {:ts #xt/zoned-date-time "2020-05-01T00:00:00+01:00[Europe/London]"}
             (jdbc/execute-one! conn ["SELECT TIMESTAMP '2020-05-01+01:00[Europe/London]' ts"])))

    (t/is (= {:ts #xt/zoned-date-time "2020-05-01T00:00:00-08:00"} (jdbc/execute-one! conn ["SELECT TIMESTAMP '2020-05-01T00:00:00-08:00' ts"])))

    (jdbc/execute! conn ["SET datestyle = 'iso'"])
    (t/is (= {:ts #xt/zoned-date-time "2020-05-01T00:00:00-08:00"} (jdbc/execute-one! conn ["SELECT TIMESTAMP '2020-05-01T00:00:00-08:00' ts"]))))

  (when (psql-available?)
    (psql-session
     (fn [send read]
       (send "SELECT TIMESTAMP '2020-01-01Z' ts;\n")
       (t/is (= [["ts"] ["2020-01-01 00:00:00+00:00"]] (read)))

       (send "SET datestyle = 'iso8601';\n")
       (t/is (= [["SET"]] (read)))

       (send "SELECT TIMESTAMP '2020-05-01+01:00[Europe/London]' ts;\n")
       (t/is (= [["ts"] ["2020-05-01T00:00+01:00[Europe/London]"]] (read)))

       (send "SELECT TIMESTAMP '2020-05-01T00:00:00-08:00' ts;\n")
       (t/is (= [["ts"] ["2020-05-01T00:00-08:00"]] (read)))

       (send "SET datestyle = 'iso';\n")
       (t/is (= [["SET"]] (read)))

       (send "SELECT TIMESTAMP '2020-05-01T00:00:00-08:00' ts;\n")
       (t/is (= [["ts"] ["2020-05-01 00:00:00-08:00"]] (read)))))))

(t/deftest in-tx-default-tz
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["SET TIME ZONE 'Asia/Tokyo'"])

    (t/testing "DML tx"
      (jdbc/execute! conn ["BEGIN READ WRITE WITH (TIMEZONE = ?)" "America/New_York"])
      (try
        (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 1, ts: (TIMESTAMP '2020-01-01'::timestamptz)}"])
        (jdbc/execute! conn ["COMMIT"])
        (finally
          (jdbc/execute! conn ["ROLLBACK"])))

      (t/is (= {:_id 1, :ts #xt/zdt "2020-01-01T00:00-05:00[America/New_York]"}
               (jdbc/execute-one! conn ["SELECT * FROM foo"]))))

    (t/is (= {:timezone "Asia/Tokyo"}
             (jdbc/execute-one! conn ["SHOW TIME ZONE"]))
          "reset tz")

    (t/testing "DQL tx"
      (jdbc/execute! conn ["BEGIN READ ONLY WITH (TIMEZONE = ?)" "America/New_York"])
      (try
        (t/is (= {:timezone "America/New_York"} (jdbc/execute-one! conn ["SHOW TIME ZONE"])))
        (t/is (= {:ts #xt/zdt "2020-01-01T00:00-05:00[America/New_York]"} (jdbc/execute-one! conn ["SELECT TIMESTAMP '2020-01-01'::timestamptz ts"])))
        (jdbc/execute! conn ["COMMIT"])
        (finally
          (jdbc/execute! conn ["ROLLBACK"]))))

    (t/is (= {:timezone "Asia/Tokyo"}
             (jdbc/execute-one! conn ["SHOW TIME ZONE"]))
          "reset tz")))

(t/deftest in-tx-await-token
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 1}"])
    (t/is (= {:await_token (basis/->tx-basis-str {"xtdb" [0]})}
             (jdbc/execute-one! conn ["SHOW AWAIT_TOKEN"])))

    (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 2}"])
    (t/is (= {:await_token (basis/->tx-basis-str {"xtdb" [1]})}
             (jdbc/execute-one! conn ["SHOW AWAIT_TOKEN"])))

    (jdbc/execute! conn ["BEGIN READ ONLY WITH (await_token = ?)" (basis/->tx-basis-str {"xtdb" [0]})])
    (try
      (t/is (= {:await_token (basis/->tx-basis-str {"xtdb" [0]})}
               (jdbc/execute-one! conn ["SHOW AWAIT_TOKEN"])))
      (finally
        (jdbc/execute! conn ["ROLLBACK"])))

    (t/is (= {:await_token (basis/->tx-basis-str {"xtdb" [1]})}
             (jdbc/execute-one! conn ["SHOW AWAIT_TOKEN"])))))

(t/deftest await-token+snapshot-token
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1, :version 0}]])
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1, :version 1}]])

  (with-open [conn (jdbc-conn)]
    (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1, :version 2}]])
    (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1, :version 3}]])

    (jdbc/execute! conn ["BEGIN READ ONLY WITH (AWAIT_TOKEN = ?)" (basis/->tx-basis-str {"xtdb" [3]})])
    (try
      (t/is (= {:_id 1, :version 3}
               (jdbc/execute-one! conn ["SELECT * FROM foo"])))
      (finally
        (jdbc/execute! conn ["ROLLBACK"])))))

(t/deftest ex-cause-in-detail-message
  (with-open [conn (jdbc-conn)]
    (t/is (= {:sql-state "0B000",
              :message "system-time (2020-01-02T00:00:00Z) is after the latest completed tx (nil)",
              :detail #xt/illegal-arg [:xtdb/unindexed-tx
                                       "system-time (2020-01-02T00:00:00Z) is after the latest completed tx (nil)"
                                       {:latest-completed-tx nil, :system-time #xt/instant "2020-01-02T00:00:00Z"}]}
             (reading-ex
               (jdbc/execute! conn ["SETTING SNAPSHOT_TOKEN = ? SELECT 1"
                                    (basis/->time-basis-str {"xtdb" [#xt/zdt "2020-01-02Z"]})]))))

    (t/is (= {:sql-state "08P01",
              :message "Queries are unsupported in a DML transaction",
              :detail #xt/error [:incorrect :xtdb/queries-in-read-write-tx
                                 "Queries are unsupported in a DML transaction"
                                 {:query "SELECT a FROM foo"}]}
             (reading-ex
               (jdbc/with-transaction [tx conn]
                 (q tx ["INSERT INTO foo(_id, a) values(42, 42)"])
                 (q conn ["SELECT a FROM foo"])))))))

(t/deftest test-patch-records-4403
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO users RECORDS {_id: 'jms', first_name: 'James'}"])

    (jdbc/execute! conn ["INSERT INTO users RECORDS ?" {:xt/id "joe", :first-name "Joe"}])

    (jdbc/execute! conn ["PATCH INTO users RECORDS ?" {:xt/id "joe", :likes "chocolate"}])

    (t/is (= [{:xt/id "jms", :first-name "James",
               :xt/valid-from #xt/zdt "2020-01-01Z[UTC]"}
              {:xt/id "joe", :first-name "Joe",
               :xt/valid-from #xt/zdt "2020-01-02Z[UTC]",
               :xt/valid-to #xt/zdt "2020-01-03Z[UTC]"}
              {:xt/id "joe", :first-name "Joe", :likes "chocolate",
               :xt/valid-from #xt/zdt "2020-01-03Z[UTC]"}]
             (jdbc/execute! conn ["SELECT *, _valid_from, _valid_to FROM users FOR ALL VALID_TIME ORDER BY _id, _valid_from"]
                            {:builder-fn xt-jdbc/builder-fn})))))

(t/deftest test-patch-interval-4475
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["PATCH INTO my_table RECORDS ?" {:xt/id "id", :i #xt/interval "PT5M"}])

    (t/is (= [{:xt/id "id", :i #xt/interval "PT5M"}]
             (jdbc/execute! conn ["SELECT * FROM my_table"]
                            {:builder-fn xt-jdbc/builder-fn})))

    (jdbc/execute! conn ["INSERT INTO my_table RECORDS ?" {:xt/id "id", :a 1, :i #xt/interval "PT10M"}])

    (t/is (= [{:xt/id "id", :a 1, :i #xt/interval "PT10M"}]
             (jdbc/execute! conn ["SELECT * FROM my_table"]
                            {:builder-fn xt-jdbc/builder-fn})))

    (jdbc/execute! conn ["PATCH INTO my_table RECORDS ?" {:xt/id "id", :i #xt/interval "PT15M"}])

    (t/is (= [{:xt/id "id", :a 1, :i #xt/interval "PT15M"}]
             (jdbc/execute! conn ["SELECT * FROM my_table"]
                            {:builder-fn xt-jdbc/builder-fn})))))

(t/deftest begin-async
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["BEGIN READ WRITE WITH (ASYNC = TRUE)"])
    (try
      (jdbc/execute! conn ["INSERT INTO users RECORDS {_id: 'jms', given_name: 'James'}"])
      (jdbc/execute! conn ["COMMIT"])
      (catch Throwable t
        (jdbc/execute! conn ["ROLLBACK"])
        (throw t)))

    (t/is (= [{:xt/id "jms", :given-name "James"}]
             (jdbc/execute! conn ["SELECT * FROM users"]
                            {:builder-fn xt-jdbc/builder-fn})))

    (jdbc/execute! conn ["BEGIN READ WRITE WITH (ASYNC = TRUE)"])

    (t/testing "shouldn't throw synchronously"
      (try
        (jdbc/execute! conn ["ASSERT 1 > 2"])
        (jdbc/execute! conn ["INSERT INTO users RECORDS {_id: 'jdt', given_name: 'Jeremy'}"])
        (jdbc/execute! conn ["COMMIT"])
        (catch Throwable t
          (jdbc/execute! conn ["ROLLBACK"])
          (throw t))))

    (t/is (= [{:xt/id "jms", :given-name "James"}]
             (jdbc/execute! conn ["SELECT * FROM users"]
                            {:builder-fn xt-jdbc/builder-fn})))))

(t/deftest test-show-variables-returns-binary-4498
  (with-open [conn (jdbc-conn {"prepareThreshold" 1})]
    (dotimes [n 3]
      (xt/submit-tx conn [[:put-docs :foo {:xt/id n}]]))

    (t/is (= [{:xt/id 0} {:xt/id 1} {:xt/id 2}]
             (xt/q conn ["SELECT * FROM foo ORDER BY _id"])))))

(t/deftest snapshot-token-after-lctx-4465
  (with-open [conn (jdbc-conn)]
    (xt/execute-tx conn [[:put-docs :foo {:xt/id 1}]])
    (t/is (= [{:xt/id 1}] (xt/q conn "SELECT * FROM foo")))
    (doto (.getLog (db/primary-db tu/*node*))
      (.appendMessage (Log$Message$FlushBlock. 1)))
    (t/is (= [{:xt/id 1}] (xt/q conn "SELECT * FROM foo")))))

(t/deftest test-database-metadata
  (with-open [conn (jdbc-conn)]
    ;; atm we just return 'xtdb' to keep pgjdbc happy
    (t/is (= "xtdb" (.getSQLKeywords (.getMetaData conn))))))

(t/deftest param-in-begin-statement-blocks-4588
  (with-open [conn (jdbc-conn)]
    (t/is (= {:greeting "hello"}
             (jdbc/execute-one! conn ["SELECT ? AS greeting" "hello"])))

    (jdbc/execute! conn ["BEGIN READ ONLY WITH (TIMEZONE = 'Australia/Perth')"])
    (t/is (= #xt/zone "Australia/Perth"
             (.getZone (time/->zdt (:ts (jdbc/execute-one! conn ["SELECT CURRENT_TIMESTAMP ts"]))))))
    (jdbc/execute! conn ["COMMIT"])

    (jdbc/execute! conn ["BEGIN READ ONLY WITH (TIMEZONE = ?)" "Australia/Perth"])
    (t/is (= #xt/zone "Australia/Perth"
             (.getZone (time/->zdt (:ts (jdbc/execute-one! conn ["SELECT CURRENT_TIMESTAMP ts"]))))))
    (jdbc/execute! conn ["COMMIT"])))

(t/deftest disallow-inserting-system-from-with-records-4550
  (with-open [conn (jdbc-conn)]
    (t/is (thrown-with-msg? PSQLException #"Cannot put documents with columns.*_system_from"
                            (jdbc/execute! conn ["INSERT INTO docs RECORDS {_id: 1, _system_from: TIMESTAMP '2024-01-01T00:00:00Z'}"])))

    (t/is (thrown-with-msg? PSQLException #"Cannot put documents with columns.*_system_from"
                            (jdbc/execute! conn ["INSERT INTO docs RECORDS ?"
                                                 {:_id 2, :_system_from #inst "2024-01-01T00:00:00Z"}])))

    (t/is (thrown-with-msg? PSQLException #"Cannot put documents with columns.*_system_from"
                            (jdbc/execute! conn ["INSERT INTO docs RECORDS ?"
                                                 {"_id" 2, "_system_from" #inst "2024-01-01T00:00:00Z"}])))))
