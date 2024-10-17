(ns xtdb.pgwire-test
  (:require [clojure.data.csv :as csv]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing] :as t]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as result-set]
            [pg.core :as pg]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.pgwire :as pgwire]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.io InputStream)
           (java.lang Thread$State)
           [java.net Socket]
           (java.sql Connection PreparedStatement ResultSet SQLWarning Statement Timestamp Types)
           (java.time Clock Instant LocalDate LocalDateTime OffsetDateTime ZoneId ZoneOffset)
           java.util.Calendar
           (java.util.concurrent CountDownLatch TimeUnit)
           java.util.List
           java.util.TimeZone
           (org.pg.enums OID)
           (org.pg.error PGError PGErrorResponse)
           (org.postgresql.util PGInterval PGobject PSQLException)
           xtdb.JsonSerde))

(set! *warn-on-reflection* false)
(set! *unchecked-math* false)

(def ^:dynamic ^:private *port* nil)
(def ^:dynamic ^:private *server* nil)

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
  (let [server (-> tu/*node* :system :xtdb.pgwire/server)]
    (binding [*server* server
              *port* (:port server)]
      (f))))

(defn serve
  ([] (serve {}))
  ([opts] (pgwire/serve tu/*node* (merge {:num-threads 1} opts {:port 0, :drain-wait 250}))))

(t/use-fixtures :each tu/with-mock-clock tu/with-node with-server-and-port)

(defn- pg-config [params]
  (merge
   {:host "localhost"
    :port *port*
    :user "xtdb"
    :database "xtdb"}
   params))

;; connect to the database
(defn- pg-conn ^org.pg.Connection [params]
  (pg/connect (pg-config params)))

(defn- jdbc-url [& params]
  (let [param-str (when (seq params) (str "?" (str/join "&" (for [[k v] (partition 2 params)] (str k "=" v)))))]
    (format "jdbc:postgresql://localhost:%s/xtdb%s" *port* (or param-str ""))))

(defn- jdbc-conn ^Connection [& params]
  (jdbc/get-connection (apply jdbc-url params)))

(defn- stmt->warnings [^Statement stmt]
  (loop [warn (.getWarnings stmt) res []]
    (if-not warn
      res
      (recur (.getNextWarning warn) (conj res warn)))))

(deftest connect-with-next-jdbc-test
  (with-open [_ (jdbc/get-connection (jdbc-url))])
  ;; connect a second time to make sure we are releasing server resources properly!
  (with-open [_ (jdbc/get-connection (jdbc-url))]))

(defn- try-sslmode [sslmode]
  (try
    (with-open [_ (jdbc/get-connection (jdbc-url "sslmode" sslmode))])
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
                    (for [idx (range 1 (inc (.getColumnCount md)))]
                      {(.getColumnName md idx) (.getObject rs ^long idx)})
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
    (with-open [_ (jdbc/get-connection (jdbc-url "gssEncMode" gssencmode))])
    :ok
    (catch PSQLException e
      (if (= "The server does not support GSS Encoding." (.getMessage e))
        :unsupported
        (throw e)))))

(deftest gssenc-test
  (t/are [gssencmode expect]
      (= expect (try-gssencmode gssencmode))

    "disable" :ok
    "prefer" :ok
    "require" :unsupported))

(deftest query-test
  (with-open [conn (jdbc-conn)
              stmt (.createStatement conn)
              rs (.executeQuery stmt "SELECT a.a FROM (VALUES ('hello, world')) a (a)")]
    (is (= true (.next rs)))
    (is (= false (.next rs)))))

(deftest simple-query-test
  (with-open [conn (jdbc-conn "preferQueryMode" "simple")
              stmt (.createStatement conn)
              rs (.executeQuery stmt "SELECT a.a FROM (VALUES ('hello, world')) a (a)")]
    (is (= [{"a" "hello, world"}]
           (rs->maps rs)))))

;;TODO ADD support for multiple statments in a single simple query
#_(deftest mulitiple-statement-simple-query-test
  (with-open [conn (jdbc-conn "preferQueryMode" "simple")
              stmt (.createStatement conn)
              rs (.executeQuery stmt "SELECT a.a FROM (VALUES ('hello, world')) a (a)")]
    (is (= true (.next rs)))
    (is (= false (.next rs)))))

(deftest prepared-query-test
  (with-open [conn (jdbc-conn "prepareThreshold" "1")
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

(deftest parameterized-query-test
  (with-open [conn (jdbc-conn)
              stmt (doto (.prepareStatement conn "SELECT a.a FROM (VALUES (?)) a (a)")
                     (.setObject 1 "hello, world"))
              rs (.executeQuery stmt)]
    (is (= true (.next rs)))
    (is (= false (.next rs)))))

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

     {:sql "DATE '2021-12-24' - DATE '2021-12-23'", :clj 1}

     ;; arrays

     {:sql "ARRAY []", :clj []}
     {:sql "ARRAY [42]", :clj [42]}
     {:sql "ARRAY ['2022-01-02']", :clj ["2022-01-02"]}
     {:sql "ARRAY [ARRAY ['42'], 42, '42']", :clj [["42"] 42 "42"]}]))

(deftest json-representation-test
  (with-open [conn (jdbc-conn)]
    (doseq [{:keys [sql, clj, clj-pred]} json-representation-examples]
      (testing (str "SQL expression " sql " should parse to " clj)
        (with-open [stmt (.prepareStatement conn (format "SELECT a FROM (VALUES (%s), (ARRAY [])) a (a)" sql))]
          ;; empty array to force polymoprhic/json return
          (with-open [rs (.executeQuery stmt)]
            ;; one row in result set
            (.next rs)

            (testing "record set contains expected object"
              (is (instance? PGobject (.getObject rs 1)))
              (is (= "json" (.getType ^PGobject (.getObject rs 1)))))

            (testing "json parses to expected clj value"
              (let [clj-value (JsonSerde/decode (str (.getObject rs 1)))]
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
    (with-open [server (pgwire/serve tu/*node* {:port 0})]
      (close-method server)
      (check-server-resources-freed server))))

(defn <-pgobject [v]
  (if (instance? PGobject v)
    (let [^PGobject v v
          type (.getType v)
          value (.getValue v)]
      (if (#{"jsonb" "json"} type)
        (some-> value json/read-str)
        value))
    v))

(defn q [conn sql]
  (->> (jdbc/execute! conn sql)
       (mapv (fn [row]
               (update-vals row <-pgobject)))))

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

(defn check-conn-resources-freed [{{:keys [^Socket socket]} :frontend :as server-conn}]
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
  (with-open [server (serve {:drain-wait 0})]
    (binding [*port* (:port server)]
      (with-open [_client-conn (jdbc-conn)
                  server-conn (get-last-conn server)]
        (.close server)
        (is (wait-for-close server-conn 500))
        (check-conn-resources-freed server-conn)))))

(deftest canned-response-test
  ;; quick test for now to confirm canned response mechanism at least doesn't crash!
  ;; this may later be replaced by client driver tests (e.g test sqlalchemy connect & query)
  (with-redefs [pgwire/canned-responses [{:q "hello!"
                                          :cols [{:column-name "greet", :column-oid @#'pgwire/oid-json}]
                                          :rows (fn [_] [["\"hey!\""]])}]]
    (with-open [conn (jdbc-conn)]
      (is (= [{:greet "hey!"}] (q conn ["hello!"]))))))

(deftest concurrent-conns-test
  (with-open [server (serve {:num-threads 2})]
    (binding [*port* (:port server)]
      (let [results (atom [])
            spawn (fn spawn []
                    (future
                      (with-open [conn (jdbc-conn)]
                        (swap! results conj (ping conn)))))
            futs (vec (repeatedly 10 spawn))]

        (is (every? #(not= :timeout (deref % 500 :timeout)) futs))
        (is (= 10 (count @results)))

        (.close server)
        (check-server-resources-freed server)))))

(deftest concurrent-conns-close-midway-test
  (with-open [server (serve {:num-threads 2 :accept-so-timeout 10})]
    (binding [*port* (:port server)]
      (tu/with-log-level 'xtdb.pgwire :off
        (let [spawn (fn spawn [i]
                      (future
                        (try
                          (with-open [conn (jdbc-conn "loginTimeout" "1"
                                                      "socketTimeout" "1")]
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
    (let [cmd-parse @#'pgwire/cmd-parse
          {:keys [!closing?]} server
          latch (CountDownLatch. 10)]
      ;; redefine parse to block when we ping
      (with-redefs [pgwire/cmd-parse
                    (fn [conn {:keys [query] :as cmd}]
                      (if-not (str/starts-with? query "select 'ping'")
                        (cmd-parse conn cmd)
                        (do
                          (.countDown latch)
                          ;; delay until we see a draining state
                          (loop [wait-until (+ (System/currentTimeMillis) 5000)]
                            (when (and (< (System/currentTimeMillis) wait-until)
                                       (not @!closing?))
                              (recur wait-until)))
                          (cmd-parse conn cmd))))]
        (let [spawn (fn spawn [] (future (with-open [conn (jdbc-conn)] (ping conn))))
              futs (vec (repeatedly 10 spawn))]

          (is (.await latch 1 TimeUnit/SECONDS))

          (util/close server)

          (is (every? #(= "ping" (deref % 1000 :timeout)) futs))

          (check-server-resources-freed server))))))

;;TODO no current support for cancelling queries

(deftest jdbc-prepared-query-close-test
  (with-open [conn (jdbc-conn "prepareThreshold" "1"
                              "preparedStatementCacheQueries" 0
                              "preparedStatementCacheMiB" 0)]
    (dotimes [i 3]

      (with-open [stmt (.prepareStatement conn (format "SELECT a.a FROM (VALUES (%s)) a (a)" i))]
        (.executeQuery stmt)))

    (testing "no portal should remain, they are closed by sync, explicit close or rebinding the unamed portal"
      (is (empty? (:portals @(:conn-state (get-last-conn))))))

    (testing "the last statement should still exist as they last the duration of the session and are only closed by
              an explicit close message, which the pg driver sends between execs"
      ;; S_3 because i == 3
      (is (= #{"", "S_3"} (set (keys (:prepared-statements @(:conn-state (get-last-conn))))))))))

(defn psql-available?
  "Returns true if psql is available in $PATH"
  []
  (try (= 0 (:exit (sh/sh "which" "psql"))) (catch Throwable _ false)))

(defn psql-session
  "Takes a function of two args (send, read).

  Send puts a string in to psql stdin, reads the next string from psql stdout. You can use (read :err) if you wish to read from stderr instead."
  [f]
  ;; there are other ways to do this, but its a straightforward factoring that removes some boilerplate for now.
  (let [^List argv ["psql" "-h" "localhost" "-p" (str *port*) "--csv"]
        pb (ProcessBuilder. argv)
        p (.start pb)
        in (.getInputStream p)
        err (.getErrorStream p)
        out (.getOutputStream p)

        send
        (fn [^String s]
          (.write out (.getBytes s "utf-8"))
          (.flush out))

        read
        (fn read
          ([] (read in))
          ([stream]
           (let [^InputStream stream (case stream :err err stream)]
             (loop [wait-until (+ (System/currentTimeMillis) 1000)]
               (or (when (pos? (.available stream))
                     (let [barr (byte-array (.available stream))]
                       (.read stream barr)
                       (let [read-str (String. barr)]
                         (when-not (str/starts-with? read-str "Null display is")
                           (csv/read-csv read-str)))))

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

        (util/try-close in)
        (util/try-close out)
        (util/try-close err)))))

;; define psql tests if psql is available on path
;; (will probably move to a selector)
(when (psql-available?)
  (deftest psql-connect-test
    (let [{:keys [exit, out]} (sh/sh "psql" "-h" "localhost" "-p" (str *port*) "-c" "select 'ping' ping")]
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

  (with-open [node (xtn/start-node {:server {:port 0
                                             :ssl {:keystore (io/file (io/resource "xtdb/pgwire/xtdb.jks"))
                                                   :keystore-password "password123"}}})]
    (binding [*port* (.getServerPort node)]
      (with-open [conn (jdbc/get-connection (jdbc-url "sslmode" "require"))]
        (jdbc/execute! conn ["INSERT INTO foo (_id) VALUES (1)"])
        (t/is (= [{:_id 1}]
                 (jdbc/execute! conn ["SELECT * FROM foo"]))))

      (when (psql-available?)
        (let [{:keys [exit, out]} (sh/sh "psql" "-h" "localhost" "-p" (str *port*) "-c" "\\conninfo")]
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
         (with-redefs [clojure.tools.logging/logf (constantly nil)]
           (send "slect baz.a from baz;\n")
           (is (str/includes? (->> (read :err) (map str/join) (str/join "\n"))
                              "line 1:0 mismatched input 'slect' expecting")))

         (testing "query error allows session to continue"
           (send "select 'ping';\n")
           (is (= [["_column_1"] ["ping"]] (read)))))

       (testing "error during query execution"
         (with-redefs [clojure.tools.logging/logf (constantly nil)]
           (send "select (1 / 0) from (values (42)) a (a);\n")
           (is (= [["ERROR:  data exception — division by zero"]] (read :err))))

         (testing "query error allows session to continue"
           (send "select 'ping';\n")
           (is (= [["_column_1"] ["ping"]] (read)))))))))


;; maps cannot be created from SQL yet, or used as parameters - but we can read them from XT.
(deftest map-read-test
  (with-open [conn (jdbc-conn)]
    (-> (xt/submit-tx tu/*node* [[:put-docs :a {:xt/id "map-test", :a {:b 42}}]])
        (tu/then-await-tx tu/*node*))

    (let [rs (q conn ["select a.a from a a"])]
      (is (= [{:a {"b" 42}}] rs)))))

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
(deftest transaction-by-default-pins-the-basis-to-last-tx-test
  (let [insert #(xt/submit-tx tu/*node* [[:put-docs %1 %2]])]
    (-> (insert :a {:xt/id :fred, :name "Fred"})
        (tu/then-await-tx tu/*node*))

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
    (is (= [{:_id 42}] (q conn ["SELECT * FROM foo"])))))

(defn- session-variables [server-conn ks]
  (-> server-conn :conn-state deref :session (select-keys ks)))

(defn- next-transaction-variables [server-conn ks]
  (-> server-conn :conn-state deref :session :next-transaction (select-keys ks)))

(deftest session-access-mode-default-test
  (with-open [_ (jdbc-conn)]
    (is (= {:access-mode :read-only} (session-variables (get-last-conn) [:access-mode])))))

(deftest set-transaction-test
  (with-open [conn (jdbc-conn)]
    (testing "SET TRANSACTION overwrites variables for the next transaction"
      (q conn ["SET TRANSACTION READ ONLY"])
      (is (= {:access-mode :read-only} (next-transaction-variables (get-last-conn) [:access-mode])))
      (q conn ["SET TRANSACTION READ WRITE"])
      (is (= {:access-mode :read-write} (next-transaction-variables (get-last-conn) [:access-mode]))))

    (testing "opening and closing a transaction clears this state"
      (q conn ["SET TRANSACTION READ ONLY"])
      (jdbc/with-transaction [tx conn] (ping tx))
      (is (= {} (next-transaction-variables (get-last-conn) [:access-mode])))
      (is (= {:access-mode :read-only} (session-variables (get-last-conn) [:access-mode]))))

    (testing "rolling back a transaction clears this state"
      (q conn ["SET TRANSACTION READ WRITE"])
      (.setAutoCommit conn false)
      ;; explicitly send rollback .rollback doesn't necessarily do it if you haven't issued a query
      (q conn ["ROLLBACK"])
      (.setAutoCommit conn true)
      (is (= {} (next-transaction-variables (get-last-conn) [:access-mode]))))))

(defn tx! [conn & sql]
  (q conn ["SET TRANSACTION READ WRITE"])
  (jdbc/with-transaction [tx conn]
    (run! #(q tx %) sql)))

(deftest dml-test
  (with-open [conn (jdbc-conn)]
    (testing "mixing a read causes rollback"
      (q conn ["SET TRANSACTION READ WRITE"])
      (is (thrown-with-msg? PSQLException #"Queries are unsupported in a DML transaction"
                            (jdbc/with-transaction [tx conn]
                              (q tx ["INSERT INTO foo(_id, a) values(42, 42)"])
                              (q conn ["SELECT a FROM foo"])))))

    (testing "insert it"
      (q conn ["SET TRANSACTION READ WRITE"])
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
       (testing "set transaction"
         (send "SET TRANSACTION READ WRITE;\n")
         (is (= [["SET TRANSACTION"]] (read))))

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

;; SQL:2011 p1037 1,a,i
(deftest set-transaction-in-transaction-error-test
  (with-open [conn (jdbc-conn)]
    (q conn ["BEGIN"])
    (is (thrown-with-msg?
          PSQLException
          #"ERROR\: invalid transaction state \-\- active SQL\-transaction"
          (q conn ["SET TRANSACTION READ WRITE"])))))

(deftest begin-in-a-transaction-error-test
  (with-open [conn (jdbc-conn)]
    (q conn ["BEGIN"])
    (is (thrown-with-msg?
          PSQLException
          #"ERROR\: invalid transaction state \-\- active SQL\-transaction"
          (q conn ["BEGIN"])))))

(deftest test-current-time
  ;; no support for setting current-time so need to interact with clock directly
  (let [custom-clock (Clock/fixed (Instant/parse "2000-08-16T11:08:03Z") (ZoneId/of "GMT"))]

    (swap! (:server-state *server*) assoc :clock custom-clock)

    (with-open [conn (jdbc-conn)]

      (let [current-time #(get (first (rs->maps (.executeQuery (.createStatement %) "SELECT CURRENT_TIMESTAMP ct"))) "ct")]

        (t/is (= #inst "2000-08-16T11:08:03.000000000-00:00" (current-time conn)))

        (testing "current ts instant is pinned during a tx, regardless of what happens to the session clock"

          (let [{:keys [conn-state]} (get-last-conn)]
            (swap! conn-state assoc-in [:session :clock] (Clock/fixed Instant/EPOCH ZoneOffset/UTC)))

          (jdbc/with-transaction [tx conn]
            (let [epoch #inst "1970-01-01T00:00:00.000000000-00:00"]
              (t/is (= epoch (current-time tx)))
              (Thread/sleep 10)
              (t/is (= epoch (current-time tx)))

              (testing "inside a transaction the instant is fixed, regardless of the session clock"
                (let [{:keys [conn-state]} (get-last-conn)]
                  (swap! conn-state assoc-in [:session :clock] custom-clock))
                (t/is (= epoch (current-time tx)))))))))))

(deftest test-timezone
  (with-open [conn (jdbc-conn)]

    (let [q-tz #(get (first (rs->maps (.executeQuery (.createStatement %) "SHOW TIMEZONE"))) "timezone")
          exec #(.execute (.createStatement %1) %2)
          default-tz (str (.getZone (Clock/systemDefaultZone)))]

      (t/testing "expect server timezone"
        (t/is (= default-tz (q-tz conn))))

      (t/testing "utc"
        (exec conn "SET TIME ZONE '+00:00'")
        (t/is (= "Z" (q-tz conn))))

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
        (t/is (= "Asia/Tokyo" (q-tz conn)))))))

(deftest pg-begin-unsupported-syntax-error-test
  (with-open [conn (jdbc-conn "autocommit" "false")]
    (is (thrown-with-msg? PSQLException #"line 1:6 mismatched input 'not'" (q conn ["BEGIN not valid sql!"])))
    (is (thrown-with-msg? PSQLException #"line 1:6 extraneous input 'SERIALIZABLE'" (q conn ["BEGIN SERIALIZABLE"])))))

(deftest begin-with-access-mode-test
  (with-open [conn (jdbc-conn "autocommit" "false")]
    (testing "DML enabled with BEGIN READ WRITE"
      (q conn ["BEGIN READ WRITE"])
      (q conn ["INSERT INTO foo (_id) VALUES (42)"])
      (q conn ["COMMIT"])
      (is (= [{:_id 42}] (q conn ["SELECT _id from foo"]))))

    (testing "BEGIN access mode overrides SET TRANSACTION"
      (q conn ["SET TRANSACTION READ WRITE"])
      (is (= {:access-mode :read-write} (next-transaction-variables (get-last-conn) [:access-mode])))
      (q conn ["BEGIN READ ONLY"])
      (testing "next-transaction cleared on begin"
        (is (= {} (next-transaction-variables (get-last-conn) [:access-mode]))))
      (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (q conn ["INSERT INTO foo (_id) VALUES (43)"])))
      (q conn ["ROLLBACK"]))))

(deftest start-transaction-test
  (with-open [conn (jdbc-conn "autocommit" "false")]
    (let [sql #(q conn [%])]

      (sql "START TRANSACTION READ ONLY")
      (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction"
                            (sql "INSERT INTO foo (_id) VALUES (42)")))
      (sql "ROLLBACK")

      (t/testing "we can now infer the transaction access-mode #3624"
        (sql "START TRANSACTION")
        (sql "INSERT INTO foo (_id) VALUES (42)")
        (sql "COMMIT")
        (is (= [{:_id 42}] (q conn ["SELECT _id from foo"]))))

      (t/testing "we can't mix modes"
        (t/testing "w -> r"
          (sql "START TRANSACTION")
          (sql "INSERT INTO foo (_id) VALUES (42)")
          (is (thrown-with-msg? PSQLException #"Queries are unsupported in a DML transaction"
                                (sql "SELECT * FROM foo")))
          (sql "ROLLBACK"))

        (t/testing "r -> w"
          (sql "START TRANSACTION")
          (is (= [{:_id 42}] (q conn ["SELECT _id from foo"])))
          (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction"
                                (sql "INSERT INTO foo (_id) VALUES (42)")))
          (sql "ROLLBACK")))

      (t/testing "explicit READ WRITE"
        (sql "START TRANSACTION READ WRITE")
        (sql "INSERT INTO foo (_id) VALUES (43)")
        (sql "COMMIT")
        (is (= #{{:_id 42} {:_id 43}} (set (q conn ["SELECT _id from foo"])))))

      (testing "access mode overrides SET TRANSACTION"
        (sql "SET TRANSACTION READ WRITE")
        (sql "START TRANSACTION READ ONLY")
        (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (sql "INSERT INTO foo (_id) VALUES (42)")))
        (sql "ROLLBACK"))

      (testing "set transaction overrides inference"
        (testing "set transaction cleared"
          (sql "SET TRANSACTION READ ONLY")
          (sql "START TRANSACTION")
          (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (sql "INSERT INTO foo (_id) VALUES (42)")))
          (sql "ROLLBACK"))

        (testing "set transaction cleared"
          (sql "START TRANSACTION")
          (is (sql "INSERT INTO foo (_id) VALUES (44)"))
          (sql "COMMIT")
          (is (= #{{:_id 42} {:_id 43} {:_id 44}} (set (q conn ["SELECT _id from foo"])))))))))

(deftest set-session-characteristics-test
  (with-open [conn (jdbc-conn "autocommit" "false")]
    (let [sql #(q conn [%])]
      (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE")
      (sql "START TRANSACTION")
      (sql "INSERT INTO foo (_id) VALUES (42)")
      (sql "COMMIT")

      (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
      (is (= [{:_id 42}] (q conn ["SELECT _id from foo"])))

      (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE")
      (sql "START TRANSACTION")
      (sql "INSERT INTO foo (_id) VALUES (43)")
      (sql "COMMIT")

      (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
      (is (= #{{:_id 42}, {:_id 43}} (set (q conn ["SELECT _id from foo"])))))))

(deftest set-valid-time-defaults-test
  (with-open [conn (jdbc-conn)]
    (let [sql #(q conn [%])]
      (sql "START TRANSACTION READ WRITE")
      (sql "INSERT INTO foo (_id, version) VALUES ('foo', 0)")
      (sql "COMMIT")

      (is (= [{:version 0,
               :_valid_from #inst "2020-01-01T00:00:00.000000000-00:00",
               :_valid_to nil}]
             (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

      (sql "START TRANSACTION READ WRITE")
      (sql "UPDATE foo SET version = 1 WHERE _id = 'foo'")
      (sql "COMMIT")

      (is (= [{:version 1,
              :_valid_from #inst "2020-01-02T00:00:00.000000000-00:00",
              :_valid_to nil}]
             (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

      (is (= [{:version 0,
               :_valid_from #inst "2020-01-01T00:00:00.000000000-00:00",
               :_valid_to #inst "2020-01-02T00:00:00.000000000-00:00"}
              {:version 1,
               :_valid_from #inst "2020-01-02T00:00:00.000000000-00:00",
               :_valid_to nil}]
             (q conn ["SETTING DEFAULT VALID_TIME ALL
                       SELECT version, _valid_from, _valid_to FROM foo ORDER BY version"])))

      (is (= [{:version 1,
               :_valid_from #inst "2020-01-02T00:00:00.000000000-00:00",
               :_valid_to nil}]
             (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

      (sql "START TRANSACTION READ WRITE")
      (sql "UPDATE foo FOR ALL VALID_TIME SET version = 2 WHERE _id = 'foo'")
      (sql "COMMIT")

      (is (= [{:version 2,
               :_valid_from #inst "2020-01-02T00:00:00.000000000-00:00",
               :_valid_to nil}]
             (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

      (is (= [{:version 2,
               :_valid_from #inst "2020-01-01T00:00:00.000000000-00:00",
               :_valid_to #inst "2020-01-02T00:00:00.000000000-00:00"}
              {:version 2,
               :_valid_from #inst "2020-01-02T00:00:00.000000000-00:00",
               :_valid_to nil}]
             (q conn ["SETTING DEFAULT VALID_TIME ALL
                       SELECT version, _valid_from, _valid_to FROM foo"])))

      (is (= [{:version 2,
               :_valid_from #inst "2020-01-02T00:00:00.000000000-00:00",
               :_valid_to nil}]
             (q conn ["SETTING DEFAULT VALID_TIME AS OF NOW SELECT version, _valid_from, _valid_to FROM foo"]))))))

(t/deftest test-setting-basis-current-time-3505
  (with-open [conn (jdbc-conn)]
    (is (= [{:ts #inst "2020-01-01"}]
           (q conn ["SETTING CURRENT_TIME = TIMESTAMP '2020-01-01T00:00:00Z'
                     SELECT CURRENT_TIMESTAMP AS ts"])))

    (q conn ["INSERT INTO foo (_id, version) VALUES ('foo', 0)"])

    (is (= [{:version 0,
             :_valid_from #inst "2020-01-01T00:00:00.000000000-00:00",
             :_valid_to nil}]
           (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

    (q conn ["UPDATE foo SET version = 1 WHERE _id = 'foo'"])

    (is (= [{:version 1,
             :_valid_from #inst "2020-01-02T00:00:00.000000000-00:00",
             :_valid_to nil}]
           (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

    (is (= [{:version 0,
             :_valid_from #inst "2020-01-01T00:00:00.000000000-00:00",
             :_valid_to nil
             :ts #inst "2024-01-01"}]
           (q conn ["SETTING BASIS = TIMESTAMP '2020-01-01T00:00:00Z',
                             CURRENT_TIME = TIMESTAMP '2024-01-01T00:00:00Z'
                     SELECT version, _valid_from, _valid_to, CURRENT_TIMESTAMP ts FROM foo"]))
        "both basis and current time")

    (q conn ["UPDATE foo SET version = 2 WHERE _id = 'foo'"])

    (is (= [{:version 2}]
           (q conn ["SELECT version FROM foo"])))

    (is (= [{:version 0}]
           (q conn ["SETTING BASIS = TIMESTAMP '2020-01-01T00:00:00Z'
                     SELECT version FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '2020-01-02T00:00:00Z'"]))
        "for system-time cannot override basis")

    (is (= [{:version 1}]
           (q conn ["SELECT version FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '2020-01-02T00:00:00Z'"]))
        "version would have been 1 if basis was not set")))

(t/deftest test-setting-import-system-time-3616
  (with-open [conn (jdbc-conn)]
    (let [sql #(q conn [%])]
      (t/testing "as part of START TRANSACTION"
        (sql "SET TIME ZONE 'Europe/London'")
        (sql "START TRANSACTION READ WRITE, AT SYSTEM_TIME DATE '2021-08-01'")
        (sql "INSERT INTO foo (_id, version) VALUES ('foo', 0)")
        (sql "COMMIT")

        (is (= [{:version 0,
                 :_system_from #inst "2021-07-31T23:00:00.000000000-00:00"}]
               (q conn ["SELECT version, _system_from FROM foo"]))))

      (t/testing "with SET TRANSACTION"
        (sql "SET TRANSACTION READ WRITE, AT SYSTEM_TIME TIMESTAMP '2021-08-03T00:00:00'")
        (sql "BEGIN")
        (sql "INSERT INTO foo (_id, version) VALUES ('foo', 1)")
        (sql "COMMIT")

        (is (= [{:version 0,
                 :_system_from #inst "2021-07-31T23:00:00.000000000-00:00"}
                {:version 1,
                 :_system_from #inst "2021-08-02T23:00:00.000000000-00:00"}]
               (q conn ["SELECT version, _system_from FROM foo FOR ALL VALID_TIME ORDER BY version"]))))

      (t/testing "past system time"
        (sql "SET TRANSACTION READ WRITE, AT SYSTEM_TIME TIMESTAMP '2021-08-02T00:00:00Z'")
        (sql "BEGIN")
        (sql "INSERT INTO foo (_id, version) VALUES ('foo', 2)")
        (t/is (thrown-with-msg? PSQLException #"specified system-time older than current tx"
                                (sql "COMMIT")))))))

;; this demonstrates that session / set variables do not change the next statement
;; its undefined - but we can say what it is _not_.
(deftest implicit-transaction-stop-gap-test
  (with-open [conn (jdbc-conn "autocommit" "false")]
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
          (is (= [{:_id 42}] (sql "select _id from foo")))))

      (testing "override session to start a read only transaction"
        (sql "BEGIN READ ONLY")
        (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (sql "INSERT INTO foo (_id) values (43)")))
        (sql "ROLLBACK")
        (testing "despite read write setting read remains available outside tx"
          (is (= [{:_id 42}] (sql "select _id from foo")))))

      (testing "set transaction is not cleared by read/autocommit DML, as they don't start a transaction"
        (sql "SET TRANSACTION READ WRITE")
        (is (= [{:_id 42}] (sql "select _id from foo")))
        (sql "INSERT INTO foo (_id) values (43)")
        (is (= #{{:_id 42} {:_id 43}} (set (sql "select _id from foo"))))
        (is (= {:access-mode :read-write} (next-transaction-variables (get-last-conn) [:access-mode])))))))

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
       (let [s (read :err)]
         (is (not= :timeout s))
         (is (str/includes? (->> s (map str/join) (str/join "\n"))
                            "line 1:0 mismatched input 'SLECT' expecting")))

       (send "BEGIN READ WRITE;\n")
       (read)
       ;; no-id
       (send "INSERT INTO foo (x) values (42);\n")
       (read)

       (send "COMMIT;\n")
       (let [error (read :err)]
         (is (not= :timeout error))
         (is (= [["ERROR:  Illegal argument: 'missing-id'"]] error)))

       (send "ROLLBACK;\n")
       (let [s (read)]
         (is (not= :timeout s))
         (is (= [["ROLLBACK"]] s)))))))

(deftest runtime-error-query-test
  (tu/with-log-level 'xtdb.pgwire :off
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
  (with-open [conn (jdbc-conn "prepareThreshold" 1 "binaryTransfer" false)]
    (q-seq conn ["INSERT INTO foo(_id, int8, int4, int2, float8 , var_char, bool) VALUES (?, ?, ?, ?, ?, ?, ?)"
                 #uuid "9e8b41a0-723f-4e6b-babb-c4e6afd17ef2" Long/MIN_VALUE Integer/MAX_VALUE Short/MIN_VALUE Double/MAX_VALUE "aa" true]))
  ;; no float4 for text format due to a bug in pgjdbc where it sends it as a float8 causing a union type.
  (with-open [conn (jdbc-conn "prepareThreshold" 1 "binaryTransfer" true)]
    (q-seq conn ["INSERT INTO foo(_id, int8, int4, int2, float8, float4, var_char, bool) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                 #uuid "7dd2ed62-bb05-43c8-b289-5503d9b19ee6" Long/MAX_VALUE Integer/MIN_VALUE Short/MAX_VALUE Double/MIN_VALUE Float/MAX_VALUE "bb" false])
    ;;binary format is only requested for server side preparedStatements in pgjdbc
    ;;threshold one should mean we test the text and binary format for each type
    (let [sql #(jdbc/execute! conn [%])
          res [{:float8 Double/MIN_VALUE,
                :float4 Float/MAX_VALUE,
                :int2 Short/MAX_VALUE,
                :var_char "bb",
                :int4 Integer/MIN_VALUE,
                :int8 Long/MAX_VALUE,
                :_id #uuid "7dd2ed62-bb05-43c8-b289-5503d9b19ee6",
                :bool false}
               {:float8 Double/MAX_VALUE,
                :float4 nil,
                :int2 Short/MIN_VALUE,
                :var_char "aa",
                :int4 Integer/MAX_VALUE,
                :int8 Long/MIN_VALUE,
                :_id #uuid "9e8b41a0-723f-4e6b-babb-c4e6afd17ef2",
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

(extend-protocol result-set/ReadableColumn
  PGobject
  (read-column-by-index [^PGobject obj _rs-meta _idx]
    (if (= "json" (.getType obj))
      (JsonSerde/decode (.getValue obj))
      obj)))

(deftest test-json-type
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO foo (_id, json, scalar) VALUES (0, ?, ?)"
                         (as-json-param {:a 1 :b 2})
                         (as-json-param 42)])

    (jdbc/execute! conn ["INSERT INTO foo (_id, json) VALUES (1, ?)"
                         (as-json-param {:a 2, :c {:d 3}})])

    (t/is (= [{:_id 1, :json {"a" 2, "c" {"d" 3}}, :scalar nil}
              {:_id 0, :json {"a" 1, "b" 2}, :scalar 42}]
             (jdbc/execute! conn ["SELECT * FROM foo"])))

    (t/is (= [{:_id 1, :a 2, :b nil, :c {"d" 3}, :d 3}
              {:_id 0, :a 1, :b 2, :c nil, :d nil}]
             (jdbc/execute! conn ["SELECT _id, (json).a, (json).b, (json).c, (json).c.d FROM foo"])))

    ;; we don't currently support JSON as a _query_ param-type, because we have to prepare the statement
    ;; without the dynamic arg values, and we can't know the type of the JSON arg until we see the value
    (t/is (thrown-with-msg? PSQLException
                            #"ERROR: Unsupported param-types in query: \[\"json\"\]"
                            (jdbc/execute! conn ["SELECT * FROM foo WHERE json = ?"
                                                 (as-json-param {:a 2, :c {:d 3}})])))))

(deftest test-odbc-queries
  (with-open [conn (jdbc-conn)]

    ;; ODBC issues this query by default
    (is (= []
           (q-seq conn ["select oid, typbasetype from pg_type where typname = 'lo'"])))))

(t/deftest test-pg-port
  (util/with-open [node (xtn/start-node {::pgwire/server {:port 0}})]
    (binding [*port* (.getServerPort node)]
      (with-open [conn (jdbc-conn)]
        (t/is (= "ping" (ping conn)))))))

(t/deftest test-assert-3445
  (with-open [conn (jdbc-conn)]
    (q conn ["SET TRANSACTION READ WRITE"])
    (jdbc/with-transaction [tx conn]
      (jdbc/execute! tx ["INSERT INTO foo (_id) VALUES (1)"]))

    (q conn ["SET TRANSACTION READ WRITE"])
    (jdbc/with-transaction [tx conn]
      (jdbc/execute! tx ["ASSERT 1 = (SELECT COUNT(*) FROM foo)"])
      (jdbc/execute! tx ["INSERT INTO foo (_id) VALUES (2)"]))

    (q conn ["SET TRANSACTION READ WRITE"])
    (t/is (thrown-with-msg? Exception
                            #"ERROR: Assert failed"
                            (jdbc/with-transaction [tx conn]
                              (jdbc/execute! tx ["ASSERT 1 = (SELECT COUNT(*) FROM foo)"])
                              (jdbc/execute! tx ["INSERT INTO foo (_id) VALUES (2)"]))))

    (t/is (= [{:row_count 2}]
             (q conn ["SELECT COUNT(*) row_count FROM foo"])))

    (t/is (= [{:_id 2, :committed false,
               :error {"error-key" "xtdb/assert-failed", "message" "Assert failed"}}
              {:_id 1, :committed true, :error nil}
              {:_id 0, :committed true, :error nil}]
             (q conn ["SELECT * EXCLUDE system_time FROM xt.txs"])))))

(deftest test-untyped-null-type
  (with-open [conn (jdbc-conn "prepareThreshold" -1)
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
  (with-open [conn (jdbc-conn "prepareThreshold" -1)
              stmt (.prepareStatement conn "SELECT ?")]

    (t/testing "typed null param"

      (.setObject stmt 1 nil Types/INTEGER)

      (t/is (= ["int4"] (param-metadata stmt)))

      (with-open [rs (.executeQuery stmt)]

        (t/is (= [{"_column_1" "text"}] (result-metadata stmt) (result-metadata rs))
              "untyped nulls are of type text in result sets")
        ;;the reason the above null is untyped and rather than int4 is that during bind
        ;;we prefer to use the type of the arg itself (in this case nil which is simply of type nil
        ;;in xt) rather than force it to be the type originally specified as part of query prep
        ;;
        ;;could be a mistake, postgres would almost certainly describe this as an int4 in the result set

        (t/is (= [{"_column_1" nil}] (rs->maps rs)))))))

(deftest test-prepared-statements-with-unknown-params
  (with-open [conn (jdbc-conn "prepareThreshold" -1)
              stmt (.prepareStatement conn "SELECT ?")]

    (t/testing "unknown param types are assumed to be of type text"

      (t/is (= ["text"] (param-metadata stmt))))

    (t/testing "Once a param value is supplied statement updates correctly and can be executed"

      (.setObject stmt 1 #uuid "7dd2ed62-bb05-43c8-b289-5503d9b19ee6")

      (t/is (= ["uuid"] (param-metadata stmt)))

      (with-open [rs (.executeQuery stmt)]

        (t/is (= [{"_column_1" "uuid"}] (result-metadata stmt) (result-metadata rs)))
        (t/is (= [{"_column_1" #uuid "7dd2ed62-bb05-43c8-b289-5503d9b19ee6"}] (rs->maps rs)))))))

(deftest test-prepared-statments
  (with-open [conn (jdbc-conn "prepareThreshold" -1)]
    (.execute (.prepareStatement conn "INSERT INTO foo(_id, a, b) VALUES (1, 'one', 2)"))
    (with-open [stmt (.prepareStatement conn "SELECT foo.*, ? FROM foo")]


      (t/testing "server side prepared statments where param types are known"

        (.setObject stmt 1 true Types/BOOLEAN)

        (with-open [rs (.executeQuery stmt)]

          (t/is (= [{"_id" "int8"} {"a" "text"} {"b" "int8"} {"_column_2" "bool"}]
                   (result-metadata stmt)
                   (result-metadata rs)))


          (t/is (=
                 [{"_id" 1, "a" "one", "b" 2, "_column_2" true}]
                 (rs->maps rs)))))

      (t/testing "param types can be rebound for the same prepared statment"

        (.setObject stmt 1 44.4 Types/DOUBLE)

        (with-open [rs (.executeQuery stmt)]

          (t/is (= [{"_id" "int8"} {"a" "text"} {"b" "int8"} {"_column_2" "float8"}]
                   (result-metadata stmt)
                   (result-metadata rs)))

          (t/is (=
                 [{"_id" 1, "a" "one", "b" 2, "_column_2" 44.4}]
                 (rs->maps rs)))))

      (t/testing "relevant schema change reported to pgwire client"

        (.execute (.prepareStatement conn "INSERT INTO foo(_id, a, b) VALUES (2, 1, 1)"))

        ;;TODO we just return our own custom error here, rather than 'cached plan must not change result type'
        ;;might be worth confirming if there is a specific error type/message that matters for pgjdbc as there
        ;;appears to be retry logic built into the driver
        ;;https://jdbc.postgresql.org/documentation/server-prepare/#re-execution-of-failed-statements
        (t/is (thrown-with-msg?
               PSQLException
               #"ERROR: Relevant table schema has changed since preparing query, please prepare again"
               (with-open [_rs (.executeQuery stmt)])))))))

(deftest test-nulls-in-monomorphic-types
  (with-open [conn (jdbc-conn "prepareThreshold" -1)]
    (.execute (.prepareStatement conn "INSERT INTO foo(_id, a) VALUES (1, NULL), (2, 22.2)"))
    (with-open [stmt (.prepareStatement conn "SELECT a FROM foo ORDER BY _id")]

      (with-open [rs (.executeQuery stmt)]

        (t/is (= [{"a" "float8"}]
                 (result-metadata stmt)
                 (result-metadata rs)))

        (t/is (= [{"a" nil} {"a" 22.2}]
                 (rs->maps rs)))))))

(deftest test-nulls-in-polymorphic-types
  (with-open [conn (jdbc-conn "prepareThreshold" -1)]
    (.execute (.prepareStatement conn "INSERT INTO foo(_id, a) VALUES (1, 'one'), (2, 1), (3, NULL)"))
    (with-open [stmt (.prepareStatement conn "SELECT a FROM foo ORDER BY _id")]

      (with-open [rs (.executeQuery stmt)]

        (t/is (= [{"a" "json"}]
                 (result-metadata stmt)
                 (result-metadata rs)))

        (let [[x y z :as res] (mapv #(get % "a") (rs->maps rs))]

          (t/is (= 3 (count res)))

          (t/is (= "\"one\"" (.getValue x)))
          (t/is (= "1" (.getValue y)))
          (t/is (nil? z)))))))

(deftest test-datetime-types
  (doseq [{:keys [^Class type val pg-type]} [{:type LocalDate :val #xt.time/date "2018-07-25" :pg-type "date"}
                                             {:type LocalDate :val #xt.time/date "1239-01-24" :pg-type "date"}
                                             {:type LocalDateTime :val #xt.time/date-time "2024-07-03T19:01:34.123456" :pg-type "timestamp"}
                                             {:type LocalDateTime :val #xt.time/date-time "1742-07-03T04:22:59" :pg-type "timestamp"}]
          binary? [true false]]

    (t/testing (format "binary?: %s, type: %s, pg-type: %s, val: %s" binary? type pg-type val)

      (with-open [conn (jdbc-conn "prepareThreshold" -1 "binaryTransfer" binary?)
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
        val #xt.time/offset-date-time "2024-07-03T19:01:34-07:00"
        val2 #xt.time/offset-date-time "2024-07-03T19:01:34.695959+03:00"]
    (doseq [binary? [true false]]

      (with-open [conn (jdbc-conn "prepareThreshold" -1 "binaryTransfer" binary?)
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
                 #xt.time/offset-date-time "2099-04-15T11:40:31Z"
                 (.getObject rs 1 type))
                "ZonedDateTimes can be read even if not written"))))))

(deftest test-tstz-range-3652
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 1, v: 1}"])
    (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 1, v: 2}"])

    (t/is (= [{:_id 1, :v 2,
               :_valid_time {:type "tstz-range", :value "[2020-01-02 00:00:00+00:00,)"}}
              {:_id 1, :v 1,
               :_valid_time {:type "tstz-range", :value "[2020-01-01 00:00:00+00:00,2020-01-02 00:00:00+00:00)"}}]
             (->> (jdbc/execute! conn ["SELECT *, _valid_time FROM foo FOR ALL VALID_TIME"])
                  (mapv #(update % :_valid_time (fn [^PGobject vt]
                                                  {:type (.getType vt)
                                                   :value (.getValue vt)})))))))

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
             ;;note nanosecond timestamp is returned as json
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
                      "\"2022-08-16T14:29:03.123456789\""]]
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
                      "\"2022-08-16T11:08:03.123456789\""]]
                    (read))))))))

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
             PGError #"cannot text-encode a value: 1, OID: TEXT, type: java.lang.Long"
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


      (t/is (thrown-with-msg?
             PGErrorResponse
             #"Missing types for params - Client must specify types for all params in DML statements"
             (pg/execute conn "INSERT INTO foo(_id, v) VALUES (1, $1)" {:params ["1"]
                                                                          :oids [OID/DEFAULT]}))
            "params declared with the default oid (0) by clients are
             treated as unspecified and therefore also error"))))

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

(deftest test-java-sql-timestamp
  (testing "java.sql.Timestamp"
    (with-open [conn (jdbc-conn "prepareThreshold" -1)
                stmt (.prepareStatement conn "SELECT ? AS v")]

      (.setTimestamp
       stmt
       1
       (Timestamp/from #xt.time/instant "2030-01-04T12:44:55Z")
       (Calendar/getInstance (TimeZone/getTimeZone "GMT")))

      (with-open [rs (.executeQuery stmt)]

        ;;treated as text because pgjdbc sets the param oid to 0/unspecified
        ;;will error in DML
        ;;https://github.com/pgjdbc/pgjdbc/blob/84bdec6015472f309de7becf41df7fd8e423d0ac/pgjdbc/src/main/java/org/postgresql/jdbc/PgPreparedStatement.java#L1454
        (t/is (= [{"v" "text"}]
                 (result-metadata stmt)
                 (result-metadata rs)))

        (t/is (= [{"v" "2030-01-04 12:44:55+00"}] (rs->maps rs)))))))

(deftest test-java-time-instant
  (with-open [conn (pg-conn {})]

    (t/is (= [{:v #xt.time/date-time "2030-01-04T12:44:55"}]
             (pg/execute conn "SELECT $1 v" {:params [#xt.time/instant "2030-01-04T12:44:55Z"]
                                             :oids [OID/TIMESTAMP]}))
          "when reading param, zone is ignored and instant is treated as a timestamp")

    (t/is (= [{:v #xt.time/offset-date-time "2030-01-04T12:44:55Z"}]
             (pg/execute conn "SELECT $1 v" {:params [#xt.time/instant "2030-01-04T12:44:55Z"]
                                             :oids [OID/TIMESTAMPTZ]}))
          "when reading param, zone is honored (UTC) and instant is treated as a timestamptz")))

(deftest test-declared-param-types-are-honored
  (t/testing "Declared param type VARCHAR is honored, even though it maps to utf8 which XTDB maps to text"
    ;;case applies anytime we have multiple pg types that map to the same xt type, as we choose a single pg type
    ;;for the return type.
    (with-open [conn (jdbc-conn "prepareThreshold" -1)
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

(deftest test-show-latest-submitted-tx
  (with-open [conn (jdbc-conn)]
    (t/is (= [] (q conn ["SHOW LATEST SUBMITTED TRANSACTION"])))

    (jdbc/execute! conn ["INSERT INTO foo (_id) VALUES (1)"])

    (t/is (= [{:tx_id 0, :system_time #inst "2020-01-01"}]
             (q conn ["SHOW LATEST SUBMITTED TRANSACTION"])))

    (jdbc/execute! conn ["INSERT INTO foo (_id) VALUES (2)"])

    (t/is (= [{:tx_id 1, :system_time #inst "2020-01-02"}]
             (q conn ["SHOW LATEST SUBMITTED TRANSACTION"])))))

(t/deftest test-psql-bind-3572
  #_ ; FIXME #3622
  (when (psql-available?)
    (psql-session
     (fn [send read]
       (send "INSERT INTO tbl1 (_id, foo) VALUES ($1, $2) \\bind 'a' 'b' \\g")
       (read)
       (send "SELECT * FROM tbl1 WHERE _id = $1 \\bind 'a' \\g")
       (t/is (= [["_id" "foo"] ["a" "b"]] (read)))))))

(deftest test-cast-text-data-type
  (with-open [conn (pg-conn {})]
    (t/is (= [{:v "101"}]
             (pg/execute conn "SELECT 101::text v")))))

(deftest test-resolve-result-format
  (letfn [(resolve-result-format [fmt type-count]
            (some->> (pgwire/with-result-formats (repeat type-count {}) fmt)
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

    (t/is (nil? (resolve-result-format [:text :binary] 3))
          "if more than 1 format is provided and it doesn't match the field count this is invalid")))

(deftest test-pg-boolean-param
  (doseq [binary? [true false]
          v [true false]]

    (t/testing (format "binary?: %s, value?: %s" binary? v)

      (with-open [conn (pg-conn {:binary-encode? binary? :binary-decode? binary?})]

        (t/is (= [{:v v}]
                 (pg/execute conn "SELECT ? v" {:oids [OID/BOOL]
                                                :params [v]})))))))

(deftest test-pgjdbc-boolean-param
  (doseq [binary? [true false]
          v [true false]]
    ;;pgjdbc doesn't actually send or recieve boolean in binary format
    ;;but it claims to and might someday
    (t/testing (format "binary?: %s, value?: %s" binary? v)

      (with-open [conn (jdbc-conn "prepareThreshold" -1 "binaryTransfer" binary?)
                  stmt (.prepareStatement conn "SELECT ? AS v")]

        (.setBoolean stmt 1 v)

        (with-open [rs (.executeQuery stmt)]

          (t/is (= [{"v" "bool"}]
                   (result-metadata stmt)
                   (result-metadata rs)))

          (.next rs)
          (t/is (= v (.getBoolean rs 1))))))))

(deftest test-transit-param
  (with-open [node (xtn/start-node {:server {:port 0}})]
    (t/testing "pgwire metadata query"
      (t/is (= [{:oid 16384, :typname "transit"}]
               (xt/q node "
SELECT t.oid, t.typname
FROM pg_catalog.pg_type t
  JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
WHERE t.typname = $1 AND (n.nspname = $2 OR $3 AND n.nspname = ANY (current_schemas(true)))
ORDER BY t.oid DESC LIMIT 1"
                           {:args ["transit" nil true]})))))

  (with-open [conn (jdbc-conn "prepareThreshold" -1)]
    (jdbc/execute! conn ["INSERT INTO foo (_id, v) VALUES (1, ?)" (pgwire/transit->pgobject {:a 1, :b 2})])

    (with-open [stmt (.prepareStatement conn "SELECT v FROM foo")
                rs (.executeQuery stmt)]

      (t/is (= [{"v" "json"}]
               (result-metadata stmt)
               (result-metadata rs)))

      (.next rs)
      (t/is (= "{\"a\":1,\"b\":2}"
               (.getValue ^PGobject (.getObject rs 1)))))))

(deftest test-fallback-transit
  (with-open [conn (jdbc-conn "options" "-c fallback_output_format=transit")]
    (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 1, nest: {ts: TIMESTAMP '2020-01-01T00:00:00Z'}}"])

    (with-open [stmt (.prepareStatement conn "SELECT * FROM foo")]
      (t/is (= [{"_id" "int8"} {"nest" "transit"}] (result-metadata stmt))))

    (t/is (= {:_id 1,
              :nest {"ts" #xt.time/zoned-date-time "2020-01-01T00:00Z"}}

             (-> (jdbc/execute-one! conn ["SELECT * FROM foo"])
                 (update :nest (fn [^PGobject nest]
                                 (serde/read-transit (.getBytes (.getValue nest)) :json))))))))

(deftest insert-select-test-3684
  (with-open [conn (jdbc-conn)]
    (jdbc/execute! conn ["INSERT INTO docs (_id) VALUES (1), (2)"])
    (jdbc/execute! conn ["INSERT INTO docs (SELECT *, 'hi' AS foo FROM docs WHERE _id = 1)"])

    (t/is (= #{{:_id 1, :foo "hi"} {:_id 2, :foo nil}}
             (set (q conn ["SELECT * FROM docs"]))))))

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
  (with-open [conn (jdbc-conn)
              stmt (.prepareStatement conn "SELECT 2 AS b FROM docs GROUP BY b")]
    (.execute stmt)

    (t/is (= #{"Table not found: docs" "Column not found: b"}
             (set (map #(.getMessage ^SQLWarning %) (stmt->warnings stmt))))))

  (let [warns (atom [])]
    (with-open [conn (pg-conn {:fn-notice (fn [notice] (swap! warns conj (:message notice)))})]
      (pg/execute conn "SELECT 2 AS b FROM docs GROUP BY b")
      ;; the fn-notice runs in a separate executor pool
      (Thread/sleep 100)
      (t/is (= #{"Table not found: docs" "Column not found: b"}
               (set @warns))))))

(deftest test-ignore-returning-keys-3668
  (with-open [conn (jdbc-conn)
              stmt (.prepareStatement conn "INSERT INTO people (_id, name) VALUES (6, 'fred')" Statement/RETURN_GENERATED_KEYS)]
    (t/is (false?  (.execute stmt)))
    (let [rs (.getGeneratedKeys stmt)]
      (t/is (= [] (rs->maps rs))))))

(deftest test-interval-encoding-3697
  (t/testing "Intervals"
    (with-open [conn (jdbc-conn)]
      (t/is (= [{:i (PGInterval. "P1DT1H1M1.111111S")}]
               (jdbc/execute! conn ["SELECT INTERVAL 'P1DT1H1M1.111111111S' AS i"])))

      (t/is (= [{:i (PGInterval. "P12MT0S")}]
               (jdbc/execute! conn ["SELECT INTERVAL 'P12MT0S' AS i"])))

      (t/is (= [{:i (PGInterval. "P-22MT0S")}]
               (jdbc/execute! conn ["SELECT INTERVAL 'P-22MT0S' AS i"]))))))

(deftest test-playground
  (with-open [srv (pgwire/open-playground)]
    (let [{:keys [port]} srv]
      (letfn [(pg-conn [db]
                {:dbtype "postgresql"
                 :host "localhost"
                 :port port
                 :dbname db
                 :options "-c fallback_output_format=transit"})]
        (with-open [conn1a (jdbc/get-connection (pg-conn "foo1"))
                    conn1b (jdbc/get-connection (pg-conn "foo1"))
                    conn2 (jdbc/get-connection (pg-conn "foo2"))]

          (jdbc/execute! conn1a ["INSERT INTO foo RECORDS {_id: 1}"])
          (jdbc/execute! conn1b ["INSERT INTO bar RECORDS {_id: 1}"])
          (jdbc/execute! conn2 ["INSERT INTO foo RECORDS {_id: 2}"])

          (t/is (= [{:_id 1}] (jdbc/execute! conn1a ["SELECT * FROM bar"])))
          (t/is (= [{:_id 1}] (jdbc/execute! conn1b ["SELECT * FROM foo"])))
          (t/is (= [{:_id 2}] (jdbc/execute! conn2 ["SELECT * FROM foo"]))))))))

(deftest test-time
  (with-open [conn (pg-conn {})]

    (t/is (=
           [{:v "20:40:31.932254"}]
           (pg/execute conn "SELECT TIME '20:40:31.932254' v"))
          "time is returned as json")))

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
