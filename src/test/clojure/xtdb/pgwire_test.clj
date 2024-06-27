(ns xtdb.pgwire-test
  (:require [clojure.data.json :as json]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing] :as t]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as result-set]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.pgwire :as pgwire]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (com.fasterxml.jackson.databind JsonNode ObjectMapper)
           (com.fasterxml.jackson.databind.node JsonNodeType)
           (java.io InputStream)
           (java.lang Thread$State)
           (java.sql Connection Types JDBCType)
           (java.time Clock Instant ZoneId ZoneOffset)
           (java.util.concurrent CountDownLatch TimeUnit)
           java.util.List
           (org.postgresql.util PGobject PSQLException)
           (xtdb JsonSerde)))

(set! *warn-on-reflection* false)
(set! *unchecked-math* false)

(def ^:dynamic ^:private *port* nil)
(def ^:dynamic ^:private *node* nil)
(def ^:dynamic ^:private *server* nil)

(defn require-node []
  (when-not *node*
    (set! *node* (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]}))))

(defn require-server
  ([] (require-server {}))
  ([opts]
   (require-node)
   (when-not *port*
     (set! *server* (->> (merge {:num-threads 1}
                                opts
                                {:port 0})
                         (pgwire/serve *node*)))
     (set! *port* (:port *server*)))))

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

(t/use-fixtures :each
  (fn [f]
    (binding [*port* nil
              *server* nil
              *node* nil]
      (try
        (f)
        (finally
          (util/try-close *server*)
          (util/try-close *node*))))))

(defn- jdbc-url [& params]
  (when-not *port*
    (require-server))

  (let [param-str (when (seq params) (str "?" (str/join "&" (for [[k v] (partition 2 params)] (str k "=" v)))))]
    (format "jdbc:postgresql://localhost:%s/xtdb%s" *port* (or param-str ""))))

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

(defn rs->maps [rs]
  (let [md (.getMetaData rs)]
    (-> (loop [res []]
          (if (.next rs)
            (recur (->>
                    (for [idx (range 1 (inc (.getColumnCount md)))]
                      {(.getColumnName md idx) (.getObject rs idx)})
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

(deftest ssl-test
  (t/are [sslmode expect] (= expect (try-sslmode sslmode))
    "disable" :ok
    "allow" :ok
    "prefer" :ok

    "require" :unsupported
    "verify-ca" :unsupported
    "verify-full" :unsupported))

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

(defn- jdbc-conn ^Connection [& params]
  (jdbc/get-connection (apply jdbc-url params)))

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
  :json-type the expected Jackson JsonNodeType

  :json (optional) a json string that we expect back from pgwire
  :clj (optional) a clj value that we expect from clojure.data.json/read-str
  :clj-pred (optional) a fn that returns true if the parsed arg (via data.json/read-str) is what we expect"
  (letfn [(string [s]
            {:sql (str "'" s "'")
             :json-type JsonNodeType/STRING
             :clj s})
          (integer [i]
            {:sql (str i)
             :json-type JsonNodeType/NUMBER
             :clj-pred #(= (bigint %) (bigint i))})
          (decimal [n & {:keys [add-zero]}]
            (let [d1 (bigdec n)
                  d2 (if add-zero (.setScale d1 1) d1)]
              {:sql (.toPlainString d2)
               :json-type JsonNodeType/NUMBER
               :json (str d1)
               :clj-pred #(= (bigdec %) (bigdec n))}))]

    [{:sql "null"
      :json-type JsonNodeType/NULL
      :clj nil}

     {:sql "true"
      :json-type JsonNodeType/BOOLEAN
      :clj true}

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

     {:sql "DATE '2021-12-24'"
      :json-type JsonNodeType/STRING
      :clj "2021-12-24"}
     {:sql "TIMESTAMP '2021-03-04 03:04:11'"
      :json-type JsonNodeType/STRING
      :clj "2021-03-04T03:04:11"}
     {:sql "TIMESTAMP '2021-03-04 03:04:11+02:00'"
      :json-type JsonNodeType/STRING
      :clj "2021-03-04T03:04:11+02:00"}
     {:sql "TIMESTAMP '2021-12-24 11:23:44.003'"
      :json-type JsonNodeType/STRING
      :clj "2021-12-24T11:23:44.003"}

     {:sql "INTERVAL '1' YEAR"
      :json-type JsonNodeType/STRING
      :clj "P12M"}
     {:sql "INTERVAL '1' MONTH"
      :json-type JsonNodeType/STRING
      :clj "P1M"}

     {:sql "DATE '2021-12-24' - DATE '2021-12-23'"
      :json-type JsonNodeType/NUMBER
      :clj 1}

     ;; arrays

     {:sql "ARRAY []"
      :json-type JsonNodeType/ARRAY
      :clj []}

     {:sql "ARRAY [42]"
      :json-type JsonNodeType/ARRAY
      :clj [42]}

     {:sql "ARRAY ['2022-01-02']"
      :json-type JsonNodeType/ARRAY
      :json "[\"2022-01-02\"]"
      :clj ["2022-01-02"]}

     {:sql "ARRAY [ARRAY ['42'], 42, '42']"
      :json-type JsonNodeType/ARRAY
      :clj [["42"] 42 "42"]}]))

(deftest json-representation-test
  (with-open [conn (jdbc-conn)]
    (doseq [{:keys [json-type, json, sql, clj, clj-pred] :as example} json-representation-examples]
      (testing (str "SQL expression " sql " should parse to " clj " (" (when json (str json ", ")) json-type ")")
        (with-open [stmt (.prepareStatement conn (format "SELECT a FROM (VALUES (%s), (ARRAY [])) a (a)" sql))]
          ;; empty array to force polymoprhic/json return
          (with-open [rs (.executeQuery stmt)]
            ;; one row in result set
            (.next rs)

            (testing "record set contains expected object"
              (is (instance? PGobject (.getObject rs 1)))
              (is (= "json" (.getType ^PGobject (.getObject rs 1)))))

            (testing (str "json parses to " (str json-type))
              (let [obj-mapper (ObjectMapper.)
                    json-str (str (.getObject rs 1))
                    ^JsonNode read-value (.readValue obj-mapper json-str ^Class JsonNode)]
                ;; use strings to get a better report
                (is (= (str json-type) (str (.getNodeType read-value))))
                (when json
                  (is (= json json-str) "json string should be = to :json"))))

            (testing "json parses to expected clj value"
              (let [clj-value (json/read-str (str (.getObject rs 1)))]
                (when (contains? example :clj)
                  (is (= clj clj-value) "parsed value should = :clj"))
                (when clj-pred
                  (is (clj-pred clj-value) "parsed value should pass :clj-pred"))))))))))

(defn check-server-resources-freed
  ([]
   (require-server)
   (check-server-resources-freed *server*))
  ([server]
   (testing "accept socket"
     (is (.isClosed (:accept-socket server))))

   (testing "accept thread"
     (is (= Thread$State/TERMINATED (.getState (:accept-thread server)))))

   (testing "thread pool shutdown"
     (is (.isShutdown (:thread-pool server)))
     (is (.isTerminated (:thread-pool server))))))

(deftest server-resources-freed-on-close-test
  (require-node)
  (doseq [close-method [#(.close %)]]
    (with-open [server (pgwire/serve *node* {:port 0})]
      (close-method server)
      (check-server-resources-freed server))))

(defn <-pgobject
  "Transform PGobject containing `json` or `jsonb` value to Clojure
  data."
  [^org.postgresql.util.PGobject v]
  (let [type  (.getType v)
        value (.getValue v)]
    (if (#{"jsonb" "json"} type)
      (when value
        (json/read-str value))
      value)))

(defn q [conn sql]
  (->> (jdbc/execute! conn sql)
       (mapv (fn [row]
               (update-vals row
                            (fn [v]
                              (if (instance? org.postgresql.util.PGobject v)
                                (<-pgobject v)
                                v)))))))

(defn q-seq [conn sql]
  (->> (jdbc/execute! conn sql {:builder-fn result-set/as-arrays})
       (rest)))

(defn ping [conn]
  (-> (q conn ["select 'ping' ping"])
      first
      :ping))

(deftest accept-thread-interrupt-closes-thread-test
  (require-server {:accept-so-timeout 10})

  (.interrupt (:accept-thread *server*))
  (.join (:accept-thread *server*) 1000)

  (is (= Thread$State/TERMINATED (.getState (:accept-thread *server*)))))

(deftest accept-thread-interrupt-allows-server-shutdown-test
  (require-server {:accept-so-timeout 10})

  (.interrupt (:accept-thread *server*))
  (.join (:accept-thread *server*) 1000)

  (.close *server*)
  (check-server-resources-freed))

(deftest accept-thread-socket-close-stops-thread-test
  (require-server)
  (.close (:accept-socket *server*))
  (.join (:accept-thread *server*) 1000)
  (is (= Thread$State/TERMINATED (.getState (:accept-thread *server*)))))

(deftest accept-thread-socket-close-allows-cleanup-test
  (require-server)
  (.close (:accept-socket *server*))
  (.join (:accept-thread *server*) 1000)
  (.close *server*)
  (check-server-resources-freed))

(defn- get-connections []
  (vals (:connections @(:server-state *server*))))

(defn- get-last-conn []
  (last (sort-by :cid (get-connections))))

(defn- wait-for-close [server-conn ms]
  (deref (:close-promise @(:conn-state server-conn)) ms false))

(defn check-conn-resources-freed [server-conn]
  (let [{:keys [socket]} server-conn]
    (t/is (.isClosed socket))))

(deftest conn-force-closed-by-server-frees-resources-test
  (require-server)
  (with-open [_ (jdbc-conn)]
    (let [srv-conn (get-last-conn)]
      (.close srv-conn)
      (check-conn-resources-freed srv-conn))))

(deftest conn-closed-by-client-frees-resources-test
  (require-server)
  (with-open [client-conn (jdbc-conn)
              server-conn (get-last-conn)]
    (.close client-conn)
    (is (wait-for-close server-conn 500))
    (check-conn-resources-freed server-conn)))

(deftest server-close-closes-idle-conns-test
  (require-server {:drain-wait 0})
  (with-open [_client-conn (jdbc-conn)
              server-conn (get-last-conn)]
    (.close *server*)
    (is (wait-for-close server-conn 500))
    (check-conn-resources-freed server-conn)))

(deftest canned-response-test
  (require-server)
  ;; quick test for now to confirm canned response mechanism at least doesn't crash!
  ;; this may later be replaced by client driver tests (e.g test sqlalchemy connect & query)
  (with-redefs [pgwire/canned-responses [{:q "hello!"
                                          :cols [{:column-name "greet", :column-oid @#'pgwire/oid-json}]
                                          :rows [["\"hey!\""]]}]]
    (with-open [conn (jdbc-conn)]
      (is (= [{:greet "hey!"}] (q conn ["hello!"]))))))

(deftest concurrent-conns-test
  (require-server {:num-threads 2})
  (let [results (atom [])
        spawn (fn spawn []
                (future
                  (with-open [conn (jdbc-conn)]
                    (swap! results conj (ping conn)))))
        futs (vec (repeatedly 10 spawn))]

    (is (every? #(not= :timeout (deref % 500 :timeout)) futs))
    (is (= 10 (count @results)))

    (.close *server*)
    (check-server-resources-freed)))

(deftest concurrent-conns-close-midway-test
  (require-server {:num-threads 2
                   :accept-so-timeout 10})
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

      (.close *server*)

      (is (every? #(not= :timeout (deref % 1000 :timeout)) futs))

      (check-server-resources-freed))))

;; the goal of this test is to cause a bunch of ping queries to block on parse
;; until the server is draining
;; and observe that connection continue until the multi-message extended interaction is done
;; (when we introduce read transactions I will probably extend this to short-lived transactions)
(deftest close-drains-active-extended-queries-before-stopping-test
  (require-server {:num-threads 10})
  (let [cmd-parse @#'pgwire/cmd-parse
        {:keys [!closing?]} *server*
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

        (util/close *server*)

        (is (every? #(= "ping" (deref % 1000 :timeout)) futs))

        (check-server-resources-freed)))))

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
  (require-server)
  ;; there are other ways to do this, but its a straightforward factoring that removes some boilerplate for now.
  (let [^List argv ["psql" "-h" "localhost" "-p" (str *port*)]
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
                           read-str))))

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
    (require-server)
    (let [{:keys [exit, out]} (sh/sh "psql" "-h" "localhost" "-p" (str *port*) "-c" "select 'ping' ping")]
      (is (= 0 exit))
      (is (str/includes? out " ping\n(1 row)")))))

(when (psql-available?)
  (deftest psql-interactive-test
    (psql-session
     (fn [send read]
       (testing "ping"
         (send "select 'ping';\n")
         (let [s (read)]
           (is (str/includes? s "ping"))
           (is (str/includes? s "(1 row)"))))

       (testing "numeric printing"
         (send "select a.a from (values (42)) a (a);\n")
         (is (str/includes? (read) "42")))

       (testing "expecting column name"
         (send "select a.flibble from (values (42)) a (flibble);\n")
         (is (str/includes? (read) "flibble")))

       (testing "mixed type col"
         (send "select a.a from (values (42), ('hello!'), (array [1,2,3])) a (a);\n")
         (let [s (read)]
           (is (str/includes? s "42"))
           (is (str/includes? s "\"hello!\""))
           (is (str/includes? s "[1,2,3]"))
           (is (str/includes? s "(3 rows)"))))

       (testing "error during plan"
         (with-redefs [clojure.tools.logging/logf (constantly nil)]
           (send "slect baz.a from baz;\n")
           (is (str/includes? (read :err) "mismatched input 'slect' expecting")))

         (testing "query error allows session to continue"
           (send "select 'ping';\n")
           (is (str/includes? (read) "ping"))))

       (testing "error during query execution"
         (with-redefs [clojure.tools.logging/logf (constantly nil)]
           (send "select (1 / 0) from (values (42)) a (a);\n")
           (is (str/includes? (read :err) "data exception — division by zero")))

         (testing "query error allows session to continue"
           (send "select 'ping';\n")
           (is (str/includes? (read) "ping"))))))))


;; maps cannot be created from SQL yet, or used as parameters - but we can read them from XT.
(deftest map-read-test
  (with-open [conn (jdbc-conn)]
    (-> (xt/submit-tx *node* [[:put-docs :a {:xt/id "map-test", :a {:b 42}}]])
        (tu/then-await-tx *node*))

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
  (require-node)
  (let [insert #(xt/submit-tx *node* [[:put-docs %1 %2]])]
    (-> (insert :a {:xt/id :fred, :name "Fred"})
        (tu/then-await-tx *node*))

    (with-open [conn (jdbc-conn)]
      (jdbc/with-transaction [db conn]
        (is (= [{:name "Fred"}] (q db ["select a.name from a"])))
        (insert :a {:xt/id :bob, :name "Bob"})
        (is (= [{:name "Fred"}] (q db ["select a.name from a"]))))

      (is (= #{{:name "Fred"}, {:name "Bob"}} (set (q conn ["select a.name from a"])))))))

;; SET is not supported properly at the moment, so this ensure we do not really do anything too embarrassing (like crash)
(deftest set-statement-test
  (let [params #(-> (get-last-conn) :conn-state deref :session :parameters (select-keys %))]
    (with-open [conn (jdbc-conn)]

      (testing "literals saved as is"
        (is (q conn ["SET a = 'b'"]))
        (is (= {:a "b"} (params [:a])))
        (is (q conn ["SET b = 42"]))
        (is (= {:a "b", :b 42} (params [:a :b]))))

      (testing "properties can be overwritten"
        (q conn ["SET a = 42"])
        (is (= {:a 42} (params [:a]))))

      (testing "TO syntax can be used"
        (q conn ["SET a TO 43"])
        (is (= {:a 43} (params [:a])))))))

(deftest db-queryable-after-transaction-error-test
  (with-open [conn (jdbc-conn)]
    (try
      (jdbc/with-transaction [db conn]
        (is (= [] (q db ["SELECT * WHERE FALSE"])))
        (throw (Exception. "Oh no!")))
      (catch Throwable _))
    (is (= [] (q conn ["SELECT * WHERE FALSE"])))))

(deftest transactions-are-read-only-by-default-test
  (with-open [conn (jdbc-conn)]
    (is (thrown-with-msg?
          PSQLException #"ERROR\: DML is not allowed in a READ ONLY transaction"
          (jdbc/with-transaction [db conn] (q db ["insert into foo(_id) values (42)"]))))
    (is (= [] (q conn ["SELECT * WHERE FALSE"])))))

(defn- session-variables [server-conn ks]
  (-> server-conn :conn-state deref :session (select-keys ks)))

(defn- next-transaction-variables [server-conn ks]
  (-> server-conn :conn-state deref :session :next-transaction (select-keys ks)))

(deftest session-access-mode-default-test
  (require-node)
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
      (is (thrown-with-msg? PSQLException #"queries are unsupported in a READ WRITE transaction"
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
         (is (str/includes? (read) "SET TRANSACTION")))

       (testing "begin"
         (send "BEGIN;\n")
         (is (str/includes? (read) "BEGIN")))

       (testing "insert"
         (send "INSERT INTO foo (_id, a) values (42, 42);\n")
         (is (str/includes? (read) "INSERT 0 0")))

       (testing "insert 2"
         (send "INSERT INTO foo (_id, a) values (366, 366);\n")
         (is (str/includes? (read) "INSERT 0 0")))

       (testing "commit"
         (send "COMMIT;\n")
         (is (str/includes? (read) "COMMIT")))

       (testing "read your own writes"
         (send "SELECT a FROM foo;\n")
         (let [s (read)]
           (is (str/includes? s "42"))
           (is (str/includes? s "366"))))

       (testing "delete"
         (send "BEGIN READ WRITE;\n")
         (read)
         (send "DELETE FROM foo;\n")
         (let [s (read)]
           (is (str/includes? s "DELETE 0"))
           (testing "no description sent"
             (is (not (str/includes? s "_iid"))))))))))

(when (psql-available?)
  (deftest psql-dml-at-prompt-test
    (psql-session
     (fn [send read]
       (send "INSERT INTO foo(_id, a) VALUES (42, 42);\n")
       (is (str/includes? (read) "INSERT 0 0"))))))

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

(defn- current-ts [conn]
  (:a (first (q conn ["select current_timestamp a"]))))

(deftest half-baked-clock-test
  ;; goal is to test time basis params are sent with queries
  ;; no support yet for SET/SHOW TIME ZONE so testing with internals to
  ;; provoke some clock specialised behaviour and make sure something not totally wrong happens
  (require-server)
  (let [custom-clock (Clock/fixed (Instant/parse "2022-08-16T11:08:03Z") (ZoneOffset/ofHoursMinutes 3 12))]

    (swap! (:server-state *server*) assoc :clock custom-clock)

    (testing "server zone is inherited by conn session"
      (with-open [conn (jdbc-conn)]
        (is (= {:clock custom-clock} (session-variables (get-last-conn) [:clock])))
        (is (= "2022-08-16T14:20:03+03:12" (current-ts conn)))))

    (swap! (:server-state *server*) assoc :clock (Clock/systemDefaultZone))

    ;; I am not 100% this is the behaviour we actually want as it stands
    ;; going off repeatable-read steer from ADR-40
    (testing "current ts instant is pinned during a tx, regardless of what happens to the session clock"
      (with-open [conn (jdbc-conn)]
        (let [{:keys [conn-state]} (get-last-conn)]
          (swap! conn-state assoc-in [:session :clock] (Clock/fixed Instant/EPOCH ZoneOffset/UTC)))

        (jdbc/with-transaction [tx conn]
          (is (= "1970-01-01T00:00Z" (current-ts tx)))
          (Thread/sleep 10)
          (is (= "1970-01-01T00:00Z" (current-ts tx)))
          (testing "inside a transaction, changing the zone is permitted, but the instant is fixed, regardless of the session clock"
            (let [{:keys [conn-state]} (get-last-conn)]
              (swap! conn-state assoc-in [:session :clock] custom-clock))
            (is (= "1970-01-01T03:12+03:12" (current-ts tx)))))

        (is (= "2022-08-16T14:20:03+03:12" (current-ts conn)))))))

(deftest set-time-zone-test
  (require-server {:num-threads 2})
  (let [server-clock (Clock/fixed (Instant/parse "2022-08-16T11:08:03Z") (ZoneOffset/ofHoursMinutes 3 12))]
    (swap! (:server-state *server*) assoc :clock server-clock)
    (with-open [conn (jdbc-conn)]
      ;; for sanity
      (testing "expect server clock"
        (is (= "2022-08-16T14:20:03+03:12" (current-ts conn))))

      (testing "utc"
        (q conn ["SET TIME ZONE '+00:00'"])
        (is (= "2022-08-16T11:08:03Z" (current-ts conn))))

      (testing "tz is session scoped"
        (with-open [conn2 (jdbc-conn)]
          (is (= "2022-08-16T14:20:03+03:12" (current-ts conn2)))
          (is (= "2022-08-16T11:08:03Z" (current-ts conn)))
          (q conn2 ["SET TIME ZONE '+00:01'"])
          (is (= "2022-08-16T11:09:03+00:01" (current-ts conn2)))
          (is (= "2022-08-16T11:08:03Z" (current-ts conn)))))

      (testing "postive sign"
        (q conn ["SET TIME ZONE '+01:34'"])
        (is (= "2022-08-16T12:42:03+01:34" (current-ts conn))))

      (testing "negative sign"
        (q conn ["SET TIME ZONE '-01:04'"])
        (is (= "2022-08-16T10:04:03-01:04" (current-ts conn))))

      (jdbc/with-transaction [tx conn]
        (testing "in a transaction, inherits session tz"
          (is (= "2022-08-16T10:04:03-01:04" (current-ts tx))))

        (testing "tz can be modified in a transaction"
          (q conn ["SET TIME ZONE '+00:00'"])
          (is (= "2022-08-16T11:08:03Z" (current-ts conn)))))

      (testing "SET is a session operator, so the tz still applied to the session"
        (is (= "2022-08-16T11:08:03Z" (current-ts conn)))))))

(deftest zoned-dt-printing-test
  (require-server)
  (let [custom-clock (Clock/fixed (Instant/parse "2022-08-16T11:08:03Z")
                                  ;; lets hope we do not print zone prefix!
                                  (ZoneId/ofOffset "GMT" (ZoneOffset/ofHoursMinutes 3 12)))]
    (swap! (:server-state *server*) assoc :clock custom-clock)
    (with-open [conn (jdbc-conn)]
      (is (= "2022-08-16T14:20:03+03:12" (current-ts conn))))))

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

      (sql "START TRANSACTION")
      (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (sql "INSERT INTO foo (_id) VALUES (42)")))
      (sql "ROLLBACK")

      (sql "START TRANSACTION READ WRITE")
      (sql "INSERT INTO foo (_id) VALUES (42)")
      (sql "COMMIT")
      (is (= [{:_id 42}] (q conn ["SELECT _id from foo"])))

      (testing "access mode overrides SET TRANSACTION"
        (sql "SET TRANSACTION READ WRITE")
        (sql "START TRANSACTION READ ONLY")
        (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (sql "INSERT INTO foo (_id) VALUES (42)")))
        (sql "ROLLBACK"))

      (testing "set transaction cleared"
        (sql "START TRANSACTION")
        (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (sql "INSERT INTO foo (_id) VALUES (42)")))
        (sql "ROLLBACK")))))

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

      (is (= [{:version 0, :_valid_from "2020-01-01T00:00Z", :_valid_to nil}]
             (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

      (sql "START TRANSACTION READ WRITE")
      (sql "UPDATE foo SET version = 1 WHERE _id = 'foo'")
      (sql "COMMIT")

      (is (= [{:version 1, :_valid_from "2020-01-02T00:00Z", :_valid_to nil}]
             (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

      (is (= (set [{:version 0, :_valid_from "2020-01-01T00:00Z", :_valid_to "2020-01-02T00:00Z"}
                   {:version 1, :_valid_from "2020-01-02T00:00Z", :_valid_to nil}])
             (set (q conn ["SETTING DEFAULT VALID_TIME ALL
                            SELECT version, _valid_from, _valid_to FROM foo"]))))

      (is (= [{:version 1, :_valid_from "2020-01-02T00:00Z", :_valid_to nil}]
             (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

      (sql "START TRANSACTION READ WRITE")
      (sql "UPDATE foo FOR ALL VALID_TIME SET version = 2 WHERE _id = 'foo'")
      (sql "COMMIT")

      (is (= [{:version 2, :_valid_from "2020-01-02T00:00Z", :_valid_to nil}]
             (q conn ["SELECT version, _valid_from, _valid_to FROM foo"])))

      (is (= [{:version 2, :_valid_from "2020-01-01T00:00Z", :_valid_to "2020-01-02T00:00Z"}
              {:version 2, :_valid_from "2020-01-02T00:00Z", :_valid_to nil}]
             (q conn ["SETTING DEFAULT VALID_TIME ALL
                       SELECT version, _valid_from, _valid_to FROM foo"])))

      (is (= [{:version 2, :_valid_from "2020-01-02T00:00Z", :_valid_to nil}]
             (q conn ["SETTING DEFAULT VALID_TIME AS OF NOW SELECT version, _valid_from, _valid_to FROM foo"]))))))

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
      (is (thrown-with-msg? PSQLException #"INSERT does not contain mandatory _id column" (q conn ["INSERT INTO foo (a) values (42)"])))
      (is (thrown-with-msg? PSQLException #"INSERT does not contain mandatory _id column" (q conn ["COMMIT"]))))))

(when (psql-available?)
  (deftest psql-analyzer-error-test
    (psql-session
     (fn [send read]
       (send "SLECT 1 FROM foo;\n")
       (let [s (read :err)]
         (is (not= :timeout s))
         (is (re-find #"mismatched input 'SLECT' expecting" s)))

       (send "BEGIN READ WRITE;\n")
       (read)
       ;; no-id
       (send "INSERT INTO foo (x) values (42);\n")
       (let [s (read :err)]
         (is (not= :timeout s))
         (is (re-find #"(?m)INSERT does not contain mandatory _id column" s)))

       (send "COMMIT;\n")
       (let [s (read :err)]
         (is (not= :timeout s))
         (is (re-find #"(?m)INSERT does not contain mandatory _id column" s)))))))

(deftest runtime-error-query-test
  (tu/with-log-level 'xtdb.pgwire :off
    (with-open [conn (jdbc-conn)]
      (is (thrown-with-msg? PSQLException #"Data Exception - trim error."
                            (q conn ["SELECT TRIM(LEADING 'abc' FROM a.a) FROM (VALUES ('')) a (a)"]))))))

(deftest runtime-error-commit-test
  (with-open [conn (jdbc-conn)]
    (q conn ["START TRANSACTION READ WRITE"])
    (q conn ["INSERT INTO foo (_id) VALUES (TRIM(LEADING 'abc' FROM ''))"])
    (t/is (thrown-with-msg? PSQLException #"Data Exception - trim error." (q conn ["COMMIT"])))))

(deftest test-column-order
  (with-open [conn (jdbc-conn)]

    (let [sql #(q-seq conn [%])]
      (q conn ["INSERT INTO foo(_id, col0, col1) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')"])

      (is (= [[20 "b" 2] [10 "a" 1] [30 "c" 3]]
             (sql "SELECT col0, col1, _id FROM foo")))

      (is (= [[2 20 "b"] [1 10 "a"] [3 30 "c"]]
             (sql "SELECT * FROM foo"))))))

;; https://github.com/pgjdbc/pgjdbc/blob/8afde800bce64e9b22a7da10ca6c515017cf7db1/pgjdbc/src/main/java/org/postgresql/jdbc/PgConnection.java#L403
;; List of types/oids that support binary format

(deftest test-postgres-types
  (with-open [conn (jdbc-conn "prepareThreshold" 1 "binaryTransfer" false)]
    (q-seq conn ["INSERT INTO foo(xt$id, int8, int4, int2, float8 , var_char, bool, timestamptz) VALUES (?, ?, ?, ?, ?, ?, ?, TIMESTAMP '3000-03-15 20:40:31+03:44')"
                 #uuid "9e8b41a0-723f-4e6b-babb-c4e6afd17ef2" Long/MIN_VALUE Integer/MAX_VALUE Short/MIN_VALUE Double/MAX_VALUE "aa" true]))
  ;; no float4 for text format due to a bug in pgjdbc where it sends it as a float8 causing a union type.
  (with-open [conn (jdbc-conn "prepareThreshold" 1 "binaryTransfer" true)]
    (q-seq conn ["INSERT INTO foo(xt$id, int8, int4, int2, float8, float4, var_char, bool, timestamptz) VALUES (?, ?, ?, ?, ?, ?, ?, ?, TIMESTAMP '3000-03-15 20:40:31Z')"
                 #uuid "7dd2ed62-bb05-43c8-b289-5503d9b19ee6" Long/MAX_VALUE Integer/MIN_VALUE Short/MAX_VALUE Double/MIN_VALUE Float/MAX_VALUE "bb" false])
    ;;binary format is only requested for server side preparedStatements in pgjdbc
    ;;threshold one should mean we test the text and binary format for each type
    (let [sql #(jdbc/execute! conn [%])
          res [{:float8 Double/MIN_VALUE,
                :float4 Float/MAX_VALUE,
                :int2 Short/MAX_VALUE,
                :timestamptz "3000-03-15T20:40:31Z",
                :var_char "bb",
                :int4 Integer/MIN_VALUE,
                :int8 Long/MAX_VALUE,
                :_id #uuid "7dd2ed62-bb05-43c8-b289-5503d9b19ee6",
                :bool false}
               {:float8 Double/MAX_VALUE,
                :float4 nil,
                :int2 Short/MIN_VALUE,
                :timestamptz "3000-03-15T20:40:31+03:44",
                :var_char "aa",
                :int4 Integer/MAX_VALUE,
                :int8 Long/MIN_VALUE,
                :_id #uuid "9e8b41a0-723f-4e6b-babb-c4e6afd17ef2",
                :bool true}]]


      (is (=
           res
           (->> (sql "SELECT * FROM foo")
                (map #(update % :timestamptz <-pgobject))))
          "text")

      (is (=
           res
           (->> (sql "SELECT * FROM foo")
                (map #(update % :timestamptz <-pgobject))))
          "binary"))))

(deftest test-odbc-queries
  (with-open [conn (jdbc-conn)]

    ;; ODBC issues this query by default
    (is (= []
           (q-seq conn ["select oid, typbasetype from pg_type where typname = 'lo'"])))))

(t/deftest test-pg-port
  (util/with-open [node (xtn/start-node {::pgwire/server {:port 0}})]
    (binding [*port* (.getPgPort node)]
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
                            #"ERROR: Precondition failed: assert-exists"
                            (jdbc/with-transaction [tx conn]
                              (jdbc/execute! tx ["ASSERT 1 = (SELECT COUNT(*) FROM foo)"])
                              (jdbc/execute! tx ["INSERT INTO foo (_id) VALUES (2)"]))))

    (t/is (= [{:row_count 2}]
             (q conn ["SELECT COUNT(*) row_count FROM foo"])))

    (t/is (= [{:_id 2, :committed false,
               :error {"row-count" 0, "error-key" "xtdb/assert-failed",
                       "message" "Precondition failed: assert-exists"}}
              {:_id 1, :committed true, :error nil}
              {:_id 0, :committed true, :error nil}]
             (q conn ["SELECT * EXCLUDE tx_time FROM xt.txs"])))))

(deftest test-null-type
  (with-open [conn (jdbc-conn "prepareThreshold" -1)
              stmt (.prepareStatement conn "SELECT ?")]

    (t/testing "untyped null param"

      (.setObject stmt 1 nil)

      ;;pgjdbc throws a java.lang.AssertionError: Misuse of castNonNull: called with a null argument
      ;;when JRE assertions are enabled (as they are in test tasks)
      ;;this error is likely happening because we opt to describe the param type of null and unknown params
      ;;as unknown rather than as text (or some other inferred type). This is a departure from pg but
      ;;arguably more true for xtdb.
      ;;
      ;;We could do a pg does and eagerly infer a type, however I think we should wait to see if there is a
      ;;need for clients to be able to describe untyped null/unknown params before making a decision here.
      #_(t/is (= [nil] (param-metadata stmt)))

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

(deftest test-prepared-statements-with-unknown-params
  (with-open [conn (jdbc-conn "prepareThreshold" -1)
              stmt (.prepareStatement conn "SELECT ?")]

    ;;see above
    #_(t/testing "unknown param types are assumed to be of type null"

      (t/is (= [nil] (param-metadata stmt))))

    (t/testing "Once a param value is supplied statement updates correctly and can be executed"

      (.setObject stmt 1 #uuid "7dd2ed62-bb05-43c8-b289-5503d9b19ee6")

      (t/is (= ["uuid"] (param-metadata stmt)))

      (with-open [rs (.executeQuery stmt)]

        (t/is (= [{"_column_1" "uuid"}] (result-metadata stmt) (result-metadata rs)))
        (t/is (= [{"_column_1" #uuid "7dd2ed62-bb05-43c8-b289-5503d9b19ee6"}] (rs->maps rs)))))))

(deftest test-prepared-statments
  (with-open [conn (jdbc-conn "prepareThreshold" -1)]
    (.execute (.prepareStatement conn "INSERT INTO foo(xt$id, a, b) VALUES (1, 'one', 2)"))
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

        (.execute (.prepareStatement conn "INSERT INTO foo(xt$id, a, b) VALUES (2, 1, 1)"))

        ;;TODO we just return our own custom error here, rather than 'cached plan must not change result type'
        ;;might be worth confirming if there is a specific error type/message that matters for pgjdbc as there
        ;;appears to be retry logic built into the driver
        ;;https://jdbc.postgresql.org/documentation/server-prepare/#re-execution-of-failed-statements
        (t/is (thrown-with-msg?
               PSQLException
               #"ERROR: Relevant table schema has changed since preparing query, please prepare again"
               (with-open [_rs (.executeQuery stmt)])))))))
