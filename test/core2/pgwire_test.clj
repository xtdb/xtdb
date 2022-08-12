(ns core2.pgwire-test
  (:require [core2.pgwire :as pgwire]
            [clojure.test :refer [deftest is testing] :as t]
            [core2.local-node :as node]
            [core2.test-util :as tu]
            [clojure.data.json :as json]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.shell :as sh]
            [core2.util :as util]
            [core2.api :as c2])
  (:import (java.sql Connection)
           (org.postgresql.util PGobject PSQLException)
           (com.fasterxml.jackson.databind.node JsonNodeType)
           (com.fasterxml.jackson.databind ObjectMapper JsonNode)
           (java.lang Thread$State)
           (java.net SocketException)
           (java.util.concurrent CountDownLatch TimeUnit CompletableFuture)
           (core2 IResultSet)))

(set! *warn-on-reflection* false)
(set! *unchecked-math* false)

(def ^:dynamic ^:private *port*)
(def ^:dynamic ^:private *node*)
(def ^:dynamic ^:private *server*)

(defn require-node []
  (when-not *node*
    (set! *node* (node/start-node {}))))

(defn require-server
  ([] (require-server {}))
  ([opts]
   (require-node)
   (when-not *port*
     (set! *port* (tu/free-port))
     (->> (merge {:num-threads 1}
                 opts
                 {:port *port*})
          (pgwire/serve *node*)
          (set! *server*)))))

(defn- each-fixture [f]
  (binding [*port* nil
            *server* nil
            *node* nil]
    (try
      (f)
      (finally
        (some-> *node* .close)
        (some-> *server* .close)))))

(t/use-fixtures :each #'each-fixture)

(defn- once-fixture [f]
  (let [check-if-no-pgwire-threads (zero? (count @#'pgwire/servers))]
    (try
      (f)
      (finally
        (when check-if-no-pgwire-threads
          ;; warn if it looks like threads are stick around (for CI logs)
          (when-not (zero? (->> (Thread/getAllStackTraces)
                                keys
                                (map #(.getName %))
                                (filter #(str/starts-with? % "pgwire"))
                                count))
            (log/warn "dangling pgwire resources discovered after tests!"))

          ;; stop all just in case we can clean up anyway
          (pgwire/stop-all))))))

(t/use-fixtures :once #'once-fixture)

(defn- jdbc-url [& params]
  (require-server)
  (assert *port* "*port* must be bound")
  (let [param-str (when (seq params) (str "?" (str/join "&" (for [[k v] (partition 2 params)] (str k "=" v)))))]
    (format "jdbc:postgresql://:%s/xtdb%s" *port* param-str)))

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

(deftest ssl-test
  (t/are [sslmode expect]
    (= expect (try-sslmode sslmode))

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
        (is (= "\"hello, world2\"" (str (.getObject rs 1))))
        (is (= false (.next rs)))))

    (dotimes [_ 5]
      (with-open [rs (.executeQuery stmt)]
        (is (= true (.next rs)))
        (is (= "\"hello, world\"" (str (.getObject rs 1))))
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
     ;

     (integer 0)
     (integer -0)
     (integer 42)
     (integer Long/MAX_VALUE)

     ;; does not work
     ;; MAX 9223372036854775807
     ;; MIN -9223372036854775808
     ;; min > max if you remove the sign
     #_(integer Long/MIN_VALUE)

     (decimal 0.0)
     (decimal -0.0)
     (decimal 3.14)
     (decimal 42.0)

     ;; does not work no exact decimal support currently
     (decimal Double/MIN_VALUE)
     (decimal Double/MAX_VALUE :add-zero true)

     ;; dates / times
     ;

     {:sql "DATE '2021-12-24'"
      :json-type JsonNodeType/STRING
      :clj "2021-12-24"}

     ;; does not work, no timestamp literals
     #_
     {:sql "TIMESTAMP '2021-12-24 11:23:44.003'"
      :json-type JsonNodeType/STRING
      :clj "2021-12-24T11:23:44.003Z"}

     ;; does not work
     ;; java.lang.ClassCastException: class org.apache.arrow.vector.IntervalYearVector cannot be cast to class org.apache.arrow.vector.IntervalMonthDayNanoVector
     #_{:sql "1 YEAR"
        :json-type JsonNodeType/STRING
        :clj "P1Y"}
     #_
     {:sql "1 MONTH"
      :json-type JsonNodeType/STRING
      :clj "P1M"}

     ;; arrays
     ;

     ;; does not work (cannot parse empty array)
     #_
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

     ;; issue #245
     #_
     {:sql "ARRAY [ARRAY ['42'], 42, '42']"
      :json-type JsonNodeType/ARRAY
      :clj [["42"] 42 "42"]}]))

(deftest json-representation-test
  (with-open [conn (jdbc-conn)]
    (doseq [{:keys [json-type, json, sql, clj, clj-pred] :as example} json-representation-examples]
      (testing (str "SQL expression " sql " should parse to " clj " (" (when json (str json ", ")) json-type ")")
        (with-open [stmt (.prepareStatement conn (format "SELECT a.a FROM (VALUES (%s)) a (a)" sql))]
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

(defn- registered? [server]
  (= server (get @#'pgwire/servers (:port server))))

(deftest server-registered-on-start-test
  (require-server)
  (is (registered? *server*)))

(defn check-server-resources-freed
  ([]
   (require-server)
   (check-server-resources-freed *server*))
  ([server]
   (testing "unregistered"
     (is (not (registered? server))))

   (testing "accept socket"
     (is (.isClosed @(:accept-socket server))))

   (testing "accept thread"
     (is (= Thread$State/TERMINATED (.getState (:accept-thread server)))))

   (testing "thread pool shutdown"
     (is (.isShutdown (:thread-pool server)))
     (is (.isTerminated (:thread-pool server))))))

(deftest server-resources-freed-on-close-test
  (require-node)
  (doseq [close-method [#(.close %)
                         pgwire/stop-server]]
    (with-open [server (pgwire/serve *node* {:port (tu/free-port)})]
      (close-method server)
      (check-server-resources-freed server))))

(deftest server-resources-freed-if-exc-on-start-test
  (require-node)
  (with-open [server (pgwire/serve *node* {:port (tu/free-port)
                                           :unsafe-init-state
                                           {:silent-start true
                                            :injected-start-exc (Exception. "boom!")}})]
    (check-server-resources-freed server)))

(deftest accept-thread-and-socket-closed-on-uncaught-accept-exc-test
  (require-server)

  (swap! (:server-state *server*) assoc
         :injected-accept-exc (Exception. "boom")
         :silent-accept true)

  (is (thrown? Throwable (with-open [_ (jdbc-conn)])))

  (testing "registered"
    (is (registered? *server*)))

  (testing "accept socket"
    (is (.isClosed @(:accept-socket *server*))))

  (testing "accept thread"
    (is (= Thread$State/TERMINATED (.getState (:accept-thread *server*))))))

(defn q [conn sql]
  (->> (jdbc/execute! conn sql)
       (mapv (fn [row] (update-vals row (comp json/read-str str))))))

(defn ping [conn]
  (-> (q conn ["select a.ping from (values ('pong')) a (ping)"])
      first
      :ping))

(defn- inject-accept-exc
  ([]
   (inject-accept-exc (Exception. "")))
  ([ex]
   (require-server)
   (swap! (:server-state *server*)
          assoc :injected-accept-exc ex, :silent-accept true)
   nil))

(defn- connect-and-throwaway []
  (try (jdbc-conn) (catch Throwable _)))

(deftest accept-uncaught-exception-allows-free-test
  (inject-accept-exc)
  (connect-and-throwaway)
  (.close *server*)
  (check-server-resources-freed))

(deftest accept-thread-stoppage-sets-error-status
  (inject-accept-exc)
  (connect-and-throwaway)
  (is (= :error @(:server-status *server*))))

(deftest accept-thread-stoppage-allows-other-conns-to-continue-test
  (with-open [conn1 (jdbc-conn)]
    (inject-accept-exc)
    (connect-and-throwaway)
    (is (= "pong" (ping conn1)))))

(deftest accept-thread-socket-closed-exc-does-not-stop-later-accepts-test
  (inject-accept-exc (SocketException. "Socket closed"))
  (connect-and-throwaway)
  (is (with-open [conn (jdbc-conn)] true)))

(deftest accept-thread-interrupt-closes-thread-test
  (require-server {:accept-so-timeout 10})

  (.interrupt (:accept-thread *server*))
  (.join (:accept-thread *server*) 1000)

  (is (:accept-interrupted @(:server-state *server*)))
  (is (= Thread$State/TERMINATED (.getState (:accept-thread *server*)))))

(deftest accept-thread-interrupt-allows-server-shutdown-test
  (require-server {:accept-so-timeout 10})

  (.interrupt (:accept-thread *server*))
  (.join (:accept-thread *server*) 1000)

  (.close *server*)
  (check-server-resources-freed))

(deftest accept-thread-socket-close-stops-thread-test
  (require-server)
  (.close @(:accept-socket *server*))
  (.join (:accept-thread *server*) 1000)
  (is (= Thread$State/TERMINATED (.getState (:accept-thread *server*)))))

(deftest accept-thread-socket-close-allows-cleanup-test
  (require-server)
  (.close @(:accept-socket *server*))
  (.join (:accept-thread *server*) 1000)
  (.close *server*)
  (check-server-resources-freed))

(deftest accept-socket-timeout-set-by-default-test
  (require-server)
  (is (pos? (.getSoTimeout @(:accept-socket *server*)))))

(deftest accept-socket-timeout-can-be-unset-test
  (require-server {:accept-so-timeout nil})
  (is (= 0 (.getSoTimeout @(:accept-socket *server*)))))

(deftest stop-all-test
  (when-not (= 0 (count @#'pgwire/servers))
    (log/warn "skipping stop-all-test because servers already exist"))

  (when (= 0 (count @#'pgwire/servers))
    (require-node)
    (let [server1 (pgwire/serve *node* {:port (tu/free-port)})
          server2 (pgwire/serve *node* {:port (tu/free-port)})]
      (pgwire/stop-all)
      (check-server-resources-freed server1)
      (check-server-resources-freed server2))))

(deftest conn-registered-on-start-test
  (require-server {:num-threads 2})
  (with-open [_ (jdbc-conn)]
    (is (= 1 (count (:connections @(:server-state *server*)))))
    (with-open [_ (jdbc-conn)]
      (is (= 2 (count (:connections @(:server-state *server*))))))))

(defn- get-connections []
  (vals (:connections @(:server-state *server*))))

(defn- get-last-conn []
  (last (sort-by :cid (get-connections))))

(defn- wait-for-close [server-conn ms]
  (deref (:close-promise @(:conn-state server-conn)) ms false))

(deftest conn-deregistered-on-close-test
  (require-server {:num-threads 2})
  (with-open [conn1 (jdbc-conn)
              srv-conn1 (get-last-conn)
              conn2 (jdbc-conn)
              srv-conn2 (get-last-conn)]
    (.close conn1)
    (is (wait-for-close srv-conn1 500))
    (is (= 1 (count (get-connections))))

    (.close conn2)
    (is (wait-for-close srv-conn2 500))
    (is (= 0 (count (get-connections))))))

(defn check-conn-resources-freed [server-conn]
  (let [{:keys [cid, socket, server]} server-conn
        {:keys [server-state]} server]
    (is (.isClosed socket))
    (is (not (contains? (:connections @server-state) cid)))))

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
                    (catch PSQLException e
                      ))))

        futs (mapv spawn (range 10))]

    (is (some #(not= :timeout (deref % 1000 :timeout)) futs))

    (.close *server*)

    (is (every? #(not= :timeout (deref % 1000 :timeout)) futs))

    (check-server-resources-freed)))

;; the goal of this test is to cause a bunch of ping queries to block on parse
;; until the server is draining
;; and observe that connection continue until the multi-message extended interaction is done
;; (when we introduce read transactions I will probably extend this to short-lived transactions)
(deftest close-drains-active-extended-queries-before-stopping-test
  (require-server {:num-threads 10
                   :accept-so-timeout 10})
  (let [cmd-parse @#'pgwire/cmd-parse
        server-status (:server-status *server*)
        latch (CountDownLatch. 10)]
    ;; redefine parse to block when we ping
    (with-redefs [pgwire/cmd-parse
                  (fn [conn {:keys [query] :as cmd}]
                    (if-not (str/starts-with? query "select a.ping")
                      (cmd-parse conn cmd)
                      (do
                        (.countDown latch)
                        ;; delay until we see a draining state
                        (loop [wait-until (+ (System/currentTimeMillis) 5000)]
                          (when (and (< (System/currentTimeMillis) wait-until)
                                     (not= :draining @server-status))
                            (recur wait-until)))
                        (cmd-parse conn cmd))))]
      (let [spawn (fn spawn [] (future (with-open [conn (jdbc-conn)] (ping conn))))
            futs (vec (repeatedly 10 spawn))]

        (is (.await latch 1 TimeUnit/SECONDS))

        (.close *server*)

        (is (every? #(= "pong" (deref % 1000 :timeout)) futs))

        (check-server-resources-freed)))))

(deftest jdbc-query-cancellation-test
  (require-server {:num-threads 2})
  (let [stmt-promise (promise)

        start-conn1
        (fn []
          (with-open [conn (jdbc-conn)]
            (with-open [stmt (.prepareStatement conn "select a.a from a")]
              (try
                (with-redefs [c2/open-sql-async (fn [& _] (deliver stmt-promise stmt) (CompletableFuture.))]
                  (with-open [rs (.executeQuery stmt)]
                    :not-cancelled))
                (catch PSQLException e
                  (.getMessage e))))))

        fut (future (start-conn1))]

    (is (not= :timeout (deref stmt-promise 1000 :timeout)))
    (when (realized? stmt-promise) (.cancel @stmt-promise))
    (is (= "ERROR: query cancelled during execution" (deref fut 1000 :timeout)))))

(deftest jdbc-prepared-query-close-test
  (with-open [conn (jdbc-conn "prepareThreshold" "1"
                              "preparedStatementCacheQueries" 0
                              "preparedStatementCacheMiB" 0)]
    (dotimes [i 3]
      ;; do not use parameters as to trigger close it needs to be a different query every time
      (with-open [stmt (.prepareStatement conn (format "SELECT a.a FROM (VALUES (%s)) a (a)" i))]
        (.close (.executeQuery stmt))))

    (testing "only empty portal should remain"
      (is (= [""] (keys (:portals @(:conn-state (get-last-conn)))))))

    (testing "even at cache policy 0, pg jdbc caches - but we should only see the last stmt + empty"
      ;; S_3 because i == 3
      (is (= #{"", "S_3"} (set (keys (:prepared-statements @(:conn-state (get-last-conn))))))))))

(defn psql-available?
  "Returns true if psql is available in $PATH"
  []
  (try (= 0 (:exit (sh/sh "command" "-v" "psql"))) (catch Throwable _ false)))

(defn psql-session
  "Takes a function of two args (send, read).

  Send puts a string in to psql stdin, reads the next string from psql stdout. You can use (read :err) if you wish to read from stderr instead."
  [f]
  (require-server)
  ;; there are other ways to do this, but its a straightforward factoring that removes some boilerplate for now.
  (let [pb (ProcessBuilder. ["psql" "-h" "localhost" "-p" (str *port*)])
        p (.start pb)
        in (delay (.getInputStream p))
        err (delay (.getErrorStream p))
        out (delay (.getOutputStream p))

        send
        (fn [s]
          (.write @out (.getBytes s "utf-8"))
          (.flush @out))

        read
        (fn read
          ([] (read @in))
          ([stream]
           (let [stream (case stream :err @err stream)]
             (loop [wait-until (+ (System/currentTimeMillis) 1000)]
               (cond
                 (pos? (.available stream))
                 (let [barr (byte-array (.available stream))]
                   (.read stream barr)
                   (String. barr))

                 (< wait-until (System/currentTimeMillis)) :timeout
                 :else (recur wait-until))))))]
    (try
      (f send read)
      (finally
        (when (.isAlive p)
          (.destroy p))

        (is (.waitFor p 1000 TimeUnit/MILLISECONDS))
        (is (#{143, 0} (.exitValue p)))

        (when (realized? in) (util/try-close @in))
        (when (realized? out) (util/try-close @out))
        (when (realized? err) (util/try-close @err))))))

;; define psql tests if psql is available on path
;; (will probably move to a selector)
(when (psql-available?)

  (deftest psql-connect-test
    (require-server)
    (let [{:keys [exit, out]} (sh/sh "psql" "-h" "localhost" "-p" (str *port*) "-c" "select ping")]
      (is (= 0 exit))
      (is (str/includes? out " pong\n(1 row)"))))

  (deftest psql-interactive-test
    (psql-session
      (fn [send read]
        (testing "ping"
          (send "select ping;\n")
          (let [s (read)]
            (is (str/includes? s "pong"))
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

        (testing "parse error"
          (send "not really sql;\n")
          (is (str/includes? (read :err) "ERROR"))

          (testing "parse error allows session to continue"
            (send "select ping;\n")
            (is (str/includes? (read) "pong"))))

        (testing "query crash during plan"
          (with-redefs [clojure.tools.logging/logf (constantly nil)
                        c2/open-sql-async (fn [& _] (CompletableFuture/failedFuture (Throwable. "oops")))]
            (send "select a.a from (values (42)) a (a);\n")
            (is (str/includes? (read :err) "unexpected server error during query execution")))

          (testing "internal query error allows session to continue"
            (send "select ping;\n")
            (is (str/includes? (read) "pong"))))

        (testing "query crash during result set iteration"
          (with-redefs [clojure.tools.logging/logf (constantly nil)
                        c2/open-sql-async (fn [& _] (CompletableFuture/completedFuture
                                                      (reify IResultSet
                                                        (hasNext [_] true)
                                                        (next [_] (throw (Throwable. "oops")))
                                                        (close [_]))))]
            (send "select a.a from (values (42)) a (a);\n")
            (is (str/includes? (read :err) "unexpected server error during query execution")))

          (testing "internal query error allows session to continue"
            (send "select ping;\n")
            (is (str/includes? (read) "pong"))))))))

(def pg-param-representation-examples
  "A library of examples to test pg parameter oid handling.

  e.g set this object as a param, round trip it - does it match the json result?

  :param (the java object, e.g (int 42))
  :json the expected (parsed via data.json) json representation
  :json-cast (a function to apply to the json, useful for downcast for floats)"
  [{:param nil
    :json nil}

   {:param true
    :json true}

   {:param false
    :json false}

   {:param (byte 42)
    :json 42}

   {:param (byte -42)
    :json -42}

   {:param (short 257)
    :json 257}

   {:param (short -257)
    :json -257}

   {:param (int 92767)
    :json 92767}

   {:param (int -92767)
    :json -92767}

   {:param (long 4147483647)
    :json 4147483647}

   {:param (long -4147483647)
    :json -4147483647}

   {:param (float Math/PI)
    :json (float Math/PI)
    :json-cast float}

   {:param (+ 1.0 (double Float/MAX_VALUE))
    :json (+ 1.0 (double Float/MAX_VALUE))}

   {:param ""
    :json ""}

   {:param "hello, world!"
    :json "hello, world!"}

   {:param "😎"
    :json "😎"}])

(deftest pg-param-representation-test
  (with-open [conn (jdbc-conn)
              stmt (.prepareStatement conn "select a.a from (values (?)) a (a)")]
    (doseq [{:keys [param, json, json-cast]
             :or {json-cast identity}}
            pg-param-representation-examples]
      (testing (format "param %s (%s)" param (class param))

        (.clearParameters stmt)

        (condp instance? param
          Byte (.setByte stmt 1 param)
          Short (.setShort stmt 1 param)
          Integer (.setInt stmt 1 param)
          Long (.setLong stmt 1 param)
          Float (.setFloat stmt 1 param)
          Double (.setDouble stmt 1 param)
          String (.setString stmt 1 param)
          (.setObject stmt 1 param))

        (with-open [rs (.executeQuery stmt)]
          (is (.next rs))
          ;; may want more fine-grained json assertions than this, it depends on data.json behaviour
          (is (= json (json-cast (json/read-str (str (.getObject rs 1)))))))))))

;; maps cannot be created from SQL yet, or used as parameters - but we can read them from XT.
(deftest map-read-test
  (with-open [conn (jdbc-conn)]
    (-> (c2/submit-tx *node* [[:put {:id "map-test", :a {:b 42}}]])
        (tu/then-await-tx *node*))
    (let [rs (q conn ["select a.a from a a"])]
      (is (= [{:a {"b" 42}}] rs)))))

(deftest start-stop-as-module-test
  (let [port (tu/free-port)]
    (with-open [node (node/start-node {:core2/pgwire {:port port
                                                      :num-threads 3}})]
      (let [srv (get @#'pgwire/servers port)]
        (is (some? srv))
        (is (identical? (::node/node @(:!system node)) (:node srv))))

      (with-open [conn (jdbc-conn)]
        (is (= "pong" (ping conn)))))

    (is (not (contains? @#'pgwire/servers port)))))

(deftest open-close-transaction-does-not-crash-test
  (with-open [conn (jdbc-conn)]
    (jdbc/with-transaction [db conn]
      (is (= [{:ping "pong"}] (jdbc/execute! db ["select ping;"]))))))

;; for now, behaviour will change later I am sure
(deftest different-transaction-isolation-levels-accepted-and-ignored-test
  (with-open [conn (jdbc-conn)]
    (doseq [level [:read-committed
                   :read-uncommitted
                   :repeatable-read
                   :serializable]]
      (testing (format "can open and close transaction (%s)" level)
        (jdbc/with-transaction [db conn {:isolation level}]
          (is (= [{:ping "pong"}] (jdbc/execute! db ["select ping;"])))))
      (testing (format "readonly accepted (%s)" level)
        (jdbc/with-transaction [db conn {:isolation level, :read-only true}]
          (is (= [{:ping "pong"}] (jdbc/execute! db ["select ping;"])))))
      (testing (format "rollback only accepted (%s)" level)
        (jdbc/with-transaction [db conn {:isolation level, :rollback-only true}]
          (is (= [{:ping "pong"}] (jdbc/execute! db ["select ping;"]))))))))

;; right now all isolation levels have the same defined behaviour
(deftest transaction-by-default-pins-the-basis-too-last-tx-test
  (require-node)
  (let [insert #(future (-> (c2/submit-tx *node* [[:put %]]) (tu/then-await-tx *node*)))]
    @(insert {:id :fred, :name "Fred"})
    (with-open [conn (jdbc-conn)]

      (jdbc/with-transaction [db conn]
        (is (= [{:name "Fred"}] (q db ["select a.name from a"])))
        @(insert {:id :bob, :name "Bob"})
        (is (= [{:name "Fred"}] (q db ["select a.name from a"]))))

      (is (= [{:name "Fred"}, {:name "Bob"}] (q conn ["select a.name from a"]))))))

;; SET is not supported properly at the moment, so this ensure we do not really do anything too embarrassing (like crash)
(deftest set-statement-test
  (let [params #(-> (get-last-conn) :conn-state deref :session :parameters (select-keys %))]
    (with-open [conn (jdbc-conn)]

      (testing "literals saved as is (no parser hooked up yet)"
        (is (q conn ["SET a = 'b'"]))
        (is (= {"a" "'b'"} (params ["a"])))
        (is (q conn ["SET b = 42"]))
        (is (= {"a" "'b'", "b" "42"} (params ["a" "b"]))))

      (testing "properties can be overwritten"
        (q conn ["SET a = 42"])
        (is (= {"a" "42"} (params ["a"]))))

      (testing "TO syntax can be used"
        (q conn ["SET a TO 43"])
        (is (= {"a" "43"} (params ["a"])))))))

(deftest dml-is-not-permitted-by-default-test
  (with-open [conn (jdbc-conn)]
    (is (thrown-with-msg?
          PSQLException #"ERROR\: DML is unsupported in a READ ONLY transaction"
          (q conn ["insert into foo(a) values (42)"])))
    (->> "can query after auto-commit write is refused"
         (is (= [] (q conn ["select foo.a from foo"]))))))

(deftest db-queryable-after-transaction-error-test
  (with-open [conn (jdbc-conn)]
    (try
      (jdbc/with-transaction [db conn]
        (is (= [] (q db ["select a.a from a a"])))
        (throw (Exception. "Oh no!")))
      (catch Throwable _))
    (is (= [] (q conn ["select a.a from a a"])))))

(deftest transactions-are-read-only-by-default-test
  (with-open [conn (jdbc-conn)]
    (is (thrown-with-msg?
          PSQLException #"ERROR\: DML is unsupported in a READ ONLY transaction"
          (jdbc/with-transaction [db conn] (q db ["insert into foo(a) values (42)"]))))
    (is (= [] (q conn ["select foo.a from foo foo"])))))

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
                              (q tx ["INSERT INTO foo(id, a) values(42, 42)"])
                              (q conn ["SELECT foo.a FROM bar"]))))
      (is (= [] (q conn ["SELECT foo.a FROM foo"]))))

    (testing "insert it"
      (q conn ["SET TRANSACTION READ WRITE"])
      (jdbc/with-transaction [tx conn]
        (q tx ["INSERT INTO foo(id, a) values(42, 42)"]))
      (testing "read after write"
        (is (= [{:a 42}] (q conn ["SELECT foo.a FROM foo"])))))

    (testing "delete it"
      (tx! conn ["DELETE FROM foo WHERE foo.id = 42"])
      (is (= [] (q conn ["SELECT foo.a FROM foo"]))))))

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
          (send "INSERT INTO foo (id, a) values (42, 42);\n")
          (is (str/includes? (read) "INSERT 0 0")))

        (testing "insert 2"
          (send "INSERT INTO foo (id, a) values (366, 366);\n")
          (is (str/includes? (read) "INSERT 0 0")))

        (testing "commit"
          (send "COMMIT;\n")
          (is (= (str/includes? (read) "COMMIT"))))

        (testing "read your own writes"
          (send "SELECT foo.a FROM foo;\n")
          (let [s (read)]
            (is (str/includes? s "42"))
            (is (str/includes? s "366")))))))

  (deftest psql-dml-at-prompt-test
    (psql-session
      (fn [send read]
        (send "INSERT INTO foo(id, a) VALUES (42, 42);\n")
        (is (str/starts-with? (read :err) "ERROR:  DML is unsupported in a READ ONLY transaction"))))))

(deftest dml-param-test
  (with-open [conn (jdbc-conn)]
    (tx! conn ["INSERT INTO foo (id, a) VALUES (?, ?)" 42 "hello, world"])
    (is (= [{:a "hello, world"}] (q conn ["SELECT foo.a FROM foo where foo.id = 42"])))))
