(ns core2.sql.pgwire-test
  (:require [core2.sql.pgwire :as pgwire]
            [clojure.test :refer [deftest is testing] :as t]
            [core2.local-node :as node]
            [core2.test-util :as tu]
            [clojure.data.json :as json]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import (java.sql Connection)
           (org.postgresql.util PGobject PSQLException)
           (com.fasterxml.jackson.databind.node JsonNodeType)
           (com.fasterxml.jackson.databind ObjectMapper JsonNode)
           (java.lang Thread$State)
           (java.net SocketException)))

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

(defn- jdbc-conn ^Connection []
  (jdbc/get-connection (jdbc-url)))

(deftest query-test
  (with-open [conn (jdbc-conn)]
    (with-open [stmt (.createStatement conn)
                rs (.executeQuery stmt "SELECT a.a FROM (VALUES ('hello, world')) a (a)")]
      (is (= true (.next rs)))
      (is (= false (.next rs))))))

(deftest prepared-query-test
  (with-open [conn (jdbc-conn)]
    (with-open [stmt (.prepareStatement conn "SELECT a.a FROM (VALUES ('hello, world')) a (a)")
                rs (.executeQuery stmt)]
      (is (= true (.next rs)))
      (is (= false (.next rs))))))

(deftest parameterized-query-test
  (with-open [conn (jdbc-conn)]
    (with-open [stmt (doto (.prepareStatement conn "SELECT a.a FROM (VALUES (?)) a (a)")
                       (.setObject 1 "hello, world"))
                rs (.executeQuery stmt)]
      (is (= true (.next rs)))
      (is (= false (.next rs))))))

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
          (decimal [n]
            {:sql (.toPlainString (bigdec n))
             :json-type JsonNodeType/NUMBER
             :json (.toPlainString (bigdec n))
             :clj-pred #(= (bigdec %) (bigdec n))})]

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
     #_(decimal Double/MIN_VALUE)
     #_(decimal Double/MAX_VALUE)

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
