(ns xtdb.pgwire-protocol-test
  (:require [clojure.test :as t :refer [deftest]]
            [jsonista.core :as json]
            [xtdb.authn :as authn]
            [xtdb.pgwire :as pgwire]
            [xtdb.pgwire.io :as pgio]
            [xtdb.pgwire.types :as pg-types]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.lang AutoCloseable]
           [java.nio.charset StandardCharsets]
           [java.time Clock]))

(def ^:dynamic ^:private *port* nil)

(defn with-port [f]
  (let [server (-> tu/*node* :system :xtdb.pgwire/server)]
    (binding [*port* (:port server)]
      (f))))

(t/use-fixtures :each tu/with-allocator tu/with-mock-clock tu/with-node with-port)

(defn bytes->str [^bytes arr]
  (String. arr StandardCharsets/UTF_8))

(defrecord RecordingFrontend [!in-msgs !out-msgs]
  pgio/Frontend
  (send-client-msg! [_ msg-def]
    (swap! !in-msgs conj [(:name msg-def)])
    nil)

  (send-client-msg! [_ msg-def data]
    (let [data (case (:name msg-def)
                 :msg-data-row (update data :vals (partial mapv bytes->str))
                 :msg-error-response (update-in data [:error-fields :detail]
                                                (fn [detail]
                                                  (if (bytes? detail)
                                                    (json/read-value (String. ^bytes detail StandardCharsets/UTF_8)
                                                                     json/keyword-keys-object-mapper)
                                                    detail)))
                 data)]
      (swap! !in-msgs conj [(:name msg-def) data]))
    nil)

  (read-client-msg! [_]
    (let [msg (first @!out-msgs)]
      (swap! !out-msgs rest)
      (when-not msg (throw (Exception. "No more messages")))
      msg))

  (host-address [_] "127.0.0.1")

  AutoCloseable
  (close [_]))

(defn ->recording-frontend
  ([] (->recording-frontend []))
  ([out-msg] (->RecordingFrontend (atom []) (atom out-msg))))

(defn ->conn
  (^java.lang.AutoCloseable [frontend] (->conn frontend {}))
  (^java.lang.AutoCloseable [frontend startup-opts]
   (->conn frontend startup-opts [{:user nil, :method #xt.authn/method :trust, :address nil}]))
  (^java.lang.AutoCloseable [frontend startup-opts authn-rules]
   (let [conn (pgwire/map->Connection {:server {:server-state (atom {:parameters {"server_encoding" "UTF8"
                                                                                  "client_encoding" "UTF8"
                                                                                  "DateStyle" "ISO"
                                                                                  "IntervalStyle" "ISO_8601"}})
                                                :->node {"xtdb" (-> tu/*node*
                                                                    (assoc :authn (authn/->UserTableAuthn authn-rules
                                                                                                          (util/component tu/*node* :xtdb.query/query-source))))}}
                                       :allocator tu/*allocator*
                                       :frontend frontend
                                       :cid -1
                                       :!closing? (atom false)
                                       :conn-state (atom {:session {:clock (Clock/systemUTC)}})})]
     (try
       (pgwire/cmd-startup-pg30 conn startup-opts)
       (catch Exception e
         (if (::pgwire/error-code (ex-data e))
           (doto conn
             (pgwire/send-ex e)
             (pgwire/handle-msg* {:msg-name :msg-terminate}))

           (throw e)))))))

(deftest test-startup
  (let [{:keys [!in-msgs] :as frontend} (->recording-frontend [{:msg-name :msg-password :password "xtdb"}])]
    (with-open [_ (->conn frontend {"user" "xtdb", "database" "xtdb"} [{:user "xtdb", :method #xt.authn/method :password, :address "127.0.0.1"}])]
      (t/is (= [[:msg-auth {:result 3}]
                [:msg-auth {:result 0}]
                [:msg-parameter-status {:parameter "server_encoding", :value "UTF8"}]
                [:msg-parameter-status {:parameter "client_encoding", :value "UTF8"}]
                [:msg-parameter-status {:parameter "DateStyle", :value "ISO"}]
                [:msg-parameter-status {:parameter "IntervalStyle", :value "ISO_8601"}]
                [:msg-parameter-status {:parameter "user", :value "xtdb"}]
                [:msg-parameter-status {:parameter "database", :value "xtdb"}]
                [:msg-backend-key-data {:process-id -1, :secret-key 0}]
                [:msg-ready {:status :idle}]]
               @!in-msgs))

      (reset! !in-msgs []))))

(deftest test-auth-failure
  (let [{:keys [!in-msgs] :as frontend} (->recording-frontend [{:msg-name :msg-simple-query, :query "SELECT 1"}])]
    (with-open [_ (->conn frontend {"user" "xtdb", "database" "xtdb"} [{:user "xtdb", :method #xt.authn/method :password, :address "127.0.0.1"}])]

      (t/is (= [[:msg-auth {:result 3}]
                [:msg-error-response
                 {:error-fields
                  {:severity "ERROR",
                   :localized-severity "ERROR",
                   :sql-state "28000",
                   :message "password authentication failed for user: xtdb"
                   :detail nil}}]]
               @!in-msgs)))))


(deftest test-simple-query
  (let [{:keys [!in-msgs] :as frontend} (->recording-frontend {})]
    (with-open [conn (->conn frontend {"database" "xtdb"})]
      (reset! !in-msgs [])

      (pgwire/handle-msg conn {:msg-name :msg-simple-query, :query "SELECT 1"})

      (t/is (= [[:msg-row-description
                 {:columns
                  [{:column-name "_column_1",
                    :table-oid 0,
                    :column-attribute-number 0,
                    :column-oid 20,
                    :typlen 8,
                    :type-modifier -1,
                    :result-format :text}]}]
                [:msg-data-row {:vals ["1"]}]
                [:msg-command-complete {:command "SELECT 1"}]
                [:msg-ready {:status :idle}]]
               @!in-msgs)))))

(defn extended-query
  ([conn query] (extended-query conn query [] []))
  ([conn query param-oids args]
   (let [portal-name "pg-test"
         stmt-name "pg-test"]
     (pgwire/handle-msg conn {:msg-name :msg-parse :stmt-name stmt-name :query query :param-oids param-oids})
     (pgwire/handle-msg conn {:msg-name :msg-bind
                              :portal-name portal-name :stmt-name stmt-name
                              :arg-format (repeat (count args) :text)
                              ;; we are assuming strings for now
                              :args (map #(.getBytes ^String %) args)
                              ;; can be ommitted
                              :result-format nil})
     (pgwire/handle-msg conn {:msg-name :msg-describe :describe-type :prepared-stmt :describe-name stmt-name})
     (pgwire/handle-msg conn {:msg-name :msg-execute :portal-name portal-name :limit 0})
     (pgwire/handle-msg conn {:msg-name :msg-sync}))))

(deftest test-extended-query
  (let [insert "INSERT INTO docs (_id, name) VALUES ('aln', $1)"
        param-types [(-> pg-types/pg-types :text :oid)]
        param-values ["alan"]
        query "SELECT * FROM docs"
        {:keys [!in-msgs] :as frontend} (->recording-frontend)]
    (with-open [conn (->conn frontend {"user" "xtdb"
                                       "database" "xtdb"})]
      (reset! !in-msgs [])
      (extended-query conn insert param-types param-values)


      (t/is (= [[:msg-parse-complete]
                [:msg-bind-complete]
                [:msg-parameter-description {:parameter-oids [25]}]
                [:msg-no-data]
                [:msg-command-complete {:command "INSERT 0 0"}]
                [:msg-ready {:status :idle}]]
               @!in-msgs))

      (reset! !in-msgs [])
      (extended-query conn query)

      (t/is (= [[:msg-parse-complete]
                [:msg-bind-complete]
                [:msg-parameter-description {:parameter-oids []}]
                [:msg-row-description
                 {:columns
                  [{:column-name "_id",
                    :table-oid 0,
                    :column-attribute-number 0,
                    :column-oid 25,
                    :typlen -1,
                    :type-modifier -1,
                    :result-format :text}
                   {:column-name "name",
                    :table-oid 0,
                    :column-attribute-number 0,
                    :column-oid 25,
                    :typlen -1,
                    :type-modifier -1,
                    :result-format :text}]}]
                [:msg-data-row {:vals ["aln" "alan"]}]
                [:msg-command-complete {:command "SELECT 1"}]
                [:msg-ready {:status :idle}]]
               @!in-msgs)))))

(deftest test-wrong-param-encoding-3653
  (let [param-types [(-> pg-types/pg-types :timestamp :oid)]
        param-values ["alan"]
        query "SELECT $1 as v"
        {:keys [!in-msgs] :as frontend} (->recording-frontend)]
    (with-open [conn (->conn frontend {"user" "xtdb"
                                       "database" "xtdb"})]
      (reset! !in-msgs [])
      (extended-query conn query param-types param-values)

      (t/is (= [[:msg-parse-complete]
                [:msg-error-response
                 {:error-fields
                  {:severity "ERROR",
                   :localized-severity "ERROR",
                   :sql-state "22P02",
                   :message "invalid timestamp: Text 'alan' could not be parsed at index 0"
                   :detail {:arg-idx 0,
                            :category "cognitect.anomalies/incorrect",
                            :arg-format "text",
                            :code "xtdb.pgwire/invalid-arg-representation",
                            :message "invalid timestamp: Text 'alan' could not be parsed at index 0"}}}]
                [:msg-ready {:status :idle}]]
               @!in-msgs)))))

(deftest test-simple-query-with-params-fails-3605
  (let [{:keys [!in-msgs] :as frontend} (->recording-frontend)]
    (with-open [conn (->conn frontend {"user" "xtdb"
                                       "database" "xtdb"})]
      (reset! !in-msgs [])

      (pgwire/handle-msg conn {:msg-name :msg-simple-query, :query "SELECT $1"})

      (t/is (= [[:msg-error-response
                 {:error-fields
                  {:severity "ERROR",
                   :localized-severity "ERROR",
                   :sql-state "08P01",
                   :message "Parameters not allowed in simple queries"
                   :detail nil}}]
                [:msg-ready {:status :idle}]]
               @!in-msgs)))))

(defn ->utf8-col [col-name]
  {:column-name col-name,
   :table-oid 0,
   :column-attribute-number 0,
   :column-oid 20,
   :typlen 8,
   :type-modifier -1,
   :result-format :text})

(deftest test-multi-stmts
  ;; all of the tools try to be too helpful here, so we have to go low-level

  (letfn [(test [q]
            (let [{:keys [!in-msgs] :as frontend} (->recording-frontend)]
              (with-open [conn (->conn frontend {"user" "xtdb"
                                                 "database" "xtdb"})]
                (reset! !in-msgs [])
                (pgwire/handle-msg* conn {:msg-name :msg-simple-query, :query q})
                @!in-msgs)))]

    (t/testing "two selects"
      (t/is (= [[:msg-row-description {:columns [(->utf8-col "one")]}]
                [:msg-data-row {:vals ["1"]}]
                [:msg-command-complete {:command "SELECT 1"}]

                [:msg-row-description {:columns [(->utf8-col "two")]}]
                [:msg-data-row {:vals ["2"]}]
                [:msg-command-complete {:command "SELECT 1"}]
                [:msg-ready {:status :idle}]]

               (test "SELECT 1 one; SELECT 2 two;"))))

    (t/testing "with COMMIT"
      (t/is (= [[:msg-command-complete {:command "INSERT 0 0"}]
                [:msg-command-complete {:command "COMMIT"}]
                [:msg-row-description {:columns [(->utf8-col "_id")]}]
                [:msg-data-row {:vals ["1"]}]
                [:msg-command-complete {:command "SELECT 1"}]
                [:msg-ready {:status :idle}]]

               (test "INSERT INTO foo RECORDS {_id: 1}; COMMIT; SELECT * FROM foo;"))))

    (t/testing "query then DML fails"
      (t/is (= [[:msg-row-description {:columns [(->utf8-col "one")]}]
                [:msg-data-row {:vals ["1"]}]
                [:msg-command-complete {:command "SELECT 1"}]
                [:msg-error-response {:error-fields
                                      {:severity "ERROR", :localized-severity "ERROR", :sql-state "08P01",
                                       :message "DML is not allowed in a READ ONLY transaction"
                                       :detail {:category "cognitect.anomalies/incorrect",
                                                :code "xtdb/dml-in-read-only-tx",
                                                :query "INSERT INTO foo RECORDS {_id: 1}",
                                                :message "DML is not allowed in a READ ONLY transaction"}}}]
                [:msg-ready {:status :idle}]]

               (test "SELECT 1 one; INSERT INTO foo RECORDS {_id: 1}"))))

    (t/testing "DML then query fails"
      (t/is (= [[:msg-command-complete {:command "INSERT 0 0"}]
                [:msg-row-description {:columns [(->utf8-col "one")]}]
                [:msg-error-response {:error-fields
                                      {:severity "ERROR", :localized-severity "ERROR", :sql-state "08P01",
                                       :message "Queries are unsupported in a DML transaction"
                                       :detail {:category "cognitect.anomalies/incorrect",
                                                :code "xtdb/queries-in-read-write-tx",
                                                :query "SELECT 1 one",
                                                :message "Queries are unsupported in a DML transaction"}}}]
                [:msg-ready {:status :idle}]]

               (test "INSERT INTO foo RECORDS {_id: 1}; SELECT 1 one"))))

    (t/testing "leaves an explicit transaction open"
      (t/is (= [[:msg-command-complete {:command "BEGIN"}]
                [:msg-row-description {:columns [(->utf8-col "one")]}]
                [:msg-data-row {:vals ["1"]}]
                [:msg-command-complete {:command "SELECT 1"}]
                [:msg-ready {:status :transaction}]]
               (test "BEGIN; SELECT 1 one;"))))

    (t/testing "leaves an explicit transaction open even on failure"
      (t/is (= [[:msg-command-complete {:command "BEGIN"}]
                [:msg-row-description {:columns [(->utf8-col "boom")]}]
                [:msg-error-response {:error-fields
                                      {:severity "ERROR", :localized-severity "ERROR", :sql-state "08P01"
                                       :message "data exception - division by zero"
                                       :detail {:category "cognitect.anomalies/incorrect",
                                                :code "xtdb.expression/division-by-zero",
                                                :message "data exception - division by zero"}}}]
                [:msg-ready {:status :failed-transaction}]]
               (test "BEGIN; SELECT 1/0 boom;"))))

    (t/testing "can't BEGIN after a query"
      (t/is (= [[:msg-row-description {:columns [(->utf8-col "one")]}]
                [:msg-data-row {:vals ["1"]}]
                [:msg-command-complete {:command "SELECT 1"}]
                [:msg-error-response {:error-fields
                                      {:severity "ERROR", :localized-severity "ERROR", :sql-state "08P01",
                                       :message "transaction already started"
                                       :detail nil}}]
                [:msg-ready {:status :idle}]]
               (test "SELECT 1 one; BEGIN"))))))
