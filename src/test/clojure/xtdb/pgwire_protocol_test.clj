(ns xtdb.pgwire-protocol-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.pgwire :as pgwire]
            [xtdb.test-util :as tu]
            [xtdb.types :as types])
  (:import [java.lang AutoCloseable]
           [java.nio.charset StandardCharsets]
           [java.time Clock]))

(def ^:dynamic ^:private *port* nil)

(defn with-port [f]
  (let [server (-> tu/*node* :system :xtdb.pgwire/server)]
    (binding [*port* (:port server)]
      (f))))

(t/use-fixtures :each tu/with-mock-clock tu/with-node with-port)

(defn bytes->str [^bytes arr]
  (String. arr StandardCharsets/UTF_8))

(defrecord RecordingFrontend [!msgs]
  pgwire/Frontend
  (send-client-msg! [_ msg-def]
    (swap! !msgs conj [(:name msg-def)])
    nil)

  (send-client-msg! [_ msg-def data]
    (let [data (case (:name msg-def)
                 :msg-data-row (update data :vals (partial mapv bytes->str))
                 data)]
      (swap! !msgs conj [(:name msg-def) data]))
    nil)

  AutoCloseable
  (close [_]))

(defn ->recording-frontend []
  (->RecordingFrontend (atom [])))

(defn ->conn
  (^java.lang.AutoCloseable [frontend] (->conn frontend {}))

  (^java.lang.AutoCloseable [frontend startup-params]
   (doto (pgwire/map->Connection {:server {:server-state (atom {:parameters {"server_encoding" "UTF8"
                                                                             "client_encoding" "UTF8"
                                                                             "DateStyle" "ISO"
                                                                             "IntervalStyle" "ISO_8601"}})}
                                  :frontend frontend
                                  :node tu/*node*
                                  :cid -1
                                  :!closing? (future false)
                                  :conn-state (atom {:session {:clock (Clock/systemUTC)}})})
     (pgwire/cmd-startup-pg30 startup-params))))

(deftest test-startup
  (let [{:keys [!msgs] :as frontend} (->recording-frontend)]
    (with-open [_ (->conn frontend {"user" "xtdb"
                                    "database" "xtdb"})]
      (t/is (= [[:msg-auth {:result 0}]
                [:msg-parameter-status {:parameter "server_encoding", :value "UTF8"}]
                [:msg-parameter-status {:parameter "client_encoding", :value "UTF8"}]
                [:msg-parameter-status {:parameter "datestyle", :value "ISO"}]
                [:msg-parameter-status {:parameter "intervalstyle", :value "ISO_8601"}]
                [:msg-parameter-status {:parameter "user", :value "xtdb"}]
                [:msg-parameter-status {:parameter "database", :value "xtdb"}]
                [:msg-backend-key-data {:process-id -1, :secret-key 0}]
                [:msg-ready {:status :idle}]]
               @!msgs)))))

(deftest test-simple-query
  (let [{:keys [!msgs] :as frontend} (->recording-frontend)]
    (with-open [conn (->conn frontend {"user" "xtdb"
                                       "database" "xtdb"})]
      (reset! !msgs [])

      (pgwire/cmd-simple-query conn {:query "SELECT 1"})


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
               @!msgs)))))

(defn extended-query
  ([conn query] (extended-query conn query [] []))
  ([conn query arg-types param-values]
   (let [portal-name "pg-test"
         stmt-name "pg-test"]
     (pgwire/handle-msg conn {:msg-name :msg-parse :stmt-name stmt-name :query query :arg-types arg-types})
     (pgwire/handle-msg conn {:msg-name :msg-bind
                              :portal-name portal-name :stmt-name stmt-name
                              :param-format (repeat (count param-values) 0)
                              ;; we are assuming strings for now
                              :params (map #(.getBytes %) param-values)
                              ;; can be ommitted
                              :result-format nil})
     (pgwire/handle-msg conn {:msg-name :msg-describe :describe-type :prepared-stmt :describe-name stmt-name})
     (pgwire/handle-msg conn {:msg-name :msg-execute :portal-name portal-name})
     (pgwire/handle-msg conn {:msg-name :msg-sync}))))

(deftest test-extended-query
  (let [insert "INSERT INTO docs (_id, name) VALUES ('aln', $1)"
        param-types [(-> types/pg-types :text :oid)]
        param-values ["alan"]
        query "SELECT * FROM docs"
        {:keys [!msgs] :as frontend} (->recording-frontend)     ]
    (with-open [conn (->conn frontend {"user" "xtdb"
                                       "database" "xtdb"})]
      (reset! !msgs [])
      (extended-query conn insert param-types param-values)


      (t/is (= [[:msg-parse-complete]
                [:msg-bind-complete]
                [:msg-parameter-description {:parameter-oids [25]}]
                [:msg-no-data]
                [:msg-command-complete {:command "INSERT 0 0"}]
                [:msg-ready {:status :idle}]]
               @!msgs))

      (reset! !msgs [])
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
               @!msgs)))))

(deftest test-wrong-param-encoding-3653
  (let [param-types [(-> types/pg-types :timestamp :oid)]
        param-values ["alan"]
        query "SELECT $1 as v"
        {:keys [!msgs] :as frontend} (->recording-frontend)]
    (with-open [conn (->conn frontend {"user" "xtdb"
                                       "database" "xtdb"})]
      (reset! !msgs [])
      (extended-query conn query param-types param-values)

      (t/is (= [[:msg-parse-complete]
                [:msg-error-response
                 {:error-fields
                  {:severity "ERROR",
                   :localized-severity "ERROR",
                   :sql-state "22P02",
                   :message "Can not parse 'alan' as timestamp"}}]
                [:msg-ready {:status :idle}]]
               @!msgs)))))
