(ns xtdb.tracer-test
  (:require [clojure.string :as string]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.pgwire-test :as pgw-test])
  (:import (io.opentelemetry.api.common Attributes AttributeKey)
           (io.opentelemetry.sdk.common CompletableResultCode)
           (io.opentelemetry.sdk.trace.data SpanData)
           (io.opentelemetry.sdk.trace.export SimpleSpanProcessor SpanExporter)))

(defn span-attributes->map [^Attributes attributes]
  (->> (.asMap attributes)
       (map (fn [[^AttributeKey k v]] [(.getKey k) v]))
       (remove (fn [[k _]] (re-find #"time" k))) ;; remove any timing-related keys
       (into {})))

(defn span-data->res [^SpanData span]
  {:name (.getName span)
   :attributes (span-attributes->map (.getAttributes span))
   :parent-id (.getParentSpanId span)
   :span-id (.getSpanId span)
   :start-nanos (.getStartEpochNanos span)})

(defn- test-span-exporter [!spans]
  (reify SpanExporter
    (export [_ span-data]
      (swap! !spans concat (mapv span-data->res span-data))
      (CompletableResultCode/ofSuccess))
    (flush [_] (CompletableResultCode/ofSuccess))
    (shutdown [_] (CompletableResultCode/ofSuccess))))

(defn build-span-tree [spans]
  ;; Group spans by parent span ID
  (let [spans-by-parent (group-by :parent-id spans)]

    (letfn [(attach-children [span]
              ;; Recursively attach children, sorted by start time
              (let [child-spans (sort-by :start-nanos (spans-by-parent (:span-id span)))
                    children (mapv attach-children child-spans)]
                (-> span
                    (dissoc :span-id :parent-id :start-nanos)
                    (assoc :children children))))]

      ;; Start from root spans (those with no parent)
      (mapv attach-children (sort-by :start-nanos (spans-by-parent "0000000000000000"))))))

(t/deftest test-query-creates-span
  (t/testing "running a query creates a pgwire.query span"
    (let [!spans (atom [])
          exporter (test-span-exporter !spans)
          span-processor (SimpleSpanProcessor/create exporter)]
      (with-open [node (xtn/start-node
                        {:tracer {:enabled? true
                                  :service-name "xtdb-test"
                                  :span-processor span-processor}})]

        (xt/q node "SELECT 1")
        ;; Give spans a moment to be exported
        (Thread/sleep 100)

        (t/is (= [{:name "xtdb.query"
                   :attributes {"query.text" "SELECT 1"}
                   :children
                   [{:name "query.cursor.project"
                     :attributes {"cursor.page_count" "1"
                                  "cursor.row_count" "1"
                                  "cursor.type" "project"}
                     :children []}
                    {:name "query.cursor.table"
                     :attributes {"cursor.page_count" "1"
                                  "cursor.row_count" "1"
                                  "cursor.type" "table"}
                     :children []}]}]
                 (build-span-tree @!spans)))))))

(t/deftest ensure-pgwire-simple-query-behaves-with-traces
  (let [!spans (atom [])
        exporter (test-span-exporter !spans)
        span-processor (SimpleSpanProcessor/create exporter)]
    (with-open [node (xtn/start-node {:tracer {:enabled? true
                                               :service-name "xtdb-test"
                                               :span-processor span-processor}})]
      (binding [pgw-test/*port* (.getServerPort node)
                pgw-test/*server* node]
        (with-open [conn (pgw-test/jdbc-conn {"preferQueryMode" "simple"})
                    stmt (.createStatement conn)
                    rs (.executeQuery stmt "SELECT 1")]
          (t/is (= [{"_column_1" 1}] (pgw-test/rs->maps rs)))

          ;; Give spans a moment to be exported
          (Thread/sleep 100)

          (t/is (= [{:name "xtdb.query"
                     :attributes {"query.text" "SELECT 1"}
                     :children
                     [{:name "query.cursor.project"
                       :attributes {"cursor.page_count" "1"
                                    "cursor.row_count" "1"
                                    "cursor.type" "project"}
                       :children []}
                      {:name "query.cursor.table"
                       :attributes {"cursor.page_count" "1"
                                    "cursor.row_count" "1"
                                    "cursor.type" "table"}
                       :children []}]}]
                   (build-span-tree @!spans))))))))

(t/deftest test-scan-span-includes-table-name
  (t/testing "scan span includes the table name as an attribute"
    (let [!spans (atom [])
          exporter (test-span-exporter !spans)
          span-processor (SimpleSpanProcessor/create exporter)]
      (with-open [node (xtn/start-node
                        {:tracer {:enabled? true
                                  :service-name "xtdb-test"
                                  :span-processor span-processor}})]

        (xt/submit-tx node [[:put-docs :foo {:xt/id 1 :name "bar"}]])
        (xt/q node "SELECT * FROM foo")
        ;; Give spans a moment to be exported
        (Thread/sleep 100)

        (let [span-tree (build-span-tree @!spans)
              scan-spans (->> span-tree
                              (mapcat (fn collect-scan [span]
                                        (concat
                                         (when (string/includes? (:name span) "query.cursor.scan") [span])
                                         (mapcat collect-scan (:children span))))))
              scan-span (first scan-spans)] 
          (t/is (= "query.cursor.scan.foo" (:name scan-span)))
          (t/is (= "foo" (get-in scan-span [:attributes "table.name"])))
          (t/is (= "xtdb" (get-in scan-span [:attributes "db.name"]))))))))

(t/deftest test-transaction-tracing
  (t/testing "multiple SQL operations in one transaction create separate spans"
    (let [!spans (atom [])
          exporter (test-span-exporter !spans)
          span-processor (SimpleSpanProcessor/create exporter)]
      (with-open [node (xtn/start-node
                        {:tracer {:enabled? true
                                  :service-name "xtdb-test"
                                  :span-processor span-processor}})]

        (xt/submit-tx node [[:sql "INSERT INTO users (_id, name) VALUES (1, 'Alice')"]])
        (xt/submit-tx node [[:sql "UPDATE users SET foo='bar' WHERE name='alice'"]])
        (xt/submit-tx node [[:patch-docs :users {:xt/id "alice" :foo "baz"}]])
        (xt/submit-tx node [[:delete-docs :users 1]])
        (xt/submit-tx node [[:erase-docs :users 1]])

        ;; Give spans a moment to be exported
        (Thread/sleep 100)

        (let [span-tree (build-span-tree @!spans)
              tx-spans (filter #(= (:name %) "xtdb.transaction") span-tree)
              tx-child-spans (mapv #(first (:children %)) tx-spans)]
          (t/is (= 5 (count tx-spans)))
          ;; we may expect the patch/delete/erase to differ here if they go through delete-docs/erase-docs indexer
          (t/is (= ["xtdb.transaction.put-docs"
                    "xtdb.transaction.sql"
                    "xtdb.transaction.sql"
                    "xtdb.transaction.sql"
                    "xtdb.transaction.sql"]
                   (mapv :name tx-child-spans)))
          (let [[put-tx update-tx _patch-tx _delete-tx _erase-tx] tx-spans]
            (t/is (= {:name "xtdb.transaction"
                      :attributes {"operations.count" "1"}
                      :children
                      [{:name "xtdb.transaction.put-docs"
                        :attributes {"db" "xtdb"
                                     "schema" "public"
                                     "table" "users"},
                        :children []}]}
                     put-tx))
            (t/is (= {:name "xtdb.transaction"
                      :attributes {"operations.count" "1"}
                      :children
                      [{:name "xtdb.transaction.sql"
                        :attributes {"query.text" "UPDATE users SET foo='bar' WHERE name='alice'"},
                        :children
                        [{:name "xtdb.query"
                          :attributes {"query.text" "UPDATE users SET foo='bar' WHERE name='alice'"},
                          :children
                          [{:name "query.cursor.project"
                            :attributes {"cursor.page_count" "0" "cursor.row_count" "0" "cursor.type" "project"},
                            :children []}
                           {:name "query.cursor.project"
                            :attributes {"cursor.page_count" "0" "cursor.row_count" "0" "cursor.type" "project"},
                            :children []}
                           {:name "query.cursor.rename"
                            :attributes {"cursor.page_count" "0" "cursor.row_count" "0" "cursor.type" "rename"},
                            :children []}
                           {:name "query.cursor.select"
                            :attributes {"cursor.page_count" "0" "cursor.row_count" "0" "cursor.type" "select"},
                            :children []}
                           {:name "query.cursor.scan.users"
                            :attributes
                            {"cursor.page_count" "0",
                             "cursor.row_count" "0",
                             "cursor.type" "scan",
                             "db.name" "xtdb",
                             "schema.name" "public",
                             "table.name" "users"},
                            :children []}]}]}]}
                     update-tx))))))))

(t/deftest test-transaction-multiple-ops-tracing
  (t/testing "multiple SQL operations in one transaction create separate spans"
    (let [!spans (atom [])
          exporter (test-span-exporter !spans)
          span-processor (SimpleSpanProcessor/create exporter)]
      (with-open [node (xtn/start-node
                        {:tracer {:enabled? true
                                  :service-name "xtdb-test"
                                  :span-processor span-processor}})]

        (xt/submit-tx node [[:sql "INSERT INTO users (_id, name) VALUES (1, 'Alice')"]
                            [:sql "INSERT INTO users2 (_id, name) VALUES (2, 'Bob')"]
                            [:sql "INSERT INTO users3 (_id, name) VALUES (3, 'Charlie')"]])

        ;; Give spans a moment to be exported
        (Thread/sleep 100)

        (let [span-tree (build-span-tree @!spans)
              tx-span (first (filter #(= (:name %) "xtdb.transaction") span-tree))]
          (t/is (= {:name "xtdb.transaction"
                    :attributes {"operations.count" "3"}
                    :children
                    [{:name "xtdb.transaction.put-docs"
                      :attributes {"db" "xtdb"
                                   "schema" "public"
                                   "table" "users"},
                      :children []}
                     {:name "xtdb.transaction.put-docs"
                      :attributes {"db" "xtdb"
                                   "schema" "public"
                                   "table" "users2"},
                      :children []}
                     {:name "xtdb.transaction.put-docs"
                      :attributes {"db" "xtdb"
                                   "schema" "public"
                                   "table" "users3"},
                      :children []}]}
                   tx-span)))))))