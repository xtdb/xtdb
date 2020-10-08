(ns crux.bench
  (:require [crux.io :as cio]
            [crux.kafka.embedded :as ek]
            [crux.rocksdb :as rocks]
            [crux.lmdb :as lmdb]
            [crux.jdbc :as jdbc]
            [crux.api :as api]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clj-http.client :as client]
            [crux.fixtures :as f]
            [crux.bus :as bus]
            [crux.kv :as kv]
            [crux.kafka :as k]
            [crux.jdbc :as j])
  (:import (java.util.concurrent Executors ExecutorService)
           (java.util UUID Date List)
           (java.time Duration Instant)
           (java.io Closeable File)
           (software.amazon.awssdk.services.s3 S3Client)
           (software.amazon.awssdk.services.s3.model GetObjectRequest PutObjectRequest)
           (software.amazon.awssdk.core.sync RequestBody)
           (software.amazon.awssdk.core.exception SdkClientException)
           software.amazon.awssdk.regions.Region
           (com.amazonaws.services.logs AWSLogsClient AWSLogsClientBuilder)
           (com.amazonaws.services.logs.model StartQueryRequest StartQueryResult GetQueryResultsRequest GetQueryResultsResult ResultField)
           (com.amazonaws.services.simpleemail AmazonSimpleEmailService AmazonSimpleEmailServiceClient AmazonSimpleEmailServiceClientBuilder)
           (com.amazonaws.services.simpleemail.model Body Content Destination Message SendEmailRequest)
           (crux.bus EventBus)))


(def commit-hash
  (System/getenv "COMMIT_HASH"))

(def crux-version
  (when-let [pom-file (io/resource "META-INF/maven/juxt/crux-core/pom.properties")]
    (with-open [in (io/reader pom-file)]
      (get (cio/load-properties in) "version"))))

(def ^:dynamic ^:private *bench-ns*)
(def ^:dynamic ^:private *bench-dimensions* {})
(def ^:dynamic ^:private *!bench-results*)

(defn with-dimensions* [dims f]
  (binding [*bench-dimensions* (merge *bench-dimensions* dims)]
    (f)))

(defmacro with-dimensions [dims & body]
  `(with-dimensions* ~dims (fn [] ~@body)))

(defmacro with-crux-dimensions [& body]
  `(with-dimensions {:crux-version crux-version, :crux-commit commit-hash}
     ~@body))

(defn with-timing* [f]
  (let [start-time-ms (System/currentTimeMillis)
        ret (try
              (f)
              (catch Exception e
                (log/errorf e "caught exception during '%s':" *bench-ns*)
                {:error (str e)}))]
    (merge (when (map? ret) ret)
           {:time-taken-ms (- (System/currentTimeMillis) start-time-ms)})))

(defmacro with-timing [& body]
  `(with-timing* (fn [] ~@body)))

(defn with-additional-index-metrics* [node f]
  (let [!index-metrics (atom {:av-count 0
                              :bytes-indexed 0
                              :doc-count 0})]
    (bus/listen (:bus node)
                {:crux/event-types #{:crux.tx/indexed-docs}}
                (fn [{:keys [doc-ids av-count bytes-indexed]}]
                  (swap! !index-metrics (fn [index-metrics-map]
                                          (-> index-metrics-map
                                              (update :av-count + av-count)
                                              (update :bytes-indexed + bytes-indexed)
                                              (update :doc-count + (count doc-ids)))))))
    (let [results (f)]
      (assoc results
             :av-count (:av-count @!index-metrics)
             :bytes-indexed (:bytes-indexed @!index-metrics)
             :doc-count (:doc-count @!index-metrics)))))

(defmacro with-additional-index-metrics [node & body]
  `(with-additional-index-metrics* ~node (fn [] ~@body)))

(defn run-bench* [bench-type f]
  (log/infof "running bench '%s/%s'..." *bench-ns* (name bench-type))

  (let [ret (with-timing (f))

        res (merge (when (map? ret) ret)
                   *bench-dimensions*
                   {:bench-type bench-type})]

    (log/infof "finished bench '%s/%s'." *bench-ns* (name bench-type))

    (swap! *!bench-results* conj res)
    res))

(defmacro run-bench {:style/indent 1} [bench-type & body]
  `(run-bench* ~bench-type (fn [] ~@body)))

(defn compact-node [node]
  (run-bench :compaction
    (let [pre-compact-bytes (:crux.kv/size (api/status node))]
      (kv/compact (get-in node [:index-store :kv-store]))

      {:bytes-on-disk pre-compact-bytes
       :compacted-bytes-on-disk (:crux.kv/size (api/status node))})))

(defn post-to-slack [message]
  (if-let [slack-url (System/getenv "SLACK_URL")]
    (try
      (client/post (-> slack-url
                       (json/read-str)
                       (get "slack-url"))
                   {:body (json/write-str {:text message})
                    :content-type :json})
      (catch Exception e
        (println "Failed to post to slack, error: " e)))

    (println "Would post to Slack:\n" message)))

;; From https://rosettacode.org/wiki/Sparkline_in_unicode#Clojure
(defn sparkline [nums]
  (let [sparks   "▁▂▃▄▅▆▇█"
        high     (apply max nums)
        low      (apply min nums)
        spread   (- high low)
        quantize #(Math/round (* 7.0 (/ (- % low) (max spread 1))))]
        (apply str (map #(nth sparks (quantize %)) nums))))

(defn- result->slack-message [{:keys [time-taken-ms error bench-type percentage-difference-since-last-run
                                      minimum-time-taken-this-week maximum-time-taken-this-week times-taken
                                      doc-count av-count bytes-indexed]
                               :as bench-map}]
  (->> (concat [(format "*%s* (%s, *%s%%*. 7D Min: %s, 7D Max: %s, Trend: %s): `%s`"
                        (name bench-type)
                        (Duration/ofMillis time-taken-ms)
                        (if (neg? percentage-difference-since-last-run)
                          (format "%.2f" percentage-difference-since-last-run)
                          (format "+%.2f" percentage-difference-since-last-run))
                        (Duration/ofMillis minimum-time-taken-this-week)
                        (Duration/ofMillis maximum-time-taken-this-week)
                        (sparkline times-taken)
                        (let [time-taken-seconds (/ time-taken-ms 1000)]
                          (pr-str (dissoc bench-map :bench-ns :bench-type :crux-node-type :crux-commit :crux-version :time-taken-ms
                                          :percentage-difference-since-last-run :minimum-time-taken-this-week :maximum-time-taken-this-week :times-taken))))]
               (when (and (= bench-type :ingest) doc-count av-count bytes-indexed)
                 (->> (let [time-taken-seconds (/ time-taken-ms 1000)]
                        {:docs-per-second (int (/ doc-count time-taken-seconds))
                         :avs-per-second (int (/ av-count time-taken-seconds))
                         :bytes-indexed-per-second (int (/ bytes-indexed time-taken-seconds))})
                      (map (fn [[k v]] (format "*%s*: %s" (name k) v))))))
       (string/join "\n")))

(defn results->slack-message [results]
  (format "*%s* (%s)\n========\n%s\n"
          (:bench-ns (first results))
          (:crux-node-type (first results))
          (->> results
               (map result->slack-message)
               (string/join "\n"))))

(defn- result->html [{:keys [time-taken-ms bench-type percentage-difference-since-last-run
                             minimum-time-taken-this-week maximum-time-taken-this-week times-taken
                             doc-count av-count bytes-indexed] :as bench-map}]
  (->> (concat [(format "<p> <b>%s</b> (%s, %s. 7D Min: %s, 7D Max: %s, Trend: %s): <code>%s</code></p>"
                        (name bench-type)
                        (Duration/ofMillis time-taken-ms)
                        (if (neg? percentage-difference-since-last-run)
                          (format "<b style=\"color: green\">%.2f%%</b>" percentage-difference-since-last-run)
                          (format "<b style=\"color: red\">+%.2f%%</b>" percentage-difference-since-last-run))
                        (Duration/ofMillis minimum-time-taken-this-week)
                        (Duration/ofMillis maximum-time-taken-this-week)
                        (sparkline times-taken)
                        (pr-str (dissoc bench-map :bench-ns :bench-type :crux-node-type :crux-commit :crux-version :time-taken-ms
                                        :percentage-difference-since-last-run :minimum-time-taken-this-week :maximum-time-taken-this-week :times-taken)))]
               (when (= bench-type :ingest)
                 (->> (let [time-taken-seconds (/ time-taken-ms 1000)]
                        {:docs-per-second (int (/ doc-count time-taken-seconds))
                         :avs-per-second (int (/ av-count time-taken-seconds))
                         :bytes-indexed-per-second (int (/ bytes-indexed time-taken-seconds))})
                      (map (fn [[k v]] (format "<p><b>%s</b>: <code>%s</code></p>" (name k) v))))))

       (string/join " ")))

(defn results->email [bench-results]
  (str "<h1>Crux bench results</h1>"
       (->> (for [[bench-ns results] (group-by :bench-ns bench-results)]
              (str (format "<h2>%s</h2>" bench-ns)
                   (->> (for [[crux-node-type results] (group-by :crux-node-type results)]
                          (format "<h3>%s</h3> %s"
                                  crux-node-type
                                  (->> results
                                       (map result->html)
                                       (string/join " "))))
                        (string/join))))
            (string/join))))

(defn with-bench-ns* [bench-ns f]
  (log/infof "running bench-ns '%s'..." bench-ns)

  (binding [*bench-ns* bench-ns
            *!bench-results* (atom [])]
    (with-dimensions {:bench-ns bench-ns}
      (f))

    (log/infof "finished bench-ns '%s'." bench-ns)

    (let [results @*!bench-results*]
      (run! (comp println json/write-str) results)
      results)))

(defmacro with-bench-ns [bench-ns & body]
  `(with-bench-ns* ~bench-ns (fn [] ~@body)))

(def nodes
  {"standalone-rocksdb"
   (fn [data-dir]
     {:crux/tx-log {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "tx-log")}}
      :crux/document-store {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "doc-store")}}
      :crux/index-store {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "indexes")}}
      :crux.metrics.cloudwatch/reporter {}})

   "standalone-rocksdb-with-metrics"
   (fn [data-dir]
     {:crux/tx-log {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "tx-log")}}
      :crux/document-store {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "doc-store")}}
      :crux/index-store {:kv-store {:crux/module `rocks/->kv-store
                                    :db-dir (io/file data-dir "indexes")
                                    :metrics `crux.rocksdb.metrics/->metrics}}
      :crux.metrics.cloudwatch/reporter {}})

   "h2-rocksdb"
   (fn [data-dir]
     {::j/connection-pool {:dialect 'crux.jdbc.h2/->dialect
                           :db-spec {:dbname (str (io/file data-dir "h2"))}}
      :crux/tx-log {:crux/module `jdbc/->tx-log, :connection-pool ::j/connection-pool}
      :crux/document-store {:crux/module `jdbc/->tx-log, :connection-pool ::j/connection-pool}
      :crux/index-store {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "indexes")}}
      :crux.metrics.cloudwatch/reporter {}})

   "sqlite-rocksdb"
   (fn [data-dir]
     {::j/connection-pool {:dialect 'crux.jdbc.sqlite/->dialect
                           :db-spec {:dbname (str (io/file data-dir "sqlite"))}}
      :crux/tx-log {:crux/module `jdbc/->tx-log, :connection-pool ::j/connection-pool}
      :crux/document-store {:crux/module `jdbc/->tx-log, :connection-pool ::j/connection-pool}
      :crux/index-store {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "indexes")}}
      :crux.metrics.cloudwatch/reporter {}})

   "kafka-rocksdb"
   (fn [data-dir]
     (let [uuid (UUID/randomUUID)]
       {::k/kafka-config {:bootstrap-servers "localhost:9092"}
        :crux/tx-log {:crux/module `k/->tx-log
                      :kafka-config ::k/kafka-config
                      :tx-topic-opts {:topic-name (str "kafka-rocksdb-tx-" uuid)}}
        :crux/document-store {:crux/module `k/->document-store,
                              :kafka-config ::k/kafka-config
                              :doc-topic-opts {:topic-name (str "kafka-rocksdb-doc-" uuid)}
                              :local-document-store {:kv-store {:crux/module `rocks/->kv-store
                                                                :db-dir (io/file data-dir "doc-store")}}}
        :crux/index-store {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "index-store")}}
        :crux.metrics.cloudwatch/reporter {}}))

   "embedded-kafka-rocksdb"
   (fn [data-dir]
     (let [uuid (UUID/randomUUID)]
       {::k/kafka-config {:bootstrap-servers "localhost:9091"}
        :crux/tx-log {:crux/module `k/->tx-log
                      :kafka-config ::k/kafka-config
                      :tx-topic-opts {:topic-name (str "kafka-rocksdb-tx-" uuid)}}
        :crux/document-store {:crux/module `k/->document-store
                              :kafka-config ::k/kafka-config
                              :doc-topic-opts {:topic-name (str "kafka-rocksdb-doc-" uuid)}
                              :local-document-store {:kv-store {:crux/module `rocks/->kv-store
                                                                :db-dir (io/file data-dir "doc-store")}}}
        :crux/index-store {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "index-store")}}
        :crux.metrics.cloudwatch/reporter {}}))

   "standalone-lmdb"
   (fn [data-dir]
     {:crux/tx-log {:kv-store {:crux/module `lmdb/->kv-store, :db-dir (io/file data-dir "tx-log")}}
      :crux/document-store {:kv-store {:crux/module `lmdb/->kv-store, :db-dir (io/file data-dir "doc-store")}}
      :crux/index-store {:kv-store {:crux/module `lmdb/->kv-store, :db-dir (io/file data-dir "indexes")}}
      :crux.metrics.cloudwatch/reporter {}})

   "kafka-lmdb"
   (fn [data-dir]
     (let [uuid (UUID/randomUUID)]
       {::k/kafka-config {:bootstrap-servers "localhost:9092"}
        :crux/tx-log {:crux/module `k/->tx-log
                      :kafka-config ::k/kafka-config
                      :tx-topic-opts {:topic-name (str "kafka-lmdb-tx-" uuid)}}
        :crux/document-store {:crux/module `k/->document-store
                              :kafka-config ::k/kafka-config
                              :doc-topic-opts {:topic-name (str "kafka-lmdb-doc-" uuid)}
                              :local-document-store {:kv-store {:crux/module `lmdb/->kv-store
                                                                :db-dir (io/file data-dir "doc-store")}}}
        :crux/index-store {:kv-store {:crux/module `lmdb/->kv-store, :db-dir (io/file data-dir "index-store")}}
        :crux.metrics.cloudwatch/reporter {}}))})

(defn with-embedded-kafka* [f]
  (f/with-tmp-dir "embedded-kafka" [data-dir]
    (with-open [emb (ek/start-embedded-kafka
                     #::ek{:zookeeper-data-dir (str (io/file data-dir "zookeeper"))
                           :kafka-log-dir (str (io/file data-dir "kafka-log"))
                           :kafka-port 9091})]
      (f))))

(defmacro with-embedded-kafka [& body]
  `(with-embedded-kafka* (fn [] ~@body)))

(defn with-nodes* [nodes f]
  (->> (for [[node-type ->node] nodes]
         (f/with-tmp-dir "crux-node" [data-dir]
           (with-open [node (api/start-node (->node data-dir))]
             (with-dimensions {:crux-node-type node-type}
               (log/infof "Running bench on %s node." node-type)
               (f node)))))
       (apply concat)
       (vec)))

(defmacro with-nodes [[node-binding nodes] & body]
  `(with-nodes* ~nodes (fn [~node-binding] ~@body)))

(def ^:private num-processors
  (.availableProcessors (Runtime/getRuntime)))

(defn with-thread-pool [{:keys [num-threads], :or {num-threads num-processors}} f args]
  (let [^ExecutorService pool (Executors/newFixedThreadPool num-threads)]
    (with-dimensions {:num-threads num-threads}
      (try
        (let [futures (->> (for [arg args]
                             (let [^Callable job (bound-fn [] (f arg))]
                               (.submit pool job)))
                           doall)]

          (mapv deref futures))

        (finally
          (.shutdownNow pool))))))

(defn save-to-file [file results]
  (with-open [w (io/writer file)]
    (doseq [res results]
      (.write w (prn-str res)))))

(defn- generate-s3-filename [database version]
  (let [formatted-date (->> (java.util.Date.)
                            (.format (java.text.SimpleDateFormat. "yyyyMMdd-HHmmss")))]
    (format "%s-%s/%s-%sZ.edn" database version database formatted-date)))

(def s3-client
  (delay
    (-> (S3Client/builder)
        (.region Region/EU_WEST_2)
        (.build))))

(defn save-to-s3 [{:keys [database version]} ^File file]
  (try
    (.putObject ^S3Client @s3-client
                (-> (PutObjectRequest/builder)
                    (.bucket "crux-bench")
                    (.key (generate-s3-filename database version))
                    ^PutObjectRequest (.build))
                (RequestBody/fromFile file))
    (catch SdkClientException e
      "AWS credentials not found! Results file not saved.")))

(defn load-from-s3 [key]
  (try
    (.getObject ^S3Client @s3-client
                (-> (GetObjectRequest/builder)
                    (.bucket "crux-bench")
                    (.key key)
                    ^GetObjectRequest (.build)))
    (catch SdkClientException e
      (log/warn (format "AWS credentials not found! File %s not loaded" key)))))

(def log-client
  (delay
    (try
      (AWSLogsClientBuilder/defaultClient)
      (catch com.amazonaws.SdkClientException e
        (log/info "AWS credentials not found! Cannot get comparison times.")))))

(defn get-comparison-times [results]
  (let [query-requests (for [{:keys [crux-node-type bench-type bench-ns time-taken-ms] :as result} results]
                         (let [query-id (-> (.startQuery ^AWSLogsClient @log-client
                                                         (-> (StartQueryRequest.)
                                                             (.withLogGroupName "crux-bench")
                                                             (.withQueryString (format  "fields `time-taken-ms` | filter `crux-node-type` = '%s' | filter `bench-type` = '%s' | filter `bench-ns` = '%s' | sort @timestamp desc"
                                                                                        crux-node-type (name bench-type) (name bench-ns)))
                                                             (.withStartTime (-> (Date.)
                                                                                 (.toInstant)
                                                                                 (.minus (Duration/ofDays 7))
                                                                                 (.toEpochMilli)))
                                                             (.withEndTime (.getTime (Date.)))))
                                            (.getQueryId))]
                           (-> (GetQueryResultsRequest.)
                               (.withQueryId query-id))))]

    (while (not-any? (fn [query-request]
                       (= "Complete"
                          (->> (.getQueryResults ^AWSLogsClient @log-client query-request)
                               (.getStatus))))
                     query-requests)
      (Thread/sleep 100))

    (map (fn [query-request]
           (->> (map first (-> (.getQueryResults ^AWSLogsClient @log-client query-request)
                               (.getResults)))
                (map #(.getValue ^ResultField %))
                (map #(Integer/parseInt %))))
         query-requests)))

(defn with-comparison-times [results]
  (let [comparison-times (get-comparison-times results)]
    (map (fn [{:keys [time-taken-ms] :as result} times-taken]
           (if (empty? times-taken)
             (assoc
              result
              :percentage-difference-since-last-run (float 0)
              :minimum-time-taken-this-week 0
              :maximum-time-taken-this-week 0
              :times-taken [time-taken-ms])
             (assoc
              result
              :percentage-difference-since-last-run (-> time-taken-ms
                                                        (- (first times-taken))
                                                        (/ (first times-taken))
                                                        (* 100)
                                                        (float))
              :minimum-time-taken-this-week (apply min times-taken)
              :maximum-time-taken-this-week (apply max times-taken)
              :times-taken (conj (vec (reverse times-taken)) time-taken-ms))))
         results
         comparison-times)))

(defn send-email-via-ses [message]
  (try
    (let [client (-> (AmazonSimpleEmailServiceClientBuilder/standard)
                     (.withRegion "eu-west-1")
                     ^AmazonSimpleEmailServiceClient (.build))

          email (-> (SendEmailRequest.)
                    (.withDestination (let [^List to-addresses [(string/replace "crux-bench at juxt.pro" " at " "@")]]
                                        (-> (Destination.)
                                            (.withToAddresses to-addresses))))
                    (.withMessage
                     (-> (Message.)
                         (.withBody (-> (Body.)
                                        (.withHtml (-> (Content.)
                                                       (.withCharset "UTF-8")
                                                       (.withData message)))))
                         (.withSubject (-> (Content.)
                                           (.withCharset "UTF-8")
                                           (.withData (str "Bench Results"))))))
                    (.withSource "crux-bench@juxt.pro"))]

      (.sendEmail client email))

    (catch Exception e
      (log/warn "Email failed to send! Error: " e))))
