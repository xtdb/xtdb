(ns crux.bench
  (:require [crux.io :as cio]
            [crux.kafka.embedded :as ek]
            [crux.api :as api]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clj-http.client :as client]
            [crux.fixtures :as f])
  (:import (java.util.concurrent Executors ExecutorService)
           (java.io Closeable)
           (software.amazon.awssdk.services.s3 S3Client)
           (software.amazon.awssdk.services.s3.model GetObjectRequest PutObjectRequest)
           (software.amazon.awssdk.core.sync RequestBody)
           (software.amazon.awssdk.core.exception SdkClientException)
           (com.amazonaws.services.simpleemail AmazonSimpleEmailService AmazonSimpleEmailServiceClientBuilder)
           (com.amazonaws.services.simpleemail.model Body Content Destination Message SendEmailRequest)))

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
                {:error (.getMessage e)}))]
    (merge (when (map? ret) ret)
           {:time-taken-ms (- (System/currentTimeMillis) start-time-ms)})))

(defmacro with-timing [& body]
  `(with-timing* (fn [] ~@body)))

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

(defn post-to-slack [message]
  (when-let [slack-url (System/getenv "SLACK_URL")]
    (client/post (-> slack-url
                     (json/read-str)
                     (get "slack-url"))
                 {:body (json/write-str {:text message})
                  :content-type :json})))

(defn- result->slack-message [{:keys [time-taken-ms bench-type] :as bench-map}]
  (format "*%s* (%s): `%s`"
          (name bench-type)
          (java.time.Duration/ofMillis time-taken-ms)
          (pr-str (dissoc bench-map :bench-ns :bench-type :crux-commit :crux-version :time-taken-ms))))

(defn results->slack-message [results]
  (format "*%s*\n========\n%s\n"
          (:bench-ns (first results))
          (->> results
               (map result->slack-message)
               (string/join "\n"))))

(defn- result->html [{:keys [time-taken-ms bench-type] :as bench-map}]
  (format "<p> <b>%s</b> (%s): <code>%s</code></p>"
          (name bench-type)
          (java.time.Duration/ofMillis time-taken-ms)
          (pr-str (dissoc bench-map :bench-ns :bench-type :crux-commit :crux-version :time-taken-ms))))

(defn- results->html [results]
  (format "<h3>%s</h3> %s"
          (:bench-ns (first results))
          (->> results
               (map result->html)
               (string/join " "))))

(defn results->email [bench-results]
  (str "<h1>Crux bench results</h1>"
       (->> (for [[node-type results] bench-results]
              (str (format "<h2>%s</h2>" node-type)
                   (->> (for [result results]
                          (results->html result))
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

(defn node-size-in-bytes [node]
  {:node-size-bytes (:crux.kv/size (api/status node))})

(def nodes
  {"standalone-rocksdb"
   (fn [data-dir]
     {:crux.node/topology '[crux.standalone/topology
                            crux.metrics.dropwizard.cloudwatch/reporter
                            crux.kv.rocksdb/kv-store]
      :crux.kv/db-dir (str (io/file data-dir "kv/rocksdb"))
      :crux.standalone/event-log-dir (str (io/file data-dir "eventlog/rocksdb"))
      :crux.standalone/event-log-kv-store 'crux.kv.rocksdb/kv})

   "standalone-rocksdb-with-metrics"
   (fn [data-dir]
     {:crux.node/topology '[crux.standalone/topology
                            crux.metrics.dropwizard.cloudwatch/reporter
                            crux.kv.rocksdb/kv-store-with-metrics]
      :crux.kv/db-dir (str (io/file data-dir "kv/rocksdb-with-metrics"))
      :crux.standalone/event-log-dir (str (io/file data-dir "eventlog/rocksdb-with-metrics"))
      :crux.standalone/event-log-kv-store 'crux.kv.rocksdb/kv})

   "kafka-rocksdb"
   (fn [data-dir]
     {:crux.node/topology '[crux.kafka/topology
                            crux.metrics.dropwizard.cloudwatch/reporter
                            crux.kv.rocksdb/kv-store]
      :crux.kafka/bootstrap-servers "localhost:9092"
      :crux.kafka/doc-topic "kafka-rocksdb-doc"
      :crux.kafka/tx-topic "kafka-rocksdb-tx"
      :crux.kv/db-dir (str (io/file data-dir "kv/rocksdb"))})

   "embedded-kafka-rocksdb"
   (fn [data-dir]
     {:crux.node/topology '[crux.kafka/topology
                            crux.metrics.dropwizard.cloudwatch/reporter
                            crux.kv.rocksdb/kv-store]
      :crux.kafka/bootstrap-servers "localhost:9091"
      :crux.kafka/doc-topic "kafka-rocksdb-doc"
      :crux.kafka/tx-topic "kafka-rocksdb-tx"
      :crux.kv/db-dir (str (io/file data-dir "kv/rocksdb"))})
   #_"standalone-lmdb"
   #_(fn [data-dir]
       {:crux.node/topology '[crux.standalone/topology
                              crux.metrics.dropwizard.cloudwatch/reporter
                              crux.kv.lmdb/kv-store]
        :crux.kv/db-dir (str (io/file data-dir "kv/lmdb"))
        :crux.standalone/event-log-kv-store 'crux.kv.lmdb/kv
        :crux.standalone/event-log-dir (str (io/file data-dir "eventlog/lmdb"))})

   #_"kafka-lmdb"
   #_(fn [data-dir]
       {:crux.node/topology '[crux.kafka/topology
                              crux.metrics.dropwizard.cloudwatch/reporter
                              crux.kv.lmdb/kv-store]
        :crux.kafka/bootstrap-servers "localhost:9092"
        :crux.kafka/doc-topic "kafka-lmdb-doc"
        :crux.kafka/tx-topic "kafka-lmdb-tx"
        :crux.kv/db-dir (str (io/file data-dir "kv/rocksdb"))})})

(defn with-nodes* [nodes f]
  (f/with-tmp-dir "dev-storage" [data-dir]
    (with-open [emb (ek/start-embedded-kafka
                      {:crux.kafka.embedded/zookeeper-data-dir (str (io/file data-dir "zookeeper"))
                       :crux.kafka.embedded/kafka-log-dir (str (io/file data-dir "kafka-log"))
                       :crux.kafka.embedded/kafka-port 9091})]
      (vec
       (for [[node-type ->node] nodes]
         (with-open [node (api/start-node (->node data-dir))]
           (with-dimensions {:crux-node-type node-type}
             (log/infof "Running bench on %s node." node-type)
             (post-to-slack (str "running on node: " node-type))
             [node-type (f node)])))))))

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

(defn save-to-s3 [{:keys [database version]} file]
  (try
    (.putObject (S3Client/create)
                (-> (PutObjectRequest/builder)
                    (.bucket "crux-bench")
                    (.key (generate-s3-filename database version))
                    (.build))
                (RequestBody/fromFile file))
    (catch SdkClientException e
      "AWS credentials not found! Results file not saved.")))

(defn load-from-s3 [key]
  (try
    (.getObject (S3Client/create)
                (-> (GetObjectRequest/builder)
                    (.bucket "crux-bench")
                    (.key key)
                    (.build)))
    (catch SdkClientException e
      (log/warn (format "AWS credentials not found! File %s not loaded" key)))))

(defn send-email-via-ses [message]
  (try
    (let [email (-> (SendEmailRequest.)
                    (.withDestination (-> (Destination.)
                                          (.withToAddresses [(string/replace "crux-bench at juxt.pro" " at " "@")])))
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
      (-> (AmazonSimpleEmailServiceClientBuilder/standard)
          (.withRegion "eu-west-1")
          (.build)
          (.sendEmail email)))

    (catch Exception e
      (log/warn "Email failed to send! Error: " e))))
