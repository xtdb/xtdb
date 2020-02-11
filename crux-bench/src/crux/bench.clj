(ns crux.bench
  (:require [crux.io :as cio]
            [crux.kafka.embedded :as ek]
            [crux.api :as api]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as string]
            [clj-http.client :as client]
            [crux.fixtures :as f]
            [amazonica.aws.s3 :as s3])
  (:import (java.util.concurrent Executors ExecutorService)))

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

(defn results->slack-message [results bench-ns]
  (format "*%s*\n========\n%s\n"
          bench-ns
          (->> results
               (map result->slack-message)
               (string/join "\n"))))

(defn with-bench-ns* [bench-ns f]
  (log/infof "running bench-ns '%s'..." bench-ns)

  (binding [*bench-ns* bench-ns
            *!bench-results* (atom [])]
    (f)

    (log/infof "finished bench-ns '%s'." bench-ns)

    (let [results @*!bench-results*]
      (run! (comp println json/write-str) results)
      results)))

(defmacro with-bench-ns [bench-ns & body]
  `(with-bench-ns* ~bench-ns (fn [] ~@body)))

(defn with-node* [f]
  (f/with-tmp-dir "dev-storage" [data-dir]
    (with-open [embedded-kafka (ek/start-embedded-kafka
                                 {:crux.kafka.embedded/zookeeper-data-dir (str (io/file data-dir "zookeeper"))
                                  :crux.kafka.embedded/kafka-log-dir (str (io/file data-dir "kafka-log"))
                                  :crux.kafka.embedded/kafka-port 9092})
                node (api/start-node {:crux.node/topology '[crux.kafka/topology]
                                      :crux.node/kv-store 'crux.kv.rocksdb/kv
                                      :crux.kafka/bootstrap-servers "localhost:9092"
                                      :crux.kv/db-dir (str (io/file data-dir "db-dir-1"))
                                      :crux.standalone/event-log-dir (str (io/file data-dir "eventlog-1"))})]
      (f node))))

(defmacro with-node [[node-binding] & body]
  `(with-node* (fn [~node-binding] ~@body)))

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
  (s3/put-object
   :bucket-name "crux-bench"
   :key (generate-s3-filename database version)
   :file file))

(defn load-from-s3 [key]
  (-> (s3/get-object
       :bucket-name "crux-bench"
       :key key)
      :input-stream))
