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
            [crux.fixtures :as f]))

(def commit-hash
  (System/getenv "COMMIT_HASH"))


(def crux-version
  (when-let [pom-file (io/resource "META-INF/maven/juxt/crux-core/pom.properties")]
    (with-open [in (io/reader pom-file)]
      (get (cio/load-properties in) "version"))))

(def ^:dynamic ^:private *bench-ns*)
(def ^:dynamic ^:private *!bench-results*)

(defn run-bench* [bench-type f]
  (log/infof "running bench '%s/%s'..." *bench-ns* bench-type)

  (let [start-time-ms (System/currentTimeMillis)
        ret (try
              (f)
              (catch Exception e
                (log/warnf e "error running bench '%s/%s'" *bench-ns* bench-type)
                {:error (.getMessage e)}))

        res (merge (when (map? ret) ret)
                   {:bench-ns *bench-ns*
                    :bench-type bench-type
                    :time-taken-ms (- (System/currentTimeMillis) start-time-ms)
                    :crux-commit commit-hash
                    :crux-version crux-version})]

    (log/infof "finished bench '%s/%s'." *bench-ns* bench-type)

    (swap! *!bench-results* conj res)
    res))

(defmacro ^{:style/indent 1} run-bench [bench-type & body]
  `(run-bench* ~bench-type (fn [] ~@body)))

(defn post-to-slack [message]
  (when-let [slack-url (System/getenv "SLACK_URL")]
    (client/post (-> slack-url
                     (json/read-str)
                     (get "slack-url"))
                 {:body (json/write-str {:text message})
                  :content-type :json})))

(defn ->slack-message [{bench-time :time-taken-ms :as bench-map}]
  (->> (for [[k v] (-> bench-map
                       (assoc :time-taken (java.time.Duration/ofMillis bench-time))
                       (dissoc :bench-ns :crux-commit :crux-version :time-taken-ms))]
         (format "*%s*: %s" (name k) v))
       (string/join "\n")))

(defn with-bench-ns* [bench-ns f]
  (log/infof "running bench-ns '%s'..." bench-ns)

  (binding [*bench-ns* bench-ns
            *!bench-results* (atom [])]
    (f)

    (log/infof "finished bench-ns '%s'." bench-ns)

    (let [results @*!bench-results*]
      (run! (comp println json/write-str) results)
      (post-to-slack (format "*%s*\n========\n%s\n"
                             *bench-ns*
                             (->> results
                                  (map ->slack-message)
                                  (string/join "\n\n")))))))

(defmacro with-bench-ns [bench-ns & body]
  `(with-bench-ns* ~bench-ns (fn [] ~@body)))

(def ^:dynamic *node*)

(defn with-node* [f]
  (f/with-tmp-dir "dev-storage" [data-dir]
    (with-open [embedded-kafka (ek/start-embedded-kafka
                                {:crux.kafka.embedded/zookeeper-data-dir (str (io/file data-dir "zookeeper"))
                                 :crux.kafka.embedded/kafka-log-dir (str (io/file data-dir "kafka-log"))
                                 :crux.kafka.embedded/kafka-port 9092})
                node (api/start-node {:crux.node/topology 'crux.kafka/topology
                                      :crux.node/kv-store 'crux.kv.rocksdb/kv
                                      :crux.kafka/bootstrap-servers "localhost:9092"
                                      :crux.kv/db-dir (str (io/file data-dir "db-dir-1"))
                                      :crux.standalone/event-log-dir (str (io/file data-dir "eventlog-1"))})]
      (f node))))

(defmacro with-node [[node-binding] & body]
  `(with-node* (fn [~node-binding] ~@body)))
