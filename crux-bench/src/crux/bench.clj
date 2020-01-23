(ns crux.bench
  (:require [crux.io :as cio]
            [crux.kafka.embedded :as ek]
            [crux.api :as api]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as string]
            [clj-http.client :as client]))

(def commit-hash
  (string/trim (:out (shell/sh "git" "rev-parse" "HEAD"))))

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
  (when (System/getenv "SLACK_URL")
    (client/post (-> (System/getenv "SLACK_URL")
                      (clojure.data.json/read-str)
                      (get "slack-url"))
                 {:body (json/write-str {:text message})
                  :content-type :json})))

(defn format-and-post-results-to-slack [result]
  (prn result)
  (let [result-strings (map
                        (fn [bench-map] (->> (dissoc bench-map :bench-ns :crux-commit :crux-version)
                                             (keys)
                                             (map (fn [key] (format "*%s*: %s" (name key) (bench-map key))))
                                             (string/join "\n")))
                        result)
        formatted-string (->> result-strings
                              (string/join "\n\n")
                              (format "*%s*\n========\n%s\n" *bench-ns*))]
    (post-to-slack formatted-string)))

(defn with-bench-ns* [bench-ns f]
  (log/infof "running bench-ns '%s'..." bench-ns)

  (binding [*bench-ns* bench-ns
            *!bench-results* (atom [])]
    (f)

    (log/infof "finished bench-ns '%s'." bench-ns)

    (doseq [result @*!bench-results*]
      (println (json/write-str result)))
    (format-and-post-results-to-slack @*!bench-results*)))

(defmacro with-bench-ns [bench-ns & body]
  `(with-bench-ns* ~bench-ns (fn [] ~@body)))

(def ^:dynamic *node*)

(defn with-node* [f]
  (try (with-open [embedded-kafka (ek/start-embedded-kafka
                                   {:crux.kafka.embedded/zookeeper-data-dir "dev-storage/zookeeper"
                                    :crux.kafka.embedded/kafka-log-dir "dev-storage/kafka-log"
                                    :crux.kafka.embedded/kafka-port 9092})
                   node (api/start-node {:crux.node/topology 'crux.kafka/topology
                                         :crux.node/kv-store 'crux.kv.rocksdb/kv
                                         :crux.kafka/bootstrap-servers "localhost:9092"
                                         :crux.kv/db-dir "dev-storage/db-dir-1"
                                         :crux.standalone/event-log-dir "dev-storage/eventlog-1"})]
         (f node))
       (catch Exception e e)
       (finally (cio/delete-dir "dev-storage"))))

(defmacro with-node [[node-binding] & body]
  `(with-node* (fn [~node-binding] ~@body)))
