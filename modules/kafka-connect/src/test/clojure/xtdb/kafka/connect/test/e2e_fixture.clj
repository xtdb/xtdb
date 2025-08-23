(ns xtdb.kafka.connect.test.e2e-fixture
  (:require [hato.client :as http]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [integrant.core :as ig]
            [jsonista.core :as json]
            [next.jdbc :as jdbc]
            [xtdb.pgwire :as xt-pg])
  (:import (java.util Map)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
           (org.testcontainers Testcontainers)
           (org.testcontainers.containers BindMode Container GenericContainer Network)
           (org.testcontainers.containers.wait.strategy Wait)
           (org.testcontainers.kafka ConfluentKafkaContainer)
           (org.testcontainers.utility DockerImageName)))

(defmethod ig/init-key ::xtdb [_ _]
  (let [server (xt-pg/open-playground {:port 5439})]
    (Testcontainers/exposeHostPorts (int-array [5439]))
    server))

(defmethod ig/halt-key! ::xtdb [_ server]
  (.close server))

(defmethod ig/init-key ::kafka [_ _]
  (doto (ConfluentKafkaContainer. (DockerImageName/parse "confluentinc/cp-kafka:7.6.0"))
    (.withExposedPorts (into-array [(int 9092)]))
    (.withNetwork Network/SHARED)
    (.start)))

(defmethod ig/halt-key! ::kafka [_ container]
  (.close container))

(defn kafka-endpoint-for-containers [kafka]
  (str (-> kafka .getNetworkAliases first) ":9093"))

(defn with-env [^Container c m]
  (-> (fn [c k v]
        (.withEnv c (name k) (str v)))
      (reduce-kv c m)))

(defmethod ig/init-key ::connect [_ {:keys [kafka]}]
  (let [container (doto (GenericContainer. (DockerImageName/parse "confluentinc/cp-kafka-connect:7.6.0"))
                    (.dependsOn [kafka])
                    (.withNetwork (.getNetwork kafka))
                    (.withExposedPorts (into-array [(int 8083)]))
                    (.waitingFor (Wait/forHttp "/connectors"))
                    (.withFileSystemBind
                      (let [jar-dir (io/file "modules/kafka-connect/build/libs")
                            jar-file (io/file jar-dir "xtdb-kafka-connect.jar")]
                        (if (.exists jar-file)
                          (.getAbsolutePath jar-dir)
                          (throw (IllegalStateException. (str "Not found: " jar-file)))))
                      "/usr/share/xtdb/kafka/connect/lib"
                      BindMode/READ_ONLY)
                    (with-env {:CONNECT_LOG4J_LOGGERS "xtdb.kafka=DEBUG"

                               :CONNECT_BOOTSTRAP_SERVERS (kafka-endpoint-for-containers kafka)
                               :CONNECT_REST_PORT 8083
                               :CONNECT_GROUP_ID "default"

                               :CONNECT_CONFIG_STORAGE_TOPIC "default.config"
                               :CONNECT_OFFSET_STORAGE_TOPIC "default.offsets"
                               :CONNECT_STATUS_STORAGE_TOPIC "default.status"

                               ; we only have 1 kafka broker, so topic replication factor must be one
                               :CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR 1
                               :CONNECT_STATUS_STORAGE_REPLICATION_FACTOR 1
                               :CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR 1

                               :CONNECT_PLUGIN_PATH "/usr/share/xtdb/kafka/connect/lib/xtdb-kafka-connect.jar"

                               :CONNECT_KEY_CONVERTER "org.apache.kafka.connect.json.JsonConverter"
                               :CONNECT_VALUE_CONVERTER "org.apache.kafka.connect.json.JsonConverter"
                               :CONNECT_REST_ADVERTISED_HOST_NAME "localhost"})
                    (.start))]
    container))

(defmethod ig/halt-key! ::connect [_ container]
  (.close container))

(defmethod ig/init-key ::schema-registry [_ {:keys [kafka]}]
  (doto (GenericContainer. "confluentinc/cp-schema-registry:7.6.0")
    (.dependsOn [kafka])
    (.withNetwork (.getNetwork kafka))
    (.withExposedPorts (into-array [(int 8081)]))
    (with-env {:SCHEMA_REGISTRY_HOST_NAME "schema-registry"
               :SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS (kafka-endpoint-for-containers kafka)})
    (.start)))

(defmethod ig/halt-key! ::schema-registry [_ container]
  (.close container))

(def conf {::xtdb {}
           ::kafka {}
           ::connect {:xtdb (ig/ref ::xtdb)
                      :kafka (ig/ref ::kafka)}
           ::schema-registry {:kafka (ig/ref ::kafka)}})

(defonce ^:dynamic *containers* nil)

(defn with-containers [f]
  (if *containers*
    (f)
    (binding [*containers* (ig/init conf)]
      (try
        (f)
        (finally
          (ig/halt! *containers*))))))

(defn run-permanently []
  "Manually starts the fixture until manually stopped, for faster testing at dev-time."
  (alter-var-root #'*containers* (fn [prev]
                                   (if prev
                                     prev
                                     (ig/init conf)))))

(defn stop-permanently []
  "Manually stops the fixture."
  (alter-var-root #'*containers* (fn [prev]
                                   (ig/halt! prev)
                                   nil)))

(def ^:dynamic *xtdb-db*)
(def ^:dynamic *xtdb-conn*)

(defn with-xtdb-conn [f]
  (binding [*xtdb-db* (random-uuid)]
    (with-open [xtdb-conn (jdbc/get-connection (str "jdbc:xtdb://localhost:5439/" *xtdb-db*))]
      (binding [*xtdb-conn* xtdb-conn]
        (f)))))


; Test utilities

(defn kafka-connect-api-url []
  (let [connect (::connect *containers*)]
    (str "http://" (.getHost connect) ":" (.getMappedPort connect 8083))))

(defn kafka-schema-registry-base-url []
  (let [schema-registry (::schema-registry *containers*)
        host (.getHost schema-registry)
        port (.getMappedPort schema-registry 8081)]
    (str "http://" host ":" port)))

(defn create-connector! [{:keys [topic value-converter extra-conf]}]
  (let [connector-name (str topic "-xtdb-sink")
        connector-config (merge
                           {:tasks.max "1"

                            :topics topic
                            :table.name.format "${topic}"

                            :key.converter "org.apache.kafka.connect.storage.StringConverter"
                            :key.converter.schemas.enable "false"
                            :value.converter (case value-converter
                                                 :json "org.apache.kafka.connect.json.JsonConverter"
                                                 :json-schema "io.confluent.connect.json.JsonSchemaConverter"
                                                 :avro "io.confluent.connect.avro.AvroConverter")
                            :value.converter.schema.registry.url (str "http://" (-> *containers* ::schema-registry .getNetworkAliases first) ":8081")
                            :value.converter.schemas.enable "true"

                            :connector.class "xtdb.kafka.connect.XtdbSinkConnector"
                            :jdbcUrl (str "jdbc:xtdb://host.testcontainers.internal:5439/" *xtdb-db*)}

                           (when (= value-converter :avro)
                             {"value.converter.connect.meta.data" "true"})

                           extra-conf)]

    (http/post (str (kafka-connect-api-url) "/connectors")
      {:body (json/write-value-as-string {:name connector-name
                                          :config connector-config})
       :content-type :json
       :accept :json})))

(defn delete-connector! [topic-name]
  (http/delete (str (kafka-connect-api-url) "/connectors/" (str topic-name "-xtdb-sink"))))

(defn kafka-endpoint-on-host []
  (str "localhost:" (.getMappedPort (::kafka *containers*) 9092)))

(defn send-record! [topic k m & [{:keys [use-schema-registry?
                                         value-schema-type]
                                  :or {value-schema-type :json-schema}}]]
  (with-open [producer (KafkaProducer. ^Map (merge {"bootstrap.servers" (kafka-endpoint-on-host)
                                                    "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                                                    "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"}
                                                   (when use-schema-registry?
                                                     {"value.serializer" (case value-schema-type
                                                                           :json "org.apache.kafka.common.serialization.StringSerializer"
                                                                           :json-schema "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer"
                                                                           :avro "io.confluent.kafka.serializers KafkaAvroSerializer")
                                                      "schema.registry.url" (kafka-schema-registry-base-url)
                                                      "use.schema.id" (int 1)
                                                      "id.compatibility.strict" false
                                                      "auto.register.schemas" false
                                                      "use.latest.version" true})))]
    (-> (.send producer (ProducerRecord. topic k m))
        (.get))))
