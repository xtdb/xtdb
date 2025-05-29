(ns xtdb.bench.clickbench
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.serde :as serde]
            [xtdb.util :as util])
  (:import clojure.lang.MapEntry
           [java.time Duration LocalDate LocalDateTime ZoneOffset ZonedDateTime]
           [java.time.format DateTimeFormatter]
           [java.util.zip GZIPInputStream GZIPOutputStream]
           [software.amazon.awssdk.services.s3 S3Client]
           [software.amazon.awssdk.services.s3.model GetObjectRequest]))

(def hits-header
  [:watch-id :java-enable :title :good-event :event-time :event-date :counter-id :client-ip :region-id :user-id
   :counter-class :os :user-agent :url :referer :is-refresh :referer-category-id :referer-region-id :url-category-id
   :url-region-id :resolution-width :resolution-height :resolution-depth :flash-major :flash-minor :flash-minor2
   :net-major :net-minor :user-agent-major :user-agent-minor :cookie-enable :javascript-enable :is-mobile :mobile-phone
   :mobile-phone-model :params :ip-network-id :trafic-source-id :search-engine-id :search-phrase :adv-engine-id :is-artifical
   :window-client-width :window-client-height :client-time-zone :client-event-time :silverlight-version1 :silverlight-version2
   :silverlight-version3 :silverlight-version4 :page-charset :code-version :is-link :is-download :is-not-bounce :f-uniq-id
   :original-url :hid :is-old-counter :is-event :is-parameter :dont-count-hits :with-hash :hit-color :local-event-time :age
   :sex :income :interests :robotness :remote-ip :window-name :opener-name :history-length :browser-language
   :browser-country :social-network :social-action :http-error :send-timing :dns-timing :connect-timing
   :response-start-timing :response-end-timing :fetch-timing :social-source-network-id :social-source-page :param-price
   :param-order-id :param-currency :param-currency-id :openstat-service-name :openstat-campaign-id :openstat-ad-id
   :openstat-source-id :utm-source :utm-medium :utm-campaign :utm-content :utm-term :from-tag :has-gclid :referer-hash
   :url-hash :clid])

(def numeric-columns
  #{:watch-id :java-enable :good-event :counter-id :client-ip :region-id :user-id
    :counter-class :os :user-agent :is-refresh :referer-category-id :referer-region-id
    :url-category-id :url-region-id :resolution-width :resolution-height :resolution-depth
    :flash-major :net-major :net-minor :user-agent-major :cookie-enable :javascript-enable
    :is-mobile :mobile-phone :ip-network-id :trafic-source-id :search-engine-id :adv-engine-id
    :is-artifical :window-client-width :window-client-height :client-time-zone
    :silverlight-version1 :silverlight-version2 :silverlight-version3 :silverlight-version4
    :code-version :is-link :is-download :is-not-bounce :f-uniq-id :hid :is-old-counter
    :is-event :is-parameter :dont-count-hits :with-hash :age :sex :income :interests
    :robotness :remote-ip :window-name :opener-name :history-length :http-error
    :send-timing :dns-timing :connect-timing :response-start-timing :response-end-timing
    :fetch-timing :social-source-network-id :param-price :param-currency-id :has-gclid
    :referer-hash :url-hash :clid})

(def ^java.time.format.DateTimeFormatter ts-formatter
  (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss"))

(defn parse-value [column-name, ^String value]
  (cond
    (= "\\N" value) nil

    (contains? numeric-columns column-name)
    (if (re-find #"\." value)
      (parse-double value)
      (parse-long value))

    :else (or
           ;; Exact ISO date (yyyy-MM-dd)
           (try
             (when (re-matches #"\d{4}-\d{2}-\d{2}" value)
               (LocalDate/parse value))
             (catch Exception _))

           ;; Timestamp-like string with time component
           (try
             (when (re-matches #"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}" value)
               (.atZone (LocalDateTime/parse value ts-formatter)
                        ZoneOffset/UTC))
             (catch Exception _))

           value)))

(defn with-tsv-rows [tsv-gz-file f]
  (with-open [in (io/reader (GZIPInputStream. (io/input-stream tsv-gz-file)))]
    (f (for [row (csv/read-csv in, :separator \tab, :quote \")]
         (-> (->> (map (fn [col cell]
                         (MapEntry/create col (parse-value col cell)))
                       hits-header row)
                  (into {}))
             (as-> row-map (assoc row-map :xt/id (->> (map row-map [:counter-id :event-date :user-id :event-time :watch-id])
                                                      (str/join "_")))))))))

(def ^java.io.File hits-file
  (io/file "datasets/clickbench/hits.transit.msgpack.gz"))

(comment ; to generate the transit file from the TSV file
  (def !transform
    (future
      (with-tsv-rows (io/file "/tmp/downloads/hits.tsv.gz")
        (fn [rows]
          (with-open [out (GZIPOutputStream. (io/output-stream (doto hits-file io/make-parents)))]
            (let [writer (transit/writer out :msgpack
                                         {:handlers serde/transit-write-handler-map})]
              (dorun
               (->> rows
                    (take 10000000)
                    (partition-all 1000)
                    (map-indexed (fn [batch-idx rows]
                                   (when (Thread/interrupted)
                                     (throw (InterruptedException.)))

                                   (log/debug "batch" batch-idx)
                                   (doseq [row rows]
                                     (transit/write writer row))
                                   (.flush out))))))))))))

(defn download-dataset []
  (when-not (.exists hits-file)
    (log/info "downloading" (str hits-file))
    (io/make-parents (io/file "datasets"))

    (.getObject (S3Client/create)
                (-> (GetObjectRequest/builder)
                    (.bucket "xtdb-datasets")
                    (.key "clickbench/hits.transit.msgpack.gz")
                    ^GetObjectRequest (.build))
                (.toPath hits-file))))

(defn submit-batch! [conn docs]
  (xt/submit-tx conn
                [(into [:put-docs :hits]
                       (for [{:keys [^ZonedDateTime event-time] :as doc} docs]
                         (assoc doc
                                :xt/id (->> (map doc [:counter-id :event-date :user-id :event-time :watch-id])
                                            (str/join "_"))
                                :xt/valid-from event-time
                                :xt/valid-to (-> event-time
                                                 (.plusNanos 1000)))))]))

(defn store-documents! [node docs]
  (with-open [conn (jdbc/get-connection node)]
    (dorun
     (->> (partition-all 1000 docs)
          (map-indexed (fn [batch-idx docs]
                         (log/debug "batch" batch-idx)

                         (when (Thread/interrupted)
                           (throw (InterruptedException.)))

                         (submit-batch! conn docs)))))))

(defmethod b/cli-flags :clickbench [_]
  [["-l" "--limit LIMIT"
    :parse-fn parse-long]

   ["-h" "--help"]])

(def queries
  (->> (io/file "/home/james/src/xtdb/xtdb2/modules/bench/src/main/resources/clickbench/queries.sql")
       slurp str/split-lines))

(defmethod b/->benchmark :clickbench [_ {:keys [no-load? limit]}]
  {:title "Clickbench Hits"
   :tasks [{:t :call, :stage :download
            :f (fn [_]
                 (download-dataset))}

           {:t :do, :stage :ingest
            :tasks (concat (when-not no-load?
                             [{:t :do
                               :stage :submit-docs
                               :tasks [{:t :call
                                        :f (fn [{:keys [node]}]
                                             (with-open [is (GZIPInputStream. (io/input-stream hits-file))]
                                               (store-documents! node (cond->> (serde/transit-seq (transit/reader is :msgpack
                                                                                                                  {:handlers serde/transit-read-handler-map}))
                                                                        limit (take limit)))))}]}])

                           [{:t :call, :stage :sync
                             :f (fn [{:keys [node]}]
                                  (b/sync-node node (Duration/ofHours 5)))}

                            {:t :call, :stage :finish-block
                             :f (fn [{:keys [node]}]
                                  (b/finish-block! node))}

                            {:t :call, :stage :compact
                             :f (fn [{:keys [node]}]
                                  (b/compact! node))}

                            {:t :call, :stage :queries
                             :f (fn [{:keys [node]}]
                                  (doseq [query (take 1 queries)]
                                    (log/info "Running query:" query)
                                    (xt/q node query)))}])}]})

(t/deftest ^:benchmark run-clickbench
  (-> (b/->benchmark :clickbench {})
      (b/run-benchmark {:node-dir (util/->path "/home/james/tmp/clickbench-10M")})))
