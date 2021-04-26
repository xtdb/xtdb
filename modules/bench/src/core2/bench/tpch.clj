(ns core2.bench.tpch
  (:require [clojure.tools.logging :as log]
            [core2.bench :as bench]
            [core2.core :as c2]
            [core2.tpch :as tpch])
  (:import core2.core.Node
           java.time.Duration
           java.util.function.Consumer))

(defn ingest-tpch [^Node node {:keys [scale-factor]}]
  (let [tx (bench/with-timing :submit-docs
             (tpch/submit-docs! node scale-factor))]
    (bench/with-timing :await-tx
      (c2/await-tx node tx (Duration/ofHours 5)))

    (bench/with-timing :finish-chunk
      (bench/finish-chunk node))))

(defn query-tpch [node watermark]
  (doseq [q tpch/queries]
    (bench/with-timing (str "query " (:name (meta q)))
      (try
        (with-open [cursor (c2/open-q node watermark @q)]
          (.forEachRemaining cursor
                             (reify Consumer
                               (accept [_ root]))))
        (catch Exception e
          (.printStackTrace e))))))

(comment
  (with-open [node (bench/start-node)]
    (bench/with-timing :ingest
      (ingest-tpch node {:scale-factor 0.01}))

    (with-open [watermark (c2/open-watermark node)]
      (bench/with-timing :cold-queries
        (query-tpch node watermark))

      (bench/with-timing :hot-queries
        (query-tpch node watermark)))))

(defn -main [& args]
  (try
    (let [opts (or (bench/parse-args [[nil "--scale-factor 0.01" "Scale factor for regular TPCH test"
                                       :id :scale-factor
                                       :default 0.01
                                       :parse-fn #(Double/parseDouble %)]]
                                     args)
                   (System/exit 1))]
      (log/info "Opts: " (pr-str opts))
      (with-open [node (bench/start-node)]
        (bench/with-timing :ingest
          (ingest-tpch node opts))

        (with-open [watermark (c2/open-watermark node)]
          (bench/with-timing :cold-queries
            (query-tpch node watermark))

          (bench/with-timing :hot-queries
            (query-tpch node watermark)))))

    (catch Exception e
      (.printStackTrace e)
      (System/exit 1))

    (finally
      (shutdown-agents))))
