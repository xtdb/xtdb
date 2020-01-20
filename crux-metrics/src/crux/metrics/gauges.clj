(ns crux.metrics.gauges)

(defn ingesting-docs
  "Number of documents currently queued for ingest"
  [!metrics]

  (count (:crux.metrics/indexing-docs @!metrics)))

(defn ingesting-tx
  "Number of txs currently queued for ingest"
  [!metrics]

  (count (:crux.metrics/indexing-tx @!metrics)))

(defn ingested-docs
  "Number of total ingested doc"
  [!metrics]

  (count (:crux.metrics/indexed-docs @!metrics)))

(defn ingested-tx
  "Number of total ingested tx"
  [!metrics]

  (count (:crux.metrics/indexed-tx @!metrics)))

;; TODO spilt into doc and tx
(defn latest-ingest-latency
  "Time taken for latest ingest"
  [!metrics]

  (:crux.metrics/latest-latency @!metrics))

(defn tx-id-lag
  "Diffence between the latest submitted tx-id and the current status of the
  node. If no tx has been submitted yet, return 0."
  [!metrics]

  (let [[latest status] (:crux.metrics/latest-tx-id @!metrics)]
    (if status
      (- latest status)
      0)))
