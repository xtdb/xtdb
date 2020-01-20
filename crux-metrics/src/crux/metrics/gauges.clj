(ns crux.metrics.gauges)

;; Ingest metrcis
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

(defn latest-ingest-latency-tx
  "Time taken for latest ingest of a tx"
  [!metrics]

  (:crux.metrics/latest-latency-tx @!metrics))

(defn latest-ingest-latency-docs
  "Time taken for latest ingest of a doc"
  [!metrics]

  (:crux.metrics/latest-latency-docs @!metrics))

(defn tx-id-lag
  "Diffence between the latest submitted tx-id and the current status of the
  node. If no tx has been submitted yet, return 0."
  [!metrics]

  (let [[latest status] (:crux.metrics/latest-tx-id @!metrics)]
    (if status
      (- latest status)
      0)))
