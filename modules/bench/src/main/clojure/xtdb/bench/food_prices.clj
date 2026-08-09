(ns xtdb.bench.food-prices
  "Bitemporal delta ingestion over ADBC, ported from jr200-labs/polars-hist-db.

  The library ingests a time-partitioned price series one partition at a time, and each partition
  asks XTDB two keyed questions before it writes anything: which of these rows have actually
  changed, and which of their foreign keys does the dimension table not hold yet. Both are answered
  by binding the key set as a list parameter and `UNNEST`ing it, so a partition is:

  - bind the batch's ids, join `food_prices` against them `FOR VALID_TIME AS OF` the batch's basis,
    and keep only the rows whose price actually moved;
  - resolve the place / product / unit foreign keys the same way, inserting any the dimension
    tables don't hold yet;
  - write the survivors back as one `BEGIN READ WRITE WITH (SYSTEM_TIME = …)` transaction,
    optionally closing out the rows the batch no longer mentions.

  So what this measures is the keyed temporal join against a growing fact table, and the cost of a
  write that only carries the delta. Rows arrive as Arrow, one record batch per month, and go back
  through `bulkIngest` — the same ADBC surface the library reaches over FlightSQL.

  The library itself does not do it this way: lacking `UNNEST` on MariaDB, it materialises each key
  set as a real table, joins against that, and erases it — four durable, replicated, compacted
  writes of throwaway data per partition. That is portability tax rather than anything XTDB
  requires, so it isn't reproduced here. The dataset is mirrored to S3 by `mirrors/food-prices`."

  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.bench.util :as bu]
            [xtdb.error :as err]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang IReduceInit)
           (io.micrometer.core.instrument Timer)
           (java.io File)
           (java.time Duration Instant LocalDate ZoneOffset)
           (java.util.concurrent Callable)
           (org.apache.arrow.adbc.core BulkIngestMode)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.ipc ArrowStreamReader)
           (software.amazon.awssdk.services.s3 S3Client)
           (software.amazon.awssdk.services.s3.model GetObjectRequest)
           (xtdb.api Xtdb$Connection Xtdb$Statement)
           (xtdb.api.query IKeyFn$KeyFn)
           (xtdb.arrow Relation RelationReader)))

(def ^:private prices-table "food_prices")

(def ^:private price-columns ["place_id" "product_id" "um_id" "price" "price_usd"])

(def dimensions
  "The price batch's foreign keys, and the tables they point at."
  [{:file "places.arrow", :table "place_info", :fk "place_id"}
   {:file "products.arrow", :table "product_info", :fk "product_id"}
   {:file "units.arrow", :table "unit_info", :fk "um_id"}])

;; matches the library's `max_rows_per_insert`
(def ^:private chunk-size 10000)

;; -- dataset --

(def ^:dynamic *data-dir*
  "Where the mirrored dataset lands. Dynamic so a test can point at a fixture of its own."
  (io/file "modules/bench/dataset-downloads/food-prices"))

(defn dataset-file ^File [size fname]
  (io/file *data-dir* (name size) fname))

(defn download-dataset [size]
  (doseq [fname (cons "prices.arrow" (map :file dimensions))
          :let [file (dataset-file size fname)]
          :when (not (.exists file))]
    (log/info "downloading" (str file))
    (io/make-parents file)
    (.getObject ^S3Client @bu/!s3-client
                (-> (GetObjectRequest/builder)
                    (.bucket "xtdb-datasets")
                    (.key (format "food-prices/%s/%s" (name size) fname))
                    ^GetObjectRequest (.build))
                (.toPath file))))

(defn- arrow-partitions
  "The Arrow IPC stream at `file` as a reducible of partitions — each record batch a vector of row
  maps keyed by column name. Reducible rather than a lazy seq because a batch's `Relation` is only
  valid inside the reduce, and at `big` the whole stream doesn't fit in memory."
  ^IReduceInit [^BufferAllocator al ^File file]
  (reify IReduceInit
    (reduce [_ f init]
      (with-open [in (io/input-stream file)
                  rdr (ArrowStreamReader. in al)]
        (let [root (.getVectorSchemaRoot rdr)]
          (loop [acc init]
            (if (and (not (reduced? acc)) (.loadNextBatch rdr))
              (recur (f acc (with-open [rel (Relation/fromRoot al root)]
                              (vec (.toMaps rel IKeyFn$KeyFn/SNAKE_CASE_STRING)))))
              (unreduced acc))))))))

(defn- read-table
  "The whole of a single-batch file — the dimension tables."
  [al file]
  (into [] cat (arrow-partitions al file)))

;; -- timers --

(def ^:private op-names [:delta-query :dim-resolve :dim-insert :ingest :dropout])

(defn- ->timers
  "Micrometer timers for the per-partition operations, so the summary reports the read/write split
  rather than one ingest number. Empty outside a benchmark run, where there's no registry."
  []
  (when-let [registry b/*registry*]
    (->> op-names
         (into {} (map (fn [k]
                         [k (-> (Timer/builder (name k))
                                (.publishPercentiles (double-array b/percentiles))
                                (.maximumExpectedValue (Duration/ofHours 1))
                                (.minimumExpectedValue (Duration/ofNanos 1))
                                (.register registry))]))))))

(defmacro ^:private timed [timers k & body]
  `(if-let [^Timer timer# (get ~timers ~k)]
     (.recordCallable timer# ^Callable (fn [] ~@body))
     (do ~@body)))

;; -- the ADBC operations --

(defn- execute!
  "Runs a statement, binding `args` as one positional row if given. DML reads its args straight off
  the bound relation, so unlike a query this needs no `prepare`."
  ([conn sql] (execute! conn sql nil))
  ([^Xtdb$Connection conn ^String sql args]
   (with-open [stmt (.createStatement conn sql)]
     (when (seq args)
       (with-open [rel (vw/open-args (.getAllocator conn) args)]
         (.bind stmt ^RelationReader rel)))
     (.executeUpdate stmt))))

(defn- ts-literal ^String [^Instant instant]
  (format "TIMESTAMP WITH TIME ZONE '%s'" instant))

(defn- tick!
  "The next system time to write at: the batch's own basis, or a microsecond past the last write if
  we're already there. XTDB requires system times to ascend and these are historic, so every write
  in the run — the dimension inserts and the DDL included — comes off this one clock rather than the
  wall. The library reaches the same place from the other end, reading `xt.txs` for the last system
  time and bumping past it."
  ^Instant [!clock ^Instant basis]
  (swap! !clock (fn [^Instant prev]
                  (if (and prev (not (.isBefore prev basis)))
                    (.plusNanos prev 1000)
                    basis))))

(defn- with-tx!
  "Runs `f` as one write transaction at the next system time. Writes inside an open transaction
  buffer until the commit, so an ingest and its DML land together.

  Commits through `commitSync` rather than a SQL `COMMIT`: the statement path discards the
  `ExecutedTx`, so an aborted transaction would leave the benchmark reporting timings for writes
  that never landed."
  [^Xtdb$Connection conn !clock basis f]
  (execute! conn (format "BEGIN READ WRITE WITH (SYSTEM_TIME = %s)" (ts-literal (tick! !clock basis))))
  (try
    (let [res (f)
          executed (.commitSync conn)]
      (when (and executed (not (.getCommitted executed)))
        (throw (or (.getError executed)
                   (err/fault :xtdb.bench/tx-aborted "Transaction aborted"))))
      res)
    (catch Throwable t
      (.rollbackTx conn)
      (throw t))))

(defn- create-tables!
  "Declares the tables up front. XTDB infers schema from writes, so the first partition would
  otherwise ask after a `food_prices` that doesn't exist yet and get a planning error — the same
  hole the library plugs, from the other side, by registering its table configs before it ingests."
  [conn]
  (execute! conn (format "CREATE TABLE %s (%s)" prices-table (str/join ", " (cons "_id" price-columns))))
  (doseq [{:keys [table]} dimensions]
    (execute! conn (format "CREATE TABLE %s (_id, id, name)" table))))

(defn- bulk-ingest!
  "The library's `adbc_ingest` — Arrow straight into the table, chunked as it chunks."
  [^Xtdb$Connection conn ^String table rows]
  (doseq [chunk (partition-all chunk-size rows)]
    (with-open [rel (Relation/openFromRows (.getAllocator conn) (vec chunk))
                stmt (.bulkIngest conn table BulkIngestMode/CREATE_APPEND)]
      (.bind ^Xtdb$Statement stmt ^RelationReader rel)
      (.executeUpdate stmt))))

(defn ->pk
  "The library's `xtdb-pk-v1:` encoding for a composite primary key — a deterministic text id built
  from the key columns, so a row keeps the same `_id` across restatements."
  ^String [row]
  (str "xtdb-pk-v1:[[\"place_id\"," (get row "place_id")
       "],[\"product_id\"," (get row "product_id")
       "],[\"um_id\"," (get row "um_id") "]]"))

(defn- held-by-id
  "The rows XTDB holds at `basis` for the given ids, keyed by id. Chunked, because a partition at
  `big` carries 260k ids and the library chunks its own keyed reads the same way."
  [conn ^String sql ids ^Instant basis]
  (into {}
        (comp (mapcat (fn [chunk]
                        (xt/q conn (cond-> [sql] basis (conj basis basis) :always (conj (vec chunk)))
                              {:key-fn :snake-case-string})))
              (map (juxt #(get % "_id") identity)))
        (partition-all chunk-size ids)))

(defn- current-prices
  "The rows the batch is about to restate, as XTDB holds them at `basis`."
  [conn ids ^Instant basis]
  (held-by-id conn
              (format "SELECT t._id, t.price, t.price_usd
                       FROM %s FOR VALID_TIME AS OF ? FOR SYSTEM_TIME AS OF ? AS t
                       WHERE t._id IN (SELECT k.x FROM UNNEST(?) AS k(x))"
                      prices-table)
              ids basis))

(defn- changed
  "Drops the rows XTDB already holds unchanged — the library's `drop_unchanged_rows`, which is the
  whole reason for the lookup above: without it every partition would rewrite every row."
  [rows current]
  (into [] (remove (fn [row]
                     (when-let [held (get current (get row "_id"))]
                       (and (= (get row "price") (get held "price"))
                            (= (get row "price_usd") (get held "price_usd"))))))
        rows))

(defn- resolve-dimension!
  "Inserts the dimension rows `batch` references that `table` doesn't hold yet, found the same way
  as the price delta: bind the referenced keys, probe, take the complement.

  Scoped to the keys the batch actually carries, as the library's foreign-key deduction is — the
  dimension files hold every key in the dataset, and resolving all of them every partition would
  bill the read against rows that partition never mentions."
  [conn timers !clock basis {:keys [table fk rows]} batch]
  (let [referenced (into #{} (map #(get % fk)) batch)
        rows (filterv #(referenced (get % "id")) rows)]
    (when (seq rows)
      (let [held (timed timers :dim-resolve
                        (held-by-id conn
                                    (format "SELECT t._id FROM %s AS t
                                             WHERE t._id IN (SELECT k.x FROM UNNEST(?) AS k(x))"
                                            table)
                                    (map #(get % "id") rows) nil))
            missing (remove #(held (get % "id")) rows)]
        (when (seq missing)
          (timed timers :dim-insert
                 (with-tx! conn !clock basis
                   #(bulk-ingest! conn table (map (fn [row] (assoc row "_id" (get row "id"))) missing)))))))))

(defn- close-out-missing!
  "The library's `dropout` finality: rows the batch no longer mentions stop being true as of the
  batch's basis. Off by default — the food-prices pipeline doesn't enable it."
  [conn ids ^Instant basis]
  ;; the id list can't be chunked here as it can for the reads — a NOT IN over one chunk would close
  ;; out rows the next chunk mentions
  (execute! conn
            (format "DELETE FROM %s FOR PORTION OF VALID_TIME FROM ? TO NULL AS t
                     WHERE t._id NOT IN (SELECT k.x FROM UNNEST(?) AS k(x))"
                    prices-table)
            [basis (vec ids)]))

(defn- ->basis
  "A batch is published a month after the period it describes, and that publication is both its
  system time and — since nothing in the batch carries one — the valid-from of its rows."
  ^Instant [rows]
  (-> ^LocalDate (get (first rows) "month")
      (.plusMonths 1)
      (.atStartOfDay ZoneOffset/UTC)
      .toInstant))

(defn- ingest-partition!
  "One month of restatements, end to end. Returns the number of rows actually written."
  [conn timers !clock dims rows {:keys [dropout?]}]
  (let [rows (mapv #(assoc % "_id" (->pk %)) rows)
        ids (mapv #(get % "_id") rows)
        basis (->basis rows)
        current (timed timers :delta-query (current-prices conn ids basis))
        writes (changed rows current)]

    (doseq [dim dims]
      (resolve-dimension! conn timers !clock basis dim rows))

    (with-tx! conn !clock basis
      (fn []
        (when dropout?
          (timed timers :dropout (close-out-missing! conn ids basis)))
        (timed timers :ingest
               (bulk-ingest! conn prices-table
                             (map #(select-keys % (cons "_id" price-columns)) writes)))))

    (count writes)))

(defn ingest!
  "Ingests `partitions` — a reducible of row-map batches in ascending month order — resolving the
  foreign keys of each against `dims`. Returns `{:partitions n, :written m}`."
  [conn dims opts partitions]
  (let [timers (->timers)
        !clock (atom nil)]
    (reduce (fn [{:keys [partitions written]} rows]
              (when (Thread/interrupted) (throw (InterruptedException.)))
              (let [basis (->basis rows)]
                ;; the DDL is the run's first write, so it comes off the clock too — an
                ;; auto-committed CREATE TABLE would stamp wall-clock now and fence out the
                ;; back-dated writes that follow it
                (when (zero? partitions)
                  (with-tx! conn !clock basis #(create-tables! conn)))

                (let [written (+ written (ingest-partition! conn timers !clock dims rows opts))
                      partitions (inc partitions)]
                  (when (zero? (mod partitions 10))
                    (log/debugf "partition %d, %d rows written" partitions written))
                  {:partitions partitions, :written written})))
            {:partitions 0, :written 0}
            partitions)))

(defn- ->ingest-stage [size opts]
  {:t :call, :stage :ingest
   :f (fn [{:keys [node]}]
        (with-open [conn (xt/open-adbc-conn node)]
          (let [al (.getAllocator ^Xtdb$Connection conn)
                dims (mapv #(assoc % :rows (read-table al (dataset-file size (:file %)))) dimensions)]
            (log/infof "ingesting %s: %s" (name size)
                       (str/join ", " (map #(format "%d %s" (count (:rows %)) (:table %)) dims)))

            (let [{:keys [partitions written]}
                  (ingest! conn dims opts (arrow-partitions al (dataset-file size "prices.arrow")))]
              (log/infof "ingested %d partitions, %d rows written" partitions written)))))})

(defn- ->query-stage [interval]
  {:t :call, :stage (keyword (str "query-" (name interval)))
   :f (fn [{:keys [node]}]
        (with-open [conn (xt/open-adbc-conn node)]
          (case interval
            ;; the latest price for every product in every region
            :latest (xt/q conn (format "SELECT p.place_id, p.product_id, AVG(p.price) AS avg_price
                                        FROM %s AS p GROUP BY p.place_id, p.product_id" prices-table))

            ;; the same question a few years of valid time ago — the read the library's `asof` hint serves
            :as-of (xt/q conn [(format "SELECT p.place_id, p.product_id, AVG(p.price) AS avg_price
                                        FROM %s FOR VALID_TIME AS OF ? AS p
                                        GROUP BY p.place_id, p.product_id" prices-table)
                               (.toInstant (.atStartOfDay (LocalDate/parse "2017-01-01") ZoneOffset/UTC))])

            ;; the whole restatement history, which is what the bitemporality is being paid for
            :history (xt/q conn (format "SELECT p.product_id, COUNT(*) AS versions
                                         FROM %s FOR ALL VALID_TIME AS p
                                         GROUP BY p.product_id" prices-table)))))})

(defmethod b/cli-flags :food-prices [_]
  [[nil "--size SIZE" "dataset size"
    :parse-fn keyword
    :default :small
    :validate [#{:tiny :small :med :big} "size must be one of tiny, small, med, big"]]

   [nil "--dropout" "close out rows the incoming batch no longer mentions"
    :id :dropout?]

   ["-h" "--help"]])

(defmethod b/->benchmark :food-prices [_ {:keys [size dropout? seed no-load?] :or {seed 0}}]
  (log/info {:size size :dropout? dropout? :seed seed :no-load? no-load?})
  {:title "Food Prices"
   :benchmark-type :food-prices
   :seed seed
   :parameters {:size size :dropout? dropout? :seed seed :no-load? no-load?}
   :tasks (concat (when-not no-load?
                    [{:t :call, :stage :download, :setup? true
                      :f (fn [_] (download-dataset size))}

                     (->ingest-stage size {:dropout? dropout?})

                     {:t :call, :stage :sync
                      :f (fn [{:keys [node]}] (b/sync-node node (Duration/ofHours 5)))}

                     {:t :call, :stage :finish-block
                      :f (fn [{:keys [node]}] (b/flush-block! node))}

                     {:t :call, :stage :compact
                      :f (fn [{:keys [node]}] (b/compact! node))}])

                  (map ->query-stage [:latest :as-of :history]))})

(comment
  ;; a REPL run against the tiny dataset. A `^:benchmark` deftest, as the other bench namespaces
  ;; carry, would be discovered by the module's test task — this namespace has a companion test,
  ;; so it gets loaded there — and would then fail the build reaching for S3.
  (util/with-tmp-dirs #{node-tmp-dir}
    (-> (b/->benchmark :food-prices {:size :tiny})
        (b/run-benchmark {:node-dir node-tmp-dir}))))
