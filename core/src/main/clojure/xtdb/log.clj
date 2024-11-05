(ns xtdb.log
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            xtdb.protocols
            [xtdb.sql.plan :as plan]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import java.lang.AutoCloseable
           (java.nio.channels ClosedChannelException)
           (java.time Instant)
           java.time.Duration
           (java.util ArrayList HashMap)
           (java.util.concurrent Semaphore)
           org.apache.arrow.memory.BufferAllocator
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union FieldType Schema)
           org.apache.arrow.vector.types.UnionMode
           (xtdb.api.log Log Log$Factory TxLog$Record TxLog$Subscriber)
           (xtdb.api.tx TxOp$Sql TxOptions)
           (xtdb.tx_ops Abort AssertExists AssertNotExists Call Delete DeleteDocs Erase EraseDocs Insert PutDocs SqlByteArgs Update XtqlAndArgs)
           xtdb.types.ClojureForm
           xtdb.vector.IVectorWriter))

(set! *unchecked-math* :warn-on-boxed)

(def ^java.util.concurrent.ThreadFactory subscription-thread-factory
  (util/->prefix-thread-factory "xtdb-tx-subscription"))

(defn- tx-handler [^TxLog$Subscriber subscriber]
  (fn [_last-tx-id ^TxLog$Record record]
    (when (Thread/interrupted)
      (throw (InterruptedException.)))

    (.accept subscriber record)

    (.getTxId (.getTxKey record))))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(defn handle-polling-subscription [^Log log, after-tx-id, {:keys [^Duration poll-sleep-duration]}, ^TxLog$Subscriber subscriber]
  (doto (.newThread subscription-thread-factory
                    (fn []
                      (let [thread (Thread/currentThread)]
                        (.setPriority thread Thread/MAX_PRIORITY)
                        (.onSubscribe subscriber (reify AutoCloseable
                                                   (close [_]
                                                     (.interrupt thread)
                                                     (.join thread)))))
                      (try
                        (loop [after-tx-id after-tx-id]
                          (let [last-tx-id (reduce (tx-handler subscriber)
                                                   after-tx-id
                                                   (try
                                                     (.readTxs log after-tx-id 100)
                                                     (catch ClosedChannelException e (throw e))
                                                     (catch InterruptedException e (throw e))
                                                     (catch Exception e
                                                       (log/warn e "Error polling for txs, will retry"))))]
                            (when (Thread/interrupted)
                              (throw (InterruptedException.)))
                            (when (= after-tx-id last-tx-id)
                              (Thread/sleep (.toMillis poll-sleep-duration)))
                            (recur last-tx-id)))
                        (catch InterruptedException _)
                        (catch ClosedChannelException _))))
    (.start)))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface INotifyingSubscriberHandler
  (notifyTx [^xtdb.api.TransactionKey tx])
  (subscribe [^xtdb.api.log.Log log, ^Long after-tx-id, ^xtdb.api.log.TxLog$Subscriber subscriber]))

(defrecord NotifyingSubscriberHandler [!state]
  INotifyingSubscriberHandler
  (notifyTx [_ tx]
    (let [{:keys [semaphores]} (swap! !state assoc :latest-submitted-tx-id (.getTxId tx))]
      (doseq [^Semaphore semaphore semaphores]
        (.release semaphore))))

  (subscribe [_ log after-tx-id subscriber]
    (let [semaphore (Semaphore. 0)
          {:keys [latest-submitted-tx-id]} (swap! !state update :semaphores conj semaphore)]

      (doto (.newThread subscription-thread-factory
                        (fn []
                          (let [thread (Thread/currentThread)]
                            (.setPriority thread Thread/MAX_PRIORITY)
                            (.onSubscribe subscriber (reify AutoCloseable
                                                       (close [_]
                                                         (.interrupt thread)
                                                         (.join thread)))))
                          (try
                            (loop [after-tx-id after-tx-id]
                              (let [last-tx-id (reduce (tx-handler subscriber)
                                                       after-tx-id
                                                       (if (and latest-submitted-tx-id
                                                                (or (nil? after-tx-id)
                                                                    (< ^long after-tx-id ^long latest-submitted-tx-id)))
                                                         ;; catching up
                                                         (->> (.readTxs log after-tx-id 100)
                                                              (take-while #(<= ^long (.getTxId (.getTxKey ^TxLog$Record %))
                                                                               ^long latest-submitted-tx-id)))

                                                         ;; running live
                                                         (let [permits (do
                                                                         (.acquire semaphore)
                                                                         (inc (.drainPermits semaphore)))]
                                                           (.readTxs log after-tx-id
                                                                     (if (> permits 100)
                                                                           (do
                                                                             (.release semaphore (- permits 100))
                                                                             100)
                                                                           permits)))))]
                                (when-not (Thread/interrupted)
                                  (recur last-tx-id))))

                            (catch InterruptedException _)

                            (catch ClosedChannelException ex
                              (when-not (Thread/interrupted)
                                (throw ex)))

                            (finally
                              (swap! !state update :semaphores disj semaphore)))))
        (.start)))))

(defn ->notifying-subscriber-handler [latest-submitted-tx-id]
  (->NotifyingSubscriberHandler (atom {:latest-submitted-tx-id latest-submitted-tx-id
                                       :semaphores #{}})))

;; header bytes
(def ^:const hb-user-arrow-transaction
  "Header byte for log records representing an arrow user transaction.

  A standard arrow stream IPC buffer will contain this byte, so you do not need to prefix."
  255)

(def ^:const hb-flush-chunk
  "Header byte for log records representing a signal to flush the live chunk to durable storage.

  Can be useful to protect against data loss potential when a retention period is used for the log, so messages do not remain in the log forever.

  TxRecord layout:

  - header (byte=2)

  - expected-last-tx-id in previous chunk (long)
  If this tx-id match the last tx-id who has been indexed in durable storage, then this signal is ignored.
  This is to avoid a herd effect in multi-node environments where multiple flush signals for the same chunk might be received.

  See xtdb.stagnant-log-flusher"
  2)

(def ^:private ^org.apache.arrow.vector.types.pojo.Field tx-ops-field
  (types/->field "tx-ops" (ArrowType$Union. UnionMode/Dense nil) false))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema tx-schema
  (Schema. [(types/->field "tx-ops" #xt.arrow/type :list false tx-ops-field)

            (types/col-type->field "system-time" types/nullable-temporal-type)
            (types/col-type->field "default-tz" :utf8)]))

(def ^:private forbidden-tables #{"xt/" "information_schema/" "pg_catalog/"})

(defn forbidden-table? [table-name]
  (when-not (= table-name "xt/tx_fns")
    (some (fn [s] (str/starts-with? table-name s)) forbidden-tables)))

(defn forbidden-table-ex [table-name]
  (err/illegal-arg :xtdb/forbidden-table
                   {::err/message (format "Cannot write to table: %s" table-name)
                    :table-name table-name}))

(defn- ->xtql+args-writer [^IVectorWriter op-writer, ^BufferAllocator allocator]
  (let [xtql-writer (.legWriter op-writer "xtql" (FieldType/notNullable #xt.arrow/type :struct))
        xtql-op-writer (.structKeyWriter xtql-writer "op" (FieldType/notNullable #xt.arrow/type :transit))
        args-writer (.structKeyWriter xtql-writer "args" (FieldType/nullable #xt.arrow/type :varbinary))]
    (fn write-xtql+args! [{:keys [op arg-rows]}]
      (.startStruct xtql-writer)
      (.writeObject xtql-op-writer (ClojureForm. op))

      (when arg-rows
        (util/with-open [args-wtr (vw/->vec-writer allocator "args" (FieldType/notNullable #xt.arrow/type :struct))]
          (doseq [arg-row arg-rows]
            (.writeObject args-wtr arg-row))

          (.syncValueCount args-wtr)

          (.writeBytes args-writer
                       (util/build-arrow-ipc-byte-buffer (VectorSchemaRoot. ^Iterable (seq (.getVector args-wtr))) :stream
                         (fn [write-batch!]
                           (write-batch!))))))

      (.endStruct xtql-writer))))

(defn- ->xtql-writer [^IVectorWriter op-writer]
  (let [xtql-writer (.legWriter op-writer "xtql" (FieldType/notNullable #xt.arrow/type :struct))
        xtql-op-writer (.structKeyWriter xtql-writer "op" (FieldType/notNullable #xt.arrow/type :transit))]

    ;; create this even if it's not required here
    (.structKeyWriter xtql-writer "args" (FieldType/nullable #xt.arrow/type :varbinary))

    (fn write-xtql! [op]
      (.startStruct xtql-writer)
      (.writeObject xtql-op-writer (ClojureForm. op))
      (.endStruct xtql-writer))))

(defn encode-sql-args [^BufferAllocator allocator, arg-rows]
  (if (apply not= (map count arg-rows))
    (throw (err/illegal-arg :sql/arg-rows-different-lengths
                            {::err/message "All SQL arg-rows must have the same number of columns"
                             :arg-rows arg-rows}))
    (let [param-count (count (first arg-rows))
          vecs (ArrayList. param-count)]
      (try
        (dotimes [col-idx param-count]
          (.add vecs
                (vw/open-vec allocator (symbol (str "?_" col-idx))
                             (mapv #(nth % col-idx nil) arg-rows))))

        (let [root (doto (VectorSchemaRoot. vecs) (.setRowCount (count arg-rows)))]
          (util/build-arrow-ipc-byte-buffer root :stream
                                            (fn [write-batch!]
                                              (write-batch!))))

        (finally
          (run! util/try-close vecs))))))

(defn- ->sql-writer [^IVectorWriter op-writer, ^BufferAllocator allocator]
  (let [sql-writer (.legWriter op-writer "sql" (FieldType/notNullable #xt.arrow/type :struct))
        query-writer (.structKeyWriter sql-writer "query" (FieldType/notNullable #xt.arrow/type :utf8))
        args-writer (.structKeyWriter sql-writer "args" (FieldType/nullable #xt.arrow/type :varbinary))]
    (fn write-sql! [^TxOp$Sql op]
      (let [sql (.sql op)]
        (.startStruct sql-writer)
        (.writeObject query-writer sql)

        (when-let [arg-rows (not-empty (.argRows op))]
          (.writeObject args-writer (encode-sql-args allocator arg-rows))))

      (.endStruct sql-writer))))

(defn- ->sql-byte-args-writer [^IVectorWriter op-writer]
  (let [sql-writer (.legWriter op-writer "sql" (FieldType/notNullable #xt.arrow/type :struct))
        query-writer (.structKeyWriter sql-writer "query" (FieldType/notNullable #xt.arrow/type :utf8))
        args-writer (.structKeyWriter sql-writer "args" (FieldType/nullable #xt.arrow/type :varbinary))]
    (fn write-sql! [{:keys [sql arg-bytes]}]
      (.startStruct sql-writer)
      (.writeObject query-writer sql)

      (when arg-bytes
        (.writeObject args-writer arg-bytes))

      (.endStruct sql-writer))))

(defn- ->put-writer [^IVectorWriter op-writer]
  (let [put-writer (.legWriter op-writer "put-docs" (FieldType/notNullable #xt.arrow/type :struct))
        iids-writer (.structKeyWriter put-writer "iids" (FieldType/notNullable #xt.arrow/type :list))
        iid-writer (some-> iids-writer
                           (.listElementWriter (FieldType/notNullable #xt.arrow/type [:fixed-size-binary 16])))
        doc-writer (.structKeyWriter put-writer "documents" (FieldType/notNullable #xt.arrow/type :union))
        valid-from-writer (.structKeyWriter put-writer "_valid_from" types/nullable-temporal-field-type)
        valid-to-writer (.structKeyWriter put-writer "_valid_to" types/nullable-temporal-field-type)
        table-doc-writers (HashMap.)]
    (fn write-put! [{:keys [table-name docs valid-from valid-to]}]
      (let [table-name (str (symbol (util/with-default-schema table-name)))]
        (when (forbidden-table? table-name) (throw (forbidden-table-ex table-name)))
        (.startStruct put-writer)
        (let [^IVectorWriter table-doc-writer
              (.computeIfAbsent table-doc-writers table-name
                                (fn [table]
                                  (doto (.legWriter doc-writer table (FieldType/notNullable #xt.arrow/type :list))
                                    (.listElementWriter (FieldType/notNullable #xt.arrow/type :struct)))))]

          (.writeObject table-doc-writer docs)

          (.startList iids-writer)
          (doseq [doc docs
                  :let [eid (val (or (->> doc
                                          (some (fn [e]
                                                  (when (.equals "_id" (util/->normal-form-str (key e)))
                                                    e))))
                                     (throw (err/illegal-arg :missing-id {:doc doc}))))]]
            (.writeBytes iid-writer (trie/->iid eid)))
          (.endList iids-writer))

        (.writeObject valid-from-writer valid-from)
        (.writeObject valid-to-writer valid-to)

        (.endStruct put-writer)))))

(defn- ->delete-writer [^IVectorWriter op-writer]
  (let [delete-writer (.legWriter op-writer "delete-docs" (FieldType/notNullable #xt.arrow/type :struct))
        table-writer (.structKeyWriter delete-writer "table" (FieldType/notNullable #xt.arrow/type :utf8))
        iids-writer (.structKeyWriter delete-writer "iids" (FieldType/notNullable #xt.arrow/type :list))
        iid-writer (some-> iids-writer
                           (.listElementWriter (FieldType/notNullable #xt.arrow/type [:fixed-size-binary 16])))
        valid-from-writer (.structKeyWriter delete-writer "_valid_from" types/nullable-temporal-field-type)
        valid-to-writer (.structKeyWriter delete-writer "_valid_to" types/nullable-temporal-field-type)]
    (fn write-delete! [{:keys [table-name doc-ids valid-from valid-to]}]
      (let [table-name (str (symbol (util/with-default-schema table-name)))]
        (when (forbidden-table? table-name) (throw (forbidden-table-ex table-name)))
        (when (seq doc-ids)
          (.startStruct delete-writer)

          (.writeObject table-writer table-name)

          (.startList iids-writer)
          (doseq [doc-id doc-ids]
            (.writeObject iid-writer (trie/->iid doc-id)))
          (.endList iids-writer)

          (.writeObject valid-from-writer valid-from)
          (.writeObject valid-to-writer valid-to)

          (.endStruct delete-writer))))))

(defn- ->erase-writer [^IVectorWriter op-writer]
  (let [erase-writer (.legWriter op-writer "erase-docs" (FieldType/notNullable #xt.arrow/type :struct))
        table-writer (.structKeyWriter erase-writer "table" (FieldType/notNullable #xt.arrow/type :utf8))
        iids-writer (.structKeyWriter erase-writer "iids" (FieldType/notNullable #xt.arrow/type :list))
        iid-writer (some-> iids-writer
                           (.listElementWriter (FieldType/notNullable #xt.arrow/type [:fixed-size-binary 16])))]
    (fn [{:keys [table-name doc-ids]}]
      (let [table-name (str (symbol (util/with-default-schema table-name)))]
        (when (forbidden-table? table-name) (throw (forbidden-table-ex table-name)))
        (when (seq doc-ids)
          (.startStruct erase-writer)
          (.writeObject table-writer table-name)

          (.startList iids-writer)
          (doseq [doc-id doc-ids]
            (.writeObject iid-writer (trie/->iid doc-id)))
          (.endList iids-writer)

          (.endStruct erase-writer))))))

(defn- ->call-writer [^IVectorWriter op-writer]
  (let [call-writer (.legWriter op-writer "call" (FieldType/notNullable #xt.arrow/type :struct))
        fn-iid-writer (.structKeyWriter call-writer "fn-iid" (FieldType/notNullable #xt.arrow/type [:fixed-size-binary 16]))
        args-list-writer (.structKeyWriter call-writer "args" (FieldType/notNullable #xt.arrow/type :transit))]
    (fn write-call! [{:keys [fn-id args]}]
      (.startStruct call-writer)

      (.writeObject fn-iid-writer (trie/->iid fn-id))

      (let [clj-form (xt/->ClojureForm (vec args))]
        (.writeObject args-list-writer clj-form))

      (.endStruct call-writer))))

(defn- ->abort-writer [^IVectorWriter op-writer]
  (let [abort-writer (.legWriter op-writer "abort" (FieldType/nullable #xt.arrow/type :null))]
    (fn [_op]
      (.writeNull abort-writer))))

(defn open-tx-ops-vec ^org.apache.arrow.vector.ValueVector [^BufferAllocator allocator]
  (.createVector tx-ops-field allocator))

(defn write-tx-ops! [^BufferAllocator allocator, ^IVectorWriter op-writer, tx-ops, {:keys [default-tz]}]
  (let [!write-xtql+args! (delay (->xtql+args-writer op-writer allocator))
        !write-xtql! (delay (->xtql-writer op-writer))
        !write-sql! (delay (->sql-writer op-writer allocator))
        !write-sql-byte-args! (delay (->sql-byte-args-writer op-writer))
        !write-put! (delay (->put-writer op-writer))
        !write-delete! (delay (->delete-writer op-writer))
        !write-erase! (delay (->erase-writer op-writer))
        !write-call! (delay (->call-writer op-writer))
        !write-abort! (delay (->abort-writer op-writer))]

    (doseq [tx-op tx-ops]
      (condp instance? tx-op
        XtqlAndArgs (@!write-xtql+args! tx-op)
        Insert (@!write-xtql! tx-op)
        Update (@!write-xtql! tx-op)
        Delete (@!write-xtql! tx-op)
        Erase (@!write-xtql! tx-op)
        AssertExists (@!write-xtql! tx-op)
        AssertNotExists (@!write-xtql! tx-op)

        TxOp$Sql (let [^TxOp$Sql tx-op tx-op]
                   (if-let [put-docs-ops (plan/sql->put-docs-ops (.sql tx-op) (.argRows tx-op) {:default-tz default-tz})]
                     (doseq [op put-docs-ops]
                       (@!write-put! op))

                     (@!write-sql! tx-op)))

        SqlByteArgs (@!write-sql-byte-args! tx-op)
        PutDocs (@!write-put! tx-op)
        DeleteDocs (@!write-delete! tx-op)
        EraseDocs (@!write-erase! tx-op)
        Call (@!write-call! tx-op)
        Abort (@!write-abort! tx-op)
        (throw (err/illegal-arg :invalid-tx-op {:tx-op tx-op}))))))

(defn serialize-tx-ops ^java.nio.ByteBuffer [^BufferAllocator allocator tx-ops {:keys [^Instant system-time, default-tz] :as opts}]
  (with-open [root (VectorSchemaRoot/create tx-schema allocator)]
    (let [ops-list-writer (vw/->writer (.getVector root "tx-ops"))

          default-tz-writer (vw/->writer (.getVector root "default-tz"))]

      (when system-time
        (.writeObject (vw/->writer (.getVector root "system-time")) system-time))

      (.writeObject default-tz-writer (str default-tz))

      (.startList ops-list-writer)
      (write-tx-ops! allocator (.listElementWriter ops-list-writer) tx-ops opts)
      (.endList ops-list-writer)

      (.setRowCount root 1)
      (.syncSchema root)

      (util/root->arrow-ipc-byte-buffer root :stream))))

(defmethod xtn/apply-config! :xtdb/log [config _ [tag opts]]
  (xtn/apply-config! config
                     (case tag
                       :in-memory :xtdb.log/memory-log
                       :local :xtdb.log/local-directory-log
                       :kafka :xtdb.kafka/log)
                     opts))

(defmethod ig/init-key :xtdb/log [_ ^Log$Factory factory]
  (.openLog factory))

(defmethod ig/halt-key! :xtdb/log [_ ^Log log]
  (util/close log))

(defn submit-tx& ^java.util.concurrent.CompletableFuture
  [{:keys [^BufferAllocator allocator, ^Log log, default-tz]} tx-ops ^TxOptions opts]

  (let [system-time (some-> opts .getSystemTime)]
    (.appendTx log (serialize-tx-ops allocator tx-ops
                                     {:default-tz (or (some-> opts .getDefaultTz) default-tz)
                                          :system-time system-time}))))
