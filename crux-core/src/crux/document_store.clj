(ns ^:no-doc crux.document-store
  (:require [clojure.set :as set]
            [crux.cache :as cache]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.memory :as mem]
            [crux.system :as sys]
            [taoensso.nippy :as nippy])
  (:import clojure.lang.MapEntry
           [java.io Closeable DataInputStream DataOutputStream]
           [java.nio.file Files LinkOption OpenOption Path StandardOpenOption]
           java.nio.file.attribute.FileAttribute
           [java.util.concurrent CompletableFuture Executors ExecutorService TimeUnit]
           [java.util.function Function Supplier]))

(defn- completable-future {:style/indent 1} ^java.util.concurrent.CompletableFuture
  [pool f]
  (CompletableFuture/supplyAsync
   (reify Supplier
     (get [_]
       (f)))
   pool))

(defrecord CachedDocumentStore [cache document-store]
  db/DocumentStore
  (fetch-docs-async [_ ids]
    (let [ids (set ids)
          cached-id->docs (persistent!
                           (reduce
                            (fn [acc id]
                              (if-let [doc (get cache (c/->id-buffer id))]
                                (assoc! acc id doc)
                                acc))
                            (transient {}) ids))]
      (if-let [missing-ids (not-empty (set/difference ids (set (keys cached-id->docs))))]
        (-> (db/fetch-docs-async document-store missing-ids)
            (.thenApply (reify Function
                          (apply [_ missing-id->docs]
                            (persistent!
                             (reduce-kv
                              (fn [acc id doc]
                                (assoc! acc id (cache/compute-if-absent
                                                cache
                                                (c/->id-buffer id)
                                                mem/copy-to-unpooled-buffer
                                                (fn [_]
                                                  doc))))
                              (transient cached-id->docs)
                              missing-id->docs))))))
        (CompletableFuture/completedFuture cached-id->docs))))

  (submit-docs-async [_ id-and-docs]
    (db/submit-docs-async
     document-store
     (vec (for [[id doc] id-and-docs]
            (do
              (cache/evict cache (c/->id-buffer id))
              (MapEntry/create id doc))))))

  Closeable
  (close [_]
    (cio/try-close document-store)))

(defn ->cached-document-store
  {::sys/deps {:document-store :crux/document-store
               :document-cache 'crux.cache/->cache}}
  [{:keys [document-cache document-store]}]
  (->CachedDocumentStore document-cache document-store))

(defrecord NIODocumentStore [^Path root-path, ^ExecutorService pool]
  db/DocumentStore
  (fetch-docs-async [_this ids]
    (let [futs (for [id ids]
                 (let [doc-path (.resolve root-path (str (c/new-id id)))]
                   (completable-future pool
                                       (fn []
                                         (when (Files/exists doc-path (make-array LinkOption 0))
                                           (with-open [in (Files/newInputStream doc-path (into-array OpenOption #{StandardOpenOption/READ}))]
                                             (cio/with-nippy-thaw-all
                                               (MapEntry/create id
                                                                (some->> in
                                                                         (DataInputStream.)
                                                                         (nippy/thaw-from-in!))))))))))]

      (-> (CompletableFuture/allOf (into-array CompletableFuture futs))
          (.thenApply (reify Function
                        (apply [_ _]
                          (into {} (map deref) futs)))))))

  (submit-docs-async [_this id-and-docs]
    (let [futs (vec (for [[id doc] id-and-docs
                          :let [doc-key (str (c/new-id id))]]
                      (completable-future pool
                        (fn []
                          (with-open [out (-> (.resolve root-path doc-key)
                                              (Files/newOutputStream (into-array OpenOption #{StandardOpenOption/CREATE
                                                                                              StandardOpenOption/WRITE
                                                                                              StandardOpenOption/TRUNCATE_EXISTING}))
                                              DataOutputStream.)]
                            (nippy/freeze-to-out! out doc))))))]

      (CompletableFuture/allOf (into-array CompletableFuture futs))))

  Closeable
  (close [_]
    (doto pool
      (.shutdownNow)
      (.awaitTermination 15 TimeUnit/SECONDS))))

(defn ->nio-document-store {::sys/deps {:document-cache 'crux.cache/->cache}
                            ::sys/args {:root-path {:doc "Path to store documents"
                                                    :required? true
                                                    :spec ::sys/path}
                                        :pool-size {:required? true
                                                    :default 4
                                                    :spec ::sys/pos-int}}}
  [{:keys [^Path root-path document-cache pool-size] :as opts}]

  (Files/createDirectories root-path (make-array FileAttribute 0))

  (->cached-document-store
   (assoc opts
          :document-cache document-cache
          :document-store (->NIODocumentStore root-path (Executors/newFixedThreadPool pool-size (cio/thread-factory "doc-store"))))))
