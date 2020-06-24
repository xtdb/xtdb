(ns ^:no-doc crux.document-store
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [taoensso.nippy :as nippy])
  (:import [java.io Closeable DataInputStream DataOutputStream FileInputStream FileOutputStream]
           java.util.function.Supplier
           org.agrona.ExpandableDirectByteBuffer
           [org.agrona DirectBuffer MutableDirectBuffer]
           clojure.lang.MapEntry
           crux.codec.Id
           crux.kv.KvSnapshot))

(def ^:private ^ThreadLocal seek-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

(defn encode-doc-key-to ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer content-hash]
  (assert (= c/id-size (.capacity content-hash)) (mem/buffer->hex content-hash))
  (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size c/id-size)))]
    (mem/limit-buffer
     (doto b
       (.putByte 0 c/content-hash->doc-index-id)
       (.putBytes c/index-id-size (mem/as-buffer content-hash) 0 (.capacity content-hash)))
     (+ c/index-id-size c/id-size))))

(defn decode-doc-key-from ^crux.codec.Id [^MutableDirectBuffer k]
  (assert (= (+ c/index-id-size c/id-size) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= c/content-hash->doc-index-id index-id))
    (Id. (mem/slice-buffer k c/index-id-size c/id-size) 0)))

(defrecord KvDocumentStore [kv]
  db/DocumentStore
  (fetch-docs [this ids]
    (with-open [snapshot (kv/new-snapshot kv)]
      (persistent!
       (reduce
        (fn [acc id]
          (let [seek-k (encode-doc-key-to (.get seek-buffer-tl) (c/->id-buffer id))]
            (if-let [doc (some-> (kv/get-value snapshot seek-k) (mem/<-nippy-buffer))]
              (assoc! acc id doc)
              acc)))
        (transient {}) ids))))

  (submit-docs [this id-and-docs]
    (kv/store kv (for [[id doc] id-and-docs]
                   (MapEntry/create (encode-doc-key-to nil (c/->id-buffer id))
                                    (mem/->nippy-buffer doc)))))

  Closeable
  (close [_]))

(defrecord FileDocumentStore [dir]
  db/DocumentStore
  (fetch-docs [this ids]
    (persistent!
     (reduce
      (fn [acc id]
        (let [doc-key (str (c/new-id id))
              doc-file (io/file dir doc-key)]
          (if-let [doc (when (.exists doc-file)
                         (with-open [in (FileInputStream. doc-file)]
                           (some->> in
                                    (DataInputStream.)
                                    (nippy/thaw-from-in!))))]
            (assoc! acc id doc)
            acc)))
      (transient {}) ids)))

  (submit-docs [this id-and-docs]
    (doseq [[id doc] id-and-docs
            :let [doc-key (str (c/new-id id))]]
      (with-open [out (DataOutputStream. (FileOutputStream. (io/file dir doc-key)))]
        (nippy/freeze-to-out! out doc))))

  Closeable
  (close [_]))

(defrecord CachedDocumentStore [cache document-store]
  db/DocumentStore
  (fetch-docs [this ids]
    (let [ids (set ids)
          cached-id->docs (persistent!
                           (reduce
                            (fn [acc id]
                              (if-let [doc (get cache (c/->id-buffer id))]
                                (assoc! acc id doc)
                                acc))
                            (transient {}) ids))
          missing-ids (set/difference ids (keys cached-id->docs))
          missing-id->docs (db/fetch-docs document-store missing-ids)]
      (persistent!
       (reduce-kv
        (fn [acc id doc]
          (assoc! acc id (lru/compute-if-absent
                          cache
                          (c/->id-buffer id)
                          mem/copy-to-unpooled-buffer
                          (fn [_]
                            doc))))
        (transient cached-id->docs)
        missing-id->docs))))

  (submit-docs [this id-and-docs]
    (db/submit-docs
     document-store
     (vec (for [[id doc] id-and-docs]
            (do
              (lru/evict cache (c/->id-buffer id))
              (MapEntry/create id doc))))))

  Closeable
  (close [_]))

(def ^:const default-doc-cache-size (* 128 1024))

(def doc-cache-size-opt {:doc "Cache size to use for document store."
                         :default default-doc-cache-size
                         :crux.config/type :crux.config/nat-int})

(def kv-document-store
  {:start-fn (fn [{:keys [crux.node/kv-store]} {::keys [doc-cache-size]}]
               (->CachedDocumentStore (lru/new-cache doc-cache-size) (->KvDocumentStore kv-store)))
   :deps [:crux.node/kv-store]
   :args {::doc-cache-size doc-cache-size-opt}})

(def file-document-store
  {:start-fn (fn [_ {:keys [::doc-cache-size crux.index/file-document-store-dir]}]
               (.mkdirs (io/file file-document-store-dir))
               (->CachedDocumentStore (lru/new-cache doc-cache-size) (->FileDocumentStore file-document-store-dir)))
   :args {::file-document-store-dir {:doc "Directory to store documents"
                                     :required? true
                                     :crux.config/type :crux.config/string}
          ::doc-cache-size doc-cache-size-opt}})
