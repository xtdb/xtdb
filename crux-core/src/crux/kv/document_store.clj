(ns ^:no-doc crux.kv.document-store
  (:require [crux.codec :as c]
            [crux.db :as db]
            [crux.document-store :as ds]
            [crux.memory :as mem]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.system :as sys])
  (:import java.util.function.Supplier
           org.agrona.ExpandableDirectByteBuffer
           crux.codec.Id
           clojure.lang.MapEntry
           (java.io Closeable)
           (org.agrona DirectBuffer MutableDirectBuffer)))

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

(defn ->document-store {::sys/deps {:kv-store 'crux.mem-kv/->kv-store}
                        ::sys/args {:doc-cache-size ds/doc-cache-size-opt}}
  [{:keys [kv-store doc-cache-size]}]
  (ds/->CachedDocumentStore (lru/new-cache doc-cache-size) (->KvDocumentStore kv-store)))
