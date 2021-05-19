(ns ^:no-doc crux.kv.document-store
  (:require [crux.codec :as c]
            [crux.io :as cio]
            [crux.db :as db]
            [crux.document-store :as ds]
            [crux.memory :as mem]
            [crux.kv :as kv]
            [crux.cache :as cache]
            [crux.system :as sys]
            [clojure.java.io :as io])
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
  (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size (.capacity content-hash))))]
    (mem/limit-buffer
     (doto b
       (.putByte 0 c/content-hash->doc-index-id)
       (cond-> content-hash (.putBytes c/index-id-size (mem/as-buffer content-hash) 0 (.capacity content-hash))))
     (+ c/index-id-size (.capacity content-hash)))))

(defn decode-doc-key-from ^crux.codec.Id [^MutableDirectBuffer k]
  (assert (= (+ c/index-id-size c/id-size) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= c/content-hash->doc-index-id index-id))
    (Id. (mem/slice-buffer k c/index-id-size c/id-size) 0)))

(defrecord KvDocumentStore [kv-store fsync?]
  db/DocumentStore
  (fetch-docs [_ ids]
    (cio/with-nippy-thaw-all
      (with-open [snapshot (kv/new-snapshot kv-store)]
        (persistent!
         (reduce
          (fn [acc id]
            (let [seek-k (encode-doc-key-to (.get seek-buffer-tl) (c/->id-buffer id))]
              (if-let [doc (some-> (kv/get-value snapshot seek-k) (mem/<-nippy-buffer))]
                (assoc! acc id doc)
                acc)))
          (transient {}) ids)))))

  (submit-docs [_ id-and-docs]
    (kv/store kv-store (for [[id doc] id-and-docs]
                         (MapEntry/create (encode-doc-key-to nil (c/->id-buffer id))
                                          (mem/->nippy-buffer doc))))
    (when fsync?
      (kv/fsync kv-store)))

  Closeable
  (close [_]))

(defn ->document-store {::sys/deps {:kv-store 'crux.mem-kv/->kv-store
                                    :document-cache 'crux.cache/->cache}
                        ::sys/args {:fsync? {:spec ::sys/boolean
                                             :required? true
                                             :default true}}}
  [{:keys [kv-store document-cache fsync?] :as opts}]
  (ds/->cached-document-store
   (assoc opts
          :document-cache document-cache
          :document-store (->KvDocumentStore kv-store fsync?))))

(defn -main [kv-store-config-file out-file]
  (with-open [sys (-> (sys/prep-system {:kv-store (read-string (slurp (io/file kv-store-config-file)))})
                      (sys/start-system))
              snapshot (kv/new-snapshot (:kv-store sys))
              iterator (kv/new-iterator snapshot)
              w (io/writer (io/file out-file))]
    (cio/with-nippy-thaw-all
      (letfn [(docs [^DirectBuffer k]
                (lazy-seq
                 (when (and k (= c/content-hash->doc-index-id (.getByte k 0)))
                   (cons [(decode-doc-key-from k)
                          (mem/<-nippy-buffer (kv/value iterator))]
                         (docs (kv/next iterator))))))]
        (doseq [e (docs (kv/seek iterator (encode-doc-key-to nil mem/empty-buffer)))]
          (.write w (prn-str e)))))))

(comment
  (-main "/home/james/src/juxt/crux/crux-core/src/crux/kv/kv-docs.edn"
         "/tmp/rocks-docs.edn"))
