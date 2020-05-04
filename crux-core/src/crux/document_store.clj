(ns ^:no-doc crux.document-store
  (:require [clojure.java.io :as io]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [taoensso.nippy :as nippy])
  (:import [java.io Closeable DataInputStream DataOutputStream FileInputStream FileOutputStream]
           java.util.function.Supplier
           org.agrona.ExpandableDirectByteBuffer
           crux.kv.KvSnapshot))

(def ^:private ^ThreadLocal seek-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

(defrecord KvDocumentStore [kv]
  db/DocumentStore
  (fetch-docs [this ids]
    (with-open [snapshot (kv/new-snapshot kv)]
      (->> (for [k ids
                 :let [seek-k (c/encode-doc-key-to (.get seek-buffer-tl) (c/->id-buffer k))
                       doc (some-> (kv/get-value snapshot seek-k) (c/<-nippy-buffer))]
                 :when doc]
             [k doc])
           (into {}))))

  (submit-docs [this id-and-docs]
    (kv/store kv (for [[k v] id-and-docs]
                   [(c/encode-doc-key-to nil (c/->id-buffer k))
                    (c/->nippy-buffer v)])))

  Closeable
  (close [_]))

(defrecord FileDocumentStore [dir]
  db/DocumentStore
  (fetch-docs [this ids]
    (->> (for [k ids
               :let [v (let [doc-key (str (c/new-id k))
                             doc-file (io/file dir doc-key)]
                         (when (.exists doc-file)
                           (with-open [in (FileInputStream. doc-file)]
                             (some->> in
                                      (DataInputStream.)
                                      (nippy/thaw-from-in!)))))]
               :when v]
           [k v])
         (into {})))

  (submit-docs [this id-and-docs]
    (doseq [[k v] id-and-docs
            :let [doc-key (str (c/new-id k))]]
      (with-open [out (DataOutputStream. (FileOutputStream. (io/file dir doc-key)))]
        (nippy/freeze-to-out! out v))))

  Closeable
  (close [_]))

(defrecord CachedDocumentStore [cache doc-store]
  db/DocumentStore
  (fetch-docs [this ids]
    (->> (for [k ids
               :let [v (lru/compute-if-absent
                        cache
                        (c/->id-buffer k)
                        mem/copy-to-unpooled-buffer
                        #(get (db/fetch-docs doc-store [%]) %))]
               :when v]
           [k v])
         (into {})))

  (submit-docs [this id-and-docs]
    (db/submit-docs
     doc-store
     (for [[k v] id-and-docs
           :let [k (c/->id-buffer k)]]
       (do
         (lru/evict cache k)
         [k v]))))

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
  {:start-fn (fn [{:keys [crux.node/kv-store]} {:keys [::doc-cache-size crux.index/file-doc-store-dir]}]
               (.mkdirs (io/file file-doc-store-dir))
               (->CachedDocumentStore (lru/new-cache doc-cache-size) (->FileDocumentStore file-doc-store-dir)))
   :deps [:crux.node/kv-store]
   :args {::file-doc-store-dir {:doc "Directory to store documents"
                                :required? true
                                :crux.config/type :crux.config/string}
          ::doc-cache-size doc-cache-size-opt}})
