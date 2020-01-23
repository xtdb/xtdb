(ns crux.object-store
  (:require [clojure.java.io :as io]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as i]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [taoensso.nippy :as nippy])
  (:import [java.io Closeable DataInputStream DataOutputStream FileInputStream FileOutputStream]
           org.agrona.io.DirectBufferInputStream))

(defn- keep-non-evicted-doc
  [doc]
  (when-not (i/evicted-doc? doc)
    doc))

(defrecord KvObjectStore [kv]
  db/ObjectStore
  (get-single-object [this snapshot k]
    (let [doc-key (c/->id-buffer k)
          seek-k (c/encode-doc-key-to (.get i/seek-buffer-tl) doc-key)]
      (some->> (kv/get-value snapshot seek-k)
               (DirectBufferInputStream.)
               (DataInputStream.)
               (nippy/thaw-from-in!)
               (keep-non-evicted-doc))))

  (get-objects [this snapshot ks]
    (->> (for [k ks
               :let [seek-k (c/encode-doc-key-to (.get i/seek-buffer-tl) (c/->id-buffer k))
                     v (kv/get-value snapshot seek-k)]
               :when v
               :let [doc (nippy/thaw-from-in! (DataInputStream. (DirectBufferInputStream. v)))]
               :when (keep-non-evicted-doc doc)]
           [k doc])
         (into {})))

  (put-objects [this kvs]
    (kv/store kv (for [[k v] kvs]
                   [(c/encode-doc-key-to nil (c/->id-buffer k))
                    (mem/->off-heap (nippy/fast-freeze v))])))

  (delete-objects [this ks]
    (kv/delete kv (for [k ks]
                    (c/encode-doc-key-to nil (c/->id-buffer k)))))

  Closeable
  (close [_]))

(defrecord FileObjectStore [dir]
  db/ObjectStore
  (get-single-object [this _ k]
    (let [doc-key (str (c/new-id k))
          doc-file (io/file dir doc-key)]
      (when (.exists doc-file)
        (with-open [in (FileInputStream. doc-file)]
          (some->> in
                   (DataInputStream.)
                   (nippy/thaw-from-in!)
                   (keep-non-evicted-doc))))))

  (get-objects [this _ ks]
    (->> (for [k ks
               :let [v (db/get-single-object this _ k)]
               :when v]
           [k v])
         (into {})))

  (put-objects [this kvs]
    (doseq [[k v] kvs
            :let [doc-key (str (c/new-id k))]]
      (with-open [out (DataOutputStream. (FileOutputStream. (io/file dir doc-key)))]
        (nippy/freeze-to-out! out v))))

  (delete-objects [this ks]
    (doseq [k ks
            :let [doc-key (str (c/new-id k))]]
      (.delete (io/file dir doc-key))))

  Closeable
  (close [_]))

(defrecord CachedObjectStore [cache object-store]
  db/ObjectStore
  (get-single-object [this snapshot k]
    (lru/compute-if-absent
     cache
     (c/->id-buffer k)
     mem/copy-to-unpooled-buffer
     #(db/get-single-object object-store snapshot %)))


  (get-objects [this snapshot ks]
    (->> (for [k ks
               :let [v (db/get-single-object this snapshot k)]
               :when v]
           [k v])
         (into {})))

  (put-objects [this kvs]
    (db/put-objects
      object-store
      (for [[k v] kvs
            :let [k (c/->id-buffer k)]]
        (do
          (lru/evict cache k)
          [k v]))))

  (delete-objects [this ks]
    (db/delete-objects
      object-store
      (for [k ks
            :let [k (c/->id-buffer k)]]
        (do (lru/evict cache k) k))))

  Closeable
  (close [_]))

(def ^:const default-doc-cache-size (* 128 1024))

(def doc-cache-size-opt {:doc "Cache size to use for document store."
                         :default default-doc-cache-size
                         :crux.config/type :crux.config/nat-int})

(def kv-object-store
  {:start-fn (fn [{:keys [crux.node/kv-store]} {::keys [doc-cache-size]}]
               (->CachedObjectStore (lru/new-cache doc-cache-size) (->KvObjectStore kv-store)))
   :deps [:crux.node/kv-store]
   :args {::doc-cache-size doc-cache-size-opt}})

(def file-object-store
  {:start-fn (fn [{:keys [crux.node/kv-store]} {:keys [::doc-cache-size crux.index/file-object-store-dir]}]
               (.mkdirs (io/file file-object-store-dir))
               (->CachedObjectStore (lru/new-cache doc-cache-size) (->FileObjectStore file-object-store-dir)))
   :deps [:crux.node/kv-store]
   :args {::file-object-store-dir {:doc "Directory to store objects"
                                   :required? true
                                   :crux.config/type :crux.config/string}
          ::doc-cache-size doc-cache-size-opt}})
