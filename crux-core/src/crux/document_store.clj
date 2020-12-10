(ns ^:no-doc crux.document-store
  (:require [clojure.java.io :as io]
            [crux.io :as cio]
            [clojure.set :as set]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.cache :as cache]
            [crux.memory :as mem]
            [taoensso.nippy :as nippy]
            [crux.system :as sys])
  (:import clojure.lang.MapEntry
           (java.io Closeable DataInputStream DataOutputStream FileInputStream FileOutputStream)
           (java.nio.file Path)))

(defrecord FileDocumentStore [dir]
  db/DocumentStore
  (fetch-docs [this ids]
    (cio/with-nippy-thaw-all
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
        (transient {}) ids))))

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
          (assoc! acc id (cache/compute-if-absent
                          cache
                          (c/->id-buffer id)
                          mem/copy-buffer-to-root-allocator
                          (fn [_]
                            doc))))
        (transient cached-id->docs)
        missing-id->docs))))

  (submit-docs [this id-and-docs]
    (db/submit-docs
     document-store
     (vec (for [[id doc] id-and-docs]
            (do
              (cache/evict cache (c/->id-buffer id))
              (MapEntry/create id doc))))))

  Closeable
  (close [_]))

(defn ->cached-document-store
  {::sys/deps {:document-store :crux/document-store
               :document-cache 'crux.cache/->cache}}
  [{:keys [document-cache document-store]}]
  (->CachedDocumentStore document-cache document-store))

(defn ->file-document-store {::sys/deps {:document-cache 'crux.cache/->cache}
                             ::sys/args {:dir {:doc "Directory to store documents"
                                               :required? true
                                               :spec ::sys/path}}}
  [{:keys [^Path dir document-cache] :as opts}]
  (let [dir (.toFile dir)]
    (.mkdirs dir)
    (->cached-document-store
     (assoc opts
            :document-cache document-cache
            :document-store (->FileDocumentStore dir)))))
