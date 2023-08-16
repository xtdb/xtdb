(ns xtdb.trie
  (:require [xtdb.buffer-pool]
            [xtdb.metadata :as meta]
            [xtdb.object-store]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (java.nio ByteBuffer)
           java.security.MessageDigest
           (java.util Arrays)
           (java.util.concurrent.atomic AtomicInteger)
           (java.util.function IntConsumer Supplier)
           (java.util.stream IntStream)
           java.security.MessageDigest
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector VectorSchemaRoot)
           org.apache.arrow.vector.types.UnionMode
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           (xtdb.object_store ObjectStore)
           (xtdb.trie HashTrie HashTrie$Node LiveHashTrie LiveHashTrie$Leaf)
           (xtdb.vector IVectorReader RelationReader)))

(def ^:private ^java.lang.ThreadLocal !msg-digest
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (MessageDigest/getInstance "SHA-256")))))

(defn ->iid ^ByteBuffer [eid]
  (if (uuid? eid)
    (util/uuid->byte-buffer eid)
    (ByteBuffer/wrap
     (let [^bytes eid-bytes (cond
                              (string? eid) (.getBytes (str "s" eid))
                              (keyword? eid) (.getBytes (str "k" eid))
                              (integer? eid) (.getBytes (str "i" eid))
                              :else (throw (UnsupportedOperationException. (pr-str (class eid)))))]
       (-> ^MessageDigest (.get !msg-digest)
           (.digest eid-bytes)
           (Arrays/copyOfRange 0 16))))))

(def ^org.apache.arrow.vector.types.pojo.Schema trie-schema
  (Schema. [(types/->field "nodes" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "nil" :null)
                           (types/col-type->field "branch" [:list [:union #{:null :i32}]])
                           (types/col-type->field "leaf" [:struct {'page-idx :i32
                                                                   'columns meta/metadata-col-type}]))]))

(def put-field
  (types/col-type->field "put" [:struct {'xt$valid_from types/temporal-col-type
                                         'xt$valid_to types/temporal-col-type
                                         'xt$doc [:union #{:null [:struct {}]}]}]))

(def delete-field
  (types/col-type->field "delete" [:struct {'xt$valid_from types/temporal-col-type
                                            'xt$valid_to types/temporal-col-type}]))

(def evict-field
  (types/col-type->field "evict" :null))

(def ^org.apache.arrow.vector.types.pojo.Schema log-leaf-schema
  (Schema. [(types/col-type->field "xt$iid" [:fixed-size-binary 16])
            (types/col-type->field "xt$system_from" types/temporal-col-type)
            (types/->field "op" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           put-field delete-field evict-field)]))

(defn open-leaf-root ^xtdb.vector.IRelationWriter [^BufferAllocator allocator]
  (util/with-close-on-catch [root (VectorSchemaRoot/create log-leaf-schema allocator)]
    (vw/root->writer root)))

(defn live-trie->bufs [^BufferAllocator allocator, ^LiveHashTrie trie, ^RelationReader leaf-rel]
  (when (pos? (.rowCount leaf-rel))
    (util/with-open [leaf-vsr (VectorSchemaRoot/create (Schema. (for [^IVectorReader rdr leaf-rel]
                                                                  (.getField rdr)))
                                                       allocator)
                     trie-vsr (VectorSchemaRoot/create trie-schema allocator)]
      (let [leaf-rel-wtr (vw/root->writer leaf-vsr)
            trie-rel-wtr (vw/root->writer trie-vsr)

            node-wtr (.writerForName trie-rel-wtr "nodes")
            node-wp (.writerPosition node-wtr)

            branch-wtr (.writerForTypeId node-wtr (byte 1))
            branch-el-wtr (.listElementWriter branch-wtr)

            leaf-wtr (.writerForTypeId node-wtr (byte 2))
            page-idx-wtr (.structKeyWriter leaf-wtr "page-idx")
            page-meta-wtr (meta/->page-meta-wtr (.structKeyWriter leaf-wtr "columns"))
            !page-idx (AtomicInteger. 0)
            copier (vw/->rel-copier leaf-rel-wtr leaf-rel)

            leaf-buf (util/build-arrow-ipc-byte-buffer leaf-vsr :file
                       (fn [write-batch!]
                         (letfn [(write-node! [^HashTrie$Node node]
                                   (if-let [children (.children node)]
                                     (let [!page-idxs (IntStream/builder)]
                                       (doseq [child children]
                                         (.add !page-idxs (if child
                                                            (do
                                                              (write-node! child)
                                                              (dec (.getPosition node-wp)))
                                                            -1)))
                                       (.startList branch-wtr)
                                       (.forEach (.build !page-idxs)
                                                 (reify IntConsumer
                                                   (accept [_ idx]
                                                     (if (= idx -1)
                                                       (.writeNull branch-el-wtr nil)
                                                       (.writeInt branch-el-wtr idx)))))
                                       (.endList branch-wtr)
                                       (.endRow trie-rel-wtr))

                                     (let [^LiveHashTrie$Leaf leaf node]
                                       (-> (Arrays/stream (.data leaf))
                                           (.forEach (reify IntConsumer
                                                       (accept [_ idx]
                                                         (.copyRow copier idx)))))

                                       (.syncRowCount leaf-rel-wtr)

                                       (let [leaf-rdr (vw/rel-wtr->rdr leaf-rel-wtr)
                                             put-rdr (-> leaf-rdr
                                                         (.readerForName "op")
                                                         (.legReader :put)
                                                         (.metadataReader))

                                             doc-rdr (.structKeyReader put-rdr "xt$doc")]

                                         (.writeMetadata page-meta-wtr (into [(.readerForName leaf-rdr "xt$system_from")
                                                                              (.readerForName leaf-rdr "xt$iid")
                                                                              (.structKeyReader put-rdr "xt$valid_from")
                                                                              (.structKeyReader put-rdr "xt$valid_to")]
                                                                             (map #(.structKeyReader doc-rdr %))
                                                                             (.structKeys doc-rdr))))
                                       (write-batch!)
                                       (.clear leaf-rel-wtr)
                                       (.clear leaf-vsr)

                                       (.startStruct leaf-wtr)
                                       (.writeInt page-idx-wtr (.getAndIncrement !page-idx))
                                       (.endStruct leaf-wtr)
                                       (.endRow trie-rel-wtr))))]

                           (write-node! (.rootNode trie)))))]

        (.syncSchema trie-vsr)
        (.syncRowCount trie-rel-wtr)

        {:leaf-buf leaf-buf
         :trie-buf (util/root->arrow-ipc-byte-buffer trie-vsr :file)}))))

(defn write-trie-bufs! [^ObjectStore obj-store, ^String dir, ^String chunk-idx
                        {:keys [^ByteBuffer leaf-buf ^ByteBuffer trie-buf]}]
  (-> (.putObject obj-store (format "%s/leaf-c%s.arrow" dir chunk-idx) leaf-buf)
      (util/then-compose
        (fn [_]
          (.putObject obj-store (format "%s/trie-c%s.arrow" dir chunk-idx) trie-buf)))))

(defn- bucket-for [^ByteBuffer iid level]
  (let [level-offset-bits (* HashTrie/LEVEL_BITS (inc level))
        level-offset-bytes (/ (- level-offset-bits HashTrie/LEVEL_BITS) Byte/SIZE)]
    (bit-and (bit-shift-right (.get iid ^int level-offset-bytes) (mod level-offset-bits Byte/SIZE)) HashTrie/LEVEL_MASK)))

(defn ->merge-plan
  "Returns a tree of the tasks required to merge the given tries"
  [tries, ^ByteBuffer iid]

  (letfn [(->merge-plan* [nodes path ^long level]
            (let [trie-children (mapv #(some-> ^HashTrie$Node % (.children)) nodes)]
              (if-let [^objects first-children (some identity trie-children)]
                {:path (byte-array path)
                 :node [:branch (->> (if iid
                                       [(bucket-for iid level)]
                                       (range (alength first-children)))
                                     (mapv (fn [bucket-idx]
                                             (->merge-plan* (mapv (fn [node ^objects node-children]
                                                                          (if node-children
                                                                            (aget node-children bucket-idx)
                                                                            node))
                                                                        nodes trie-children)
                                                                  (conj path bucket-idx)
                                                                  (inc level)))))]}
                {:path (byte-array path)
                 :node [:leaf (->> nodes
                                   (into [] (keep-indexed
                                             (fn [ordinal ^HashTrie$Node node]
                                               (when node
                                                 {:ordinal ordinal, :leaf node})))))]})))]

    (->merge-plan* (map #(some-> ^HashTrie % (.rootNode)) tries) [] 0)))
