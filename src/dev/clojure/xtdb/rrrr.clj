(ns xtdb.rrrr
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cognitect.transit :as t]
            xtdb.log
            [xtdb.metadata :as meta]
            [xtdb.transit :as xt.t]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.indirect :as iv]
            [xtdb.vector.writer :as vw])
  (:import (java.io File OutputStream)
           (java.nio ByteBuffer ByteOrder)
           (java.util ArrayList HashMap)
           (java.util Arrays HashMap)
           (java.util.concurrent.atomic AtomicInteger)
           java.util.function.Function
           org.apache.arrow.memory.RootAllocator
           (org.apache.arrow.vector VectorSchemaRoot)
           org.apache.arrow.vector.ipc.ArrowFileWriter
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           org.apache.arrow.vector.types.pojo.Field
           org.apache.arrow.vector.types.UnionMode
           xtdb.vector.IVectorWriter))

(def page-size 8192)

(defn- input-files [^File root-dir]
   (->> (for [^File file (.listFiles (io/file root-dir "input"))
              :let [[match row-id-str tx-id-str] (re-matches #"seg-r([\p{XDigit}]+)-tx([\p{XDigit}]+)\.transit\.json" (.getName file))]
              :when match]
          {:file file,
           :seg-row-id (util/<-lex-hex-string row-id-str)
           :seg-tx-id (util/<-lex-hex-string tx-id-str)})
        (sort-by :seg-row-id)))

(defn- lazy-transit-seq [is]
  (letfn [(lts* [rdr]
            (lazy-seq
             (try
               (cons (t/read rdr) (lts* rdr))
               (catch Throwable _ nil))))]
    (lts* (t/reader is :json {:handlers xt.t/tj-read-handlers}))))

(defn- rows->col-type [rows]
  (->> (into #{} (map (comp vw/value->col-type :doc)) rows)
       (apply types/merge-col-types)))

(defn- iid-prefix ^long [^bytes iid, ^long lvl]
  (-> (.getLong (doto (ByteBuffer/wrap iid)
                  (.order ByteOrder/BIG_ENDIAN)))
      (bit-shift-right (- 64 (* 4 (inc lvl))))
      (bit-and 0xf)))

(def tpch-col-types
  (->> (input-files (io/file "/home/james/tmp/idx-poc/tpch-01"))
       (transduce
        (comp (mapcat (fn [{:keys [^File file]}]
                        (with-open [is (io/input-stream file)]
                          (->> (lazy-transit-seq is)
                               (into [] (comp (mapcat (fn [{:keys [rows]}]
                                                        (for [{:keys [table doc]} rows
                                                              [col-sym col-type] (second (vw/value->col-type doc))]
                                                          [table col-sym col-type])))
                                              (distinct)))))))
              (distinct))
        (completing
         (fn
           ([] {})
           ([acc [table col-sym col-type]]
            (update-in acc [table (keyword col-sym)] types/merge-col-types col-type)))))))

(def ^:private nullable-inst-type [:union #{:null [:timestamp-tz :micro "UTC"]}])

(def temporal-log-schema
  (Schema. [(types/col-type->field "tx-id" :i64)
            (types/col-type->field "system-time" types/temporal-col-type)
            (types/->field "tx-ops" types/list-type false
                           (types/->field "$data" (ArrowType$Union. UnionMode/Dense (int-array (range 2))) false
                                          (types/col-type->field "put" [:struct {'table :utf8
                                                                                 'iid [:fixed-size-binary 16]
                                                                                 'row-id :i64
                                                                                 'valid-from nullable-inst-type
                                                                                 'valid-to nullable-inst-type}])
                                          (types/col-type->field "delete" [:struct {'iid [:fixed-size-binary 16]
                                                                                    'valid-from nullable-inst-type
                                                                                    'valid-to nullable-inst-type}])))]))

#_{:clj-kondo/ignore [:unused-private-var :inline-def]}
(defn- write-log-files [root-dir]
  (with-open [al (RootAllocator.)]
    (->> (doseq [{:keys [^File file]} (input-files root-dir)]
           (with-open [is (io/input-stream file)]
             (let [out-file-name (-> (.getName file) (str/replace #"\.transit\.json$" ".arrow"))
                   txs (lazy-transit-seq is)
                   seg-tx-count (count txs)
                   seg-row-count (count (mapcat :rows txs))]

                 ;;; --- content log
               (let [table-doc-legs (for [[table rows] (->> (mapcat :rows txs)
                                                            (group-by :table))]
                                      (types/col-type->field (name table) (rows->col-type rows)))

                     table-type-ids (->> table-doc-legs
                                         (into {} (map-indexed (fn [type-id, ^Field field]
                                                                 [(.getName field) type-id]))))

                     content-log-schema (Schema. [(apply types/->field "doc"
                                                         (ArrowType$Union. UnionMode/Dense (int-array (range (count table-doc-legs))))
                                                         false
                                                         table-doc-legs)])]
                 (with-open [content-log-root (VectorSchemaRoot/create content-log-schema al)
                             log-file-writer (-> (io/file root-dir "content-log" out-file-name)
                                                 (doto (io/make-parents))
                                                 (util/open-arrow-file-writer content-log-root))]
                   (.start log-file-writer)
                   (let [doc-wtr (vw/->writer (.getVector content-log-root "doc"))]
                     (doseq [{:keys [rows]} txs
                             {:keys [table doc]} rows]
                       (vw/write-value! doc (.writerForTypeId doc-wtr (byte (get table-type-ids (name table))))))

                     (.setRowCount content-log-root seg-row-count)

                     (doto log-file-writer .writeBatch .end)

                     #_
                     (println (.contentToTSVString content-log-root)))))

                 ;;; --- temporal log
               (with-open [temporal-log-root (VectorSchemaRoot/create temporal-log-schema al)
                           temporal-log-file-writer (-> (io/file root-dir "temporal-log" out-file-name)
                                                        (doto (io/make-parents))
                                                        (util/open-arrow-file-writer temporal-log-root))]
                 (.start temporal-log-file-writer)

                 (let [tx-id-wtr (vw/->writer (.getVector temporal-log-root "tx-id"))
                       tx-time-wtr (vw/->writer (.getVector temporal-log-root "system-time"))
                       tx-ops-wtr (vw/->writer (.getVector temporal-log-root "tx-ops"))
                       tx-ops-el-wtr (.listElementWriter tx-ops-wtr)

                       put-wtr (.writerForTypeId tx-ops-el-wtr (byte 0))
                       table-wtr (.structKeyWriter put-wtr "table")
                       iid-wtr (.structKeyWriter put-wtr "iid")
                       row-id-wtr (.structKeyWriter put-wtr "row-id")
                       vf-wtr (.structKeyWriter put-wtr "valid-from")
                       vt-wtr (.structKeyWriter put-wtr "valid-to")]

                   (doseq [{:keys [tx-id system-time rows]} txs]
                     (let [tx-time-µs (util/instant->micros system-time)]
                       (.writeLong tx-id-wtr tx-id)
                       (.writeLong tx-time-wtr tx-time-µs)

                       (.startList tx-ops-wtr)
                       (doseq [{:keys [table row-id iid]} rows]
                         (.startStruct put-wtr)
                         (.writeObject table-wtr (name table))
                         (.writeBytes iid-wtr (ByteBuffer/wrap iid))
                         (.writeLong row-id-wtr row-id)
                         (.writeLong vf-wtr tx-time-µs)
                         (.writeNull vt-wtr nil)
                         (.endStruct put-wtr))
                       (.endList tx-ops-wtr)))

                   (.setRowCount temporal-log-root seg-tx-count)

                   (doto temporal-log-file-writer .writeBatch .end)

                   #_
                   (println (.contentToTSVString temporal-log-root))))))))))

(comment
  (write-log-files (io/file "/home/james/tmp/idx-poc/tpch-1")))

(defn- write-temporal-diff [root-dir]
  (with-open [al (RootAllocator.)]
    (let [tables-dir (io/file root-dir "tables")]
      (doseq [{:keys [^File file, seg-row-id seg-tx-id]} (input-files root-dir)
              :let [file-name-prefix (format "r%s-tx%s"
                                             (util/->lex-hex-string seg-row-id)
                                             (util/->lex-hex-string seg-tx-id))]]
        (log/info "T1 diff for" (.getName file))

        (with-open [is (io/input-stream file)]
          (doseq [[table rows] (->> (lazy-transit-seq is)
                                    (mapcat (fn [{:keys [system-time rows]}]
                                              (for [row rows]
                                                (-> row (assoc :system-time system-time)))))
                                    (group-by :table))
                  :let [out-dir (doto (io/file tables-dir (name table) "t1-diff")
                                  .mkdirs)

                        leaf-schema (Schema. [(types/col-type->field "xt$iid" [:fixed-size-binary 16])
                                              (types/col-type->field "op"
                                                                     [:union #{:null
                                                                               [:struct {'xt$row_id :i64
                                                                                         'xt$valid_from types/temporal-col-type
                                                                                         'xt$system_from types/temporal-col-type
                                                                                         'xt$doc (rows->col-type rows)}]}])])

                        trie-schema (Schema. [(types/->field "nodes" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                                                             (types/col-type->field "nil" :null)
                                                             (types/col-type->field "branch" [:list [:union #{:null :i64}]])
                                                             (types/col-type->field "leaf" '[:struct
                                                                                             {page-idx :i64
                                                                                              columns [:list
                                                                                                       [:struct {col-name :utf8
                                                                                                                 count :i64
                                                                                                                 root-col? :bool
                                                                                                                 types [:struct {}]
                                                                                                                 bloom :varbinary}]]}]))])]]

            (with-open [trie-vsr (VectorSchemaRoot/create trie-schema al)
                        trie-file-wtr (-> (io/file out-dir (str file-name-prefix "-trie.arrow"))
                                          (util/open-arrow-file-writer trie-vsr))
                        leaf-vsr (VectorSchemaRoot/create leaf-schema al)
                        leaf-file-wtr (-> (io/file out-dir (str file-name-prefix "-leaf.arrow"))
                                          (util/open-arrow-file-writer leaf-vsr)) ]
              (.start trie-file-wtr)
              (.start leaf-file-wtr)

              (let [node-wtr (vw/->writer (.getVector trie-vsr "nodes"))
                    node-wp (.writerPosition node-wtr)

                    nil-wtr (.writerForTypeId node-wtr (byte 0))
                    branch-wtr (.writerForTypeId node-wtr (byte 1))
                    branch-el-wtr (.listElementWriter branch-wtr)

                    leaf-wtr (.writerForTypeId node-wtr (byte 2))
                    page-idx-wtr (.structKeyWriter leaf-wtr "page-idx")
                    !page-idx (AtomicInteger. 0)
                    cols-wtr (.structKeyWriter leaf-wtr "columns")
                    cols-meta-wtr (meta/->cols-meta-wtr (.listElementWriter cols-wtr))

                    iid-wtr (vw/->writer (.getVector leaf-vsr "xt$iid"))
                    iid-wp (.writerPosition iid-wtr)
                    op-wtr (vw/->writer (.getVector leaf-vsr "op"))
                    row-id-wtr (.structKeyWriter op-wtr "xt$row_id")
                    vf-wtr (.structKeyWriter op-wtr "xt$valid_from")
                    sf-wtr (.structKeyWriter op-wtr "xt$system_from")
                    doc-wtr (.structKeyWriter op-wtr "xt$doc")]
                (letfn [(write-page! [rows]
                          (let [page-idx (.getAndIncrement !page-idx)]
                            (.startStruct leaf-wtr)
                            (.writeLong page-idx-wtr page-idx)

                            (doseq [{:keys [iid row-id system-time doc]} (->> rows
                                                                              (sort-by :iid #(Arrays/compare ^bytes %1, ^bytes %2)))
                                    :let [sys-from-µs (util/instant->micros system-time)]]
                              (.writeBytes iid-wtr (ByteBuffer/wrap iid))

                              (.startStruct op-wtr)
                              (.writeLong row-id-wtr row-id)
                              (.writeLong vf-wtr sys-from-µs)
                              (.writeLong sf-wtr sys-from-µs)
                              (vw/write-value! doc doc-wtr)
                              (.endStruct op-wtr))

                            (.setRowCount leaf-vsr (.getPosition iid-wp))
                            #_(println (.contentToTSVString leaf-vsr))
                            (.writeBatch leaf-file-wtr)

                            (.startList cols-wtr)
                            (doseq [wtr [iid-wtr vf-wtr sf-wtr]]
                              (.writeMetadata cols-meta-wtr (vw/vec-wtr->rdr wtr)))
                            (.endList cols-wtr)

                            (.clear leaf-vsr)
                            (.clear iid-wtr)
                            (.clear op-wtr)

                            (.endStruct leaf-wtr)))

                        (write-branch-node! [groups lvl]
                          (let [slots (->> groups
                                           (into [] (map (fn [grp]
                                                           (if grp
                                                             (do
                                                               (trie-lvl grp (inc lvl))
                                                               (dec (.getPosition node-wp)))
                                                             -1)))))]
                            (.startList branch-wtr)
                            (doseq [idx slots]
                              (if (= -1 idx)
                                (.writeNull branch-el-wtr nil)
                                (.writeLong branch-el-wtr idx)))
                            (.endList branch-wtr)))

                        (trie-lvl [rows lvl]
                          (cond
                            (empty? rows) (.writeNull nil-wtr nil)

                            (<= (count rows) page-size) (write-page! rows)

                            :else (let [grouped-rows (->> rows
                                                          (group-by (comp #(iid-prefix % lvl) :iid)))]
                                    (write-branch-node! (map grouped-rows (range 0x10)) lvl))))]

                  (trie-lvl rows 0))

                (.end leaf-file-wtr)

                (.setRowCount trie-vsr (.getPosition node-wp))
                #_
                (println (.contentToTSVString trie-vsr))
                (.writeBatch trie-file-wtr)
                (.end trie-file-wtr)))))))))

(comment
  (write-temporal-diff (io/file "/home/james/tmp/idx-poc/tpch-01")))

(defn- write-t1-partitions [root-dir]
  (let [wtrs (HashMap.)]
    (try
      (doseq [{:keys [^File file]} (input-files root-dir)]
        (log/info "writing parts for file" (.getName file))
        (util/with-open [is (io/input-stream file)]
          (doseq [{:keys [table iid] :as row} (->> (lazy-transit-seq is)
                                                   (mapcat (fn [{:keys [system-time rows]}]
                                                             (for [row rows]
                                                               (-> row (assoc :system-time system-time))))))]
            (when (Thread/interrupted)
              (throw (InterruptedException.)))

            (let [{:keys [tw, ^OutputStream os]} (.computeIfAbsent wtrs [table (iid-prefix iid 0)]
                                                                   (reify Function
                                                                     (apply [_ [table prefix]]
                                                                       (util/with-close-on-catch [os (io/output-stream (doto (io/file root-dir "tables" (name table) "t1-parts" (format "%02x.transit.json" prefix))
                                                                                                                         (io/make-parents)))]
                                                                         {:os os, :tw (t/writer os :json {:handlers xt.t/tj-write-handlers})}))))]
              (t/write tw row)
              (.write os #=(int \newline))))))
      (finally
        (util/close (map :os (.values wtrs)))))))

(comment
  (write-t1-partitions (io/file "/home/james/tmp/idx-poc/tpch-1")))

(def hamt-directory-schema
  ;; metadata probably a `:map` but we don't have those yet
  (Schema. [(types/->field "node-trie" types/list-type false
                           (types/->field "nodes" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                                          (types/col-type->field "nil" :null)
                                          (types/col-type->field "branch" [:list [:union #{:null :i64}]])
                                          (types/col-type->field "leaf" '[:struct
                                                                          {page-idx :i64
                                                                           columns [:list
                                                                                    [:struct {col-name :utf8
                                                                                              count :i64
                                                                                              root-col? :bool
                                                                                              types [:struct {}]
                                                                                              bloom :varbinary}]]}])))

            (types/col-type->field "metadata" '[:list
                                                [:struct
                                                 {column :utf8
                                                  count :i64
                                                  root-column :bool
                                                  types [:struct {}]
                                                  bloom :varbinary}]])]))

(def t1-hamt-temporal-page-schema
  (Schema. [(types/col-type->field "xt$iid" [:fixed-size-binary 16])
            (types/col-type->field "xt$valid_from" types/temporal-col-type)
            (types/col-type->field "xt$system_from" types/temporal-col-type)
            (types/col-type->field "xt$row_id" :i64)]))

(defn- write-t1-hamt [^File root-dir]
  (util/with-open [al (RootAllocator.)]
    (let [tables-dir (io/file root-dir "tables")
          {last-seg-row-id :seg-row-id, last-seg-tx-id :seg-tx-id} (last (input-files root-dir))
          out-file-name (format "r%s-tx%s.arrow"
                                (util/->lex-hex-string last-seg-row-id)
                                (util/->lex-hex-string last-seg-tx-id))]
      (doseq [^File table-dir (.listFiles (io/file root-dir "tables"))
              :let [table (keyword (.getName table-dir))
                    col-types (get tpch-col-types table)]]
        (log/infof "T1 HAMT for '%s'" (name table))
        (with-open [directory-root (VectorSchemaRoot/create hamt-directory-schema al)
                    directory-file-wtr (-> (io/file tables-dir (name table) "t1-directory" out-file-name)
                                           (doto (io/make-parents))
                                           (util/open-arrow-file-writer directory-root))
                    temporal-root (VectorSchemaRoot/create t1-hamt-temporal-page-schema al)
                    temporal-root-file-wtr (-> (io/file tables-dir (name table) "t1-temporal" out-file-name)
                                               (doto (io/make-parents))
                                               (util/open-arrow-file-writer temporal-root))]
          (.start temporal-root-file-wtr)

          (let [content-wtrs (ArrayList.)]
            (try
              (doseq [[col-name col-type] col-types
                      :let [normalised-col-name (util/str->normal-form-str (str (symbol col-name)))
                            content-file (io/file tables-dir (name table) "t1-content" normalised-col-name out-file-name)]]
                (util/with-close-on-catch [root (VectorSchemaRoot/create (Schema. [(types/col-type->field normalised-col-name col-type)]) al)
                                           content-file-wtr (-> content-file
                                                                (doto (io/make-parents))
                                                                (util/open-arrow-file-writer root))]
                  (.start content-file-wtr)
                  (.add content-wtrs {:col-name (keyword col-name)
                                      :normalised-col-name normalised-col-name
                                      :root root
                                      :vec-wtr (vw/->writer (.getVector root normalised-col-name))
                                      :content-file-wtr content-file-wtr})))

              (let [node-trie-wtr (vw/->writer (.getVector directory-root "node-trie"))
                    node-wtr (.listElementWriter node-trie-wtr)
                    node-wp (.writerPosition node-wtr)
                    nil-wtr (.writerForTypeId node-wtr (byte 0))
                    branch-wtr (.writerForTypeId node-wtr (byte 1))
                    branch-el-wtr (.listElementWriter branch-wtr)
                    leaf-wtr (.writerForTypeId node-wtr (byte 2))
                    page-idx-wtr (.structKeyWriter leaf-wtr "page-idx")
                    cols-wtr (.structKeyWriter leaf-wtr "columns")
                    cols-meta-wtr (meta/->cols-meta-wtr (.listElementWriter cols-wtr))

                    iid-wtr (vw/->writer (.getVector temporal-root "xt$iid"))
                    vf-wtr (vw/->writer (.getVector temporal-root "xt$valid_from"))
                    sf-wtr (vw/->writer (.getVector temporal-root "xt$system_from"))
                    row-id-wtr (vw/->writer (.getVector temporal-root "xt$row_id"))
                    !page-idx (AtomicInteger.)]
                (.startList node-trie-wtr)
                (letfn [(write-page! [rows]
                          (let [rows (->> rows
                                          (sort-by :iid #(Arrays/compare ^bytes %1, ^bytes %2)))
                                row-count (count rows)
                                page-idx (.getAndIncrement !page-idx)]
                            (.startStruct leaf-wtr)
                            (.writeLong page-idx-wtr page-idx)
                            (.startList cols-wtr)

                            (doseq [{:keys [iid row-id system-time doc]} rows
                                    :let [sys-time-µs (util/instant->micros system-time)]]
                              (.writeBytes iid-wtr (ByteBuffer/wrap iid))
                              (.writeLong row-id-wtr row-id)
                              (.writeLong vf-wtr sys-time-µs)
                              (.writeLong sf-wtr sys-time-µs)

                              (doseq [{:keys [col-name vec-wtr]} content-wtrs]
                                ;; HACK doesn't handle absent - not a problem in TPC-H
                                (vw/write-value! (get doc col-name) vec-wtr)))

                            (.setRowCount temporal-root row-count)
                            (.writeBatch temporal-root-file-wtr)
                            #_
                            (println (.contentToTSVString temporal-root))

                            (doseq [^String col-name ["xt$iid" "xt$valid_from" "xt$system_from"]]
                              (.writeMetadata cols-meta-wtr (iv/->direct-vec (.getVector temporal-root col-name))))

                            (.clear temporal-root)
                            (.clear iid-wtr)
                            (.clear row-id-wtr)
                            (.clear vf-wtr)
                            (.clear sf-wtr)

                            (doseq [{:keys [^String normalised-col-name, ^VectorSchemaRoot root, ^ArrowFileWriter content-file-wtr, ^IVectorWriter vec-wtr]} content-wtrs]
                              (.setRowCount root row-count)
                              (.writeBatch content-file-wtr)
                              (.writeMetadata cols-meta-wtr (iv/->direct-vec (.getVector root normalised-col-name)))
                              (.clear root)
                              (.clear vec-wtr))

                            (.endList cols-wtr)
                            (.endStruct leaf-wtr)))

                        (write-branch-node! [groups lvl]
                          (let [slots (->> groups
                                           (into [] (map (fn [grp]
                                                           (if grp
                                                             (do
                                                               (trie-lvl grp (inc lvl))
                                                               (dec (.getPosition node-wp)))
                                                             -1)))))]
                            (.startList branch-wtr)
                            (doseq [idx slots]
                              (if (= -1 idx)
                                (.writeNull branch-el-wtr nil)
                                (.writeLong branch-el-wtr idx)))
                            (.endList branch-wtr)))

                        (trie-lvl [rows lvl]
                          (cond
                            (empty? rows) (.writeNull nil-wtr nil)

                            (<= (count rows) page-size) (write-page! rows)

                            :else (let [grouped-rows (->> rows
                                                          (group-by (comp #(iid-prefix % lvl) :iid)))]
                                    (write-branch-node! (map grouped-rows (range 0x10)) lvl))))

                        (part-file->rows [^File part-file]
                          (log/infof "T1 HAMT for '%s', part '%s'" (name table) (subs (.getName part-file) 1 2))
                          (with-open [is (io/input-stream part-file)]
                            (vec (lazy-transit-seq is))))]

                  (let [part-files (->> (.listFiles (io/file table-dir "t1-parts"))
                                        (sort-by #(.getName ^File %)))]
                    (if (contains? (-> (case (.getName root-dir)
                                         "tpch-01" #{:customer, :part, :supplier}
                                         "tpch-05" #{:customer, :supplier}
                                         #{})
                                       (conj :nation :region))
                                   table)
                      (trie-lvl (->> part-files
                                     (into [] (mapcat part-file->rows)))
                                0)

                      (write-branch-node! (->> part-files
                                               (map part-file->rows))
                                          0)))

                  (.endList node-trie-wtr)

                  (.end temporal-root-file-wtr)

                  (doseq [{:keys [^ArrowFileWriter content-file-wtr]} content-wtrs]
                    (.end content-file-wtr))

                  (.setRowCount directory-root 1)
                  (.syncSchema directory-root)

                  (.start directory-file-wtr)
                  (.writeBatch directory-file-wtr)
                  (.end directory-file-wtr)

                  #_
                  (println (.contentToTSVString directory-root))))

              (finally
                (util/close (mapcat (juxt :content-file-wtr :root) content-wtrs))))))))))

(comment
  (write-t1-hamt (io/file "/home/james/tmp/idx-poc/tpch-1")))
