(ns core2.operator.table
  (:require [clojure.spec.alpha :as s]
            [core2.error :as err]
            [core2.expression :as expr]
            [core2.logical-plan :as lp]
            [core2.types :as ty]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import (core2 ICursor)
           (core2.vector IIndirectRelation)
           (java.util LinkedList)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.complex DenseUnionVector)))

(defmethod lp/ra-expr :table [_]
  (s/cat :op #{:table}
         :explicit-col-names (s/? (s/coll-of ::lp/column :kind set?))
         :table (s/or :rows (s/coll-of (s/map-of simple-ident? any?))
                      :source ::lp/source)))

(set! *unchecked-math* :warn-on-boxed)

(deftype TableCursor [^BufferAllocator allocator
                      ^long row-count
                      cols
                      ^:unsynchronized-mutable done?]
  ICursor
  (tryAdvance [this c]
    (if (or done? (nil? cols))
      false
      (do
        (set! (.done? this) true)

        (let [out-cols (LinkedList.)]
          (try
            (doseq [[k vs] cols]
              (let [out-vec (DenseUnionVector/empty (name k) allocator)
                    out-writer (.asDenseUnion (vw/vec->writer out-vec))]
                (.add out-cols (iv/->direct-vec out-vec))
                (dorun
                 (map-indexed (fn [idx v]
                                (util/set-value-count out-vec idx)

                                (.startValue out-writer)
                                (doto (.writerForType out-writer (ty/value->col-type v))
                                  (.startValue)
                                  (->> (ty/write-value! v))
                                  (.endValue))
                                (.endValue out-writer))

                              vs))))

            (catch Exception e
              (run! util/try-close out-cols)
              (throw e)))

          (with-open [^IIndirectRelation out-rel (iv/->indirect-rel out-cols row-count)]
            (.accept c out-rel)
            true)))))

  (close [_]))

(defn table->keys [rows]
  (letfn [(row-keys [row]
            (into #{} (map symbol) (keys row)))]
    (let [col-names (row-keys (first rows))]
      (when-not (every? #(= col-names (row-keys %)) rows)
        (throw (err/illegal-arg :mismatched-keys-in-table
                                {::err/message "Mismatched keys in table"
                                 :expected col-names
                                 :key-sets (into #{} (map row-keys) rows)})))
      col-names)))

(defmethod lp/emit-expr :table [{[table-type table-arg] :table, :keys [explicit-col-names]} {:keys [src-keys table-keys]}]
  (when (and (= table-type :source) (not (contains? src-keys table-arg)))
    (throw (err/illegal-arg :unknown-table
                            {::err/message "Query refers to unknown table"
                             :table table-arg
                             :src-keys src-keys})))
  (let [col-names (or explicit-col-names
                      (case table-type
                        :rows (table->keys table-arg)
                        :source (get table-keys table-arg)))]
    ;; TODO actually pass col-types through
    {:col-types (zipmap col-names (repeat :null))
     :->cursor (fn [{:keys [allocator srcs params]}]
                 (let [rows (case table-type
                              :rows table-arg
                              :source (get srcs table-arg))]
                   (TableCursor. allocator
                                 (count rows)
                                 (when (seq rows)
                                   (->> (for [col-name col-names
                                              :let [col-k (keyword col-name)
                                                    col-s (symbol col-name)]]
                                          [col-name (vec (for [row rows
                                                               :let [v (get row col-k (get row col-s))]]
                                                           (expr/eval-scalar-value allocator v params)))])
                                        (into {})))
                                 false)))}))
