(ns core2.operator.table
  (:require [core2.error :as err]
            [core2.relation :as rel]
            [core2.types :as ty]
            [core2.util :as util])
  (:import core2.ICursor
           [java.util ArrayList LinkedList List]
           org.apache.arrow.memory.BufferAllocator))

(set! *unchecked-math* :warn-on-boxed)

(deftype TableCursor [^BufferAllocator allocator
                      ^List rows
                      ^:unsynchronized-mutable done?]
  ICursor
  (tryAdvance [this c]
    (if (or done? (.isEmpty rows))
      false
      (do
        (set! (.done? this) true)

        (let [out-cols (LinkedList.)]
          (try
            (doseq [k (keys (first rows))]
              (let [out-vec (.createVector (ty/->primitive-dense-union-field (name k)) allocator)]
                (.add out-cols (rel/<-vector out-vec))
                (dorun
                 (map-indexed (fn [idx row]
                                (ty/set-safe! out-vec idx (get row k)))
                              rows))))
            (catch Exception _
              (run! util/try-close out-cols)))

          (with-open [out-rel (rel/->read-relation out-cols)]
            (.accept c out-rel)
            true)))))

  (close [_]))

(defn ->table-cursor ^core2.ICursor [^BufferAllocator allocator, ^List rows]
  (when-not (or (empty? rows) (= 1 (count (distinct (map keys rows)))))
    (throw (err/illegal-arg :mismatched-keys-in-table
                            {::err/message "Mismatched keys in table"
                             :key-sets (into #{} (map keys) rows)})))

  (TableCursor. allocator (ArrayList. rows) false))
