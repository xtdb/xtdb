(ns core2.operator.table
  (:require [core2.error :as err]
            [core2.vector :as vec]
            [core2.util :as util])
  (:import core2.ICursor
           [java.util ArrayList List]
           org.apache.arrow.memory.BufferAllocator))

(set! *unchecked-math* :warn-on-boxed)

(deftype TableCursor [^BufferAllocator allocator
                      ^List rows
                      ^:unsynchronized-mutable done?]
  ICursor
  (tryAdvance [this c]
    (if (or done? (.isEmpty rows))
      false
      (do (set! (.done? this) true)
          (let [out-rel (vec/->fresh-append-relation allocator)]
            (try
              (doseq [k (keys (first rows))
                      :let [out-col (.appendColumn out-rel (name k))]
                      v (map k rows)]
                (.appendObject out-col v))
              (.accept c (.read out-rel))
              (finally
                (util/try-close out-rel))))
          true)))

  (close [_]))

(defn ->table-cursor ^core2.ICursor [^BufferAllocator allocator, ^List rows]
  (when-not (or (empty? rows) (= 1 (count (distinct (map keys rows)))))
    (throw (err/illegal-arg :mismatched-keys-in-table
                            {::err/message "Mismatched keys in table"
                             :key-sets (into #{} (map keys) rows)})))

  (TableCursor. allocator (ArrayList. rows) false))
