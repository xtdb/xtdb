(ns xtdb.coalesce
  (:require [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import org.apache.arrow.memory.BufferAllocator
           (xtdb.arrow Relation RelationReader)
           xtdb.ICursor))

;; We pass the first 100 results through immediately, so that any limit-like queries don't need to wait for a full page to return rows.
;; Then, we coalesce small pages together into pages of at least 100, to share the per-page costs.

(deftype CoalescingCursor [^ICursor cursor
                           ^BufferAllocator allocator
                           ^int pass-through
                           ^int ideal-min-page-size
                           ^:unsynchronized-mutable ^int seen-rows]
  ICursor
  (getCursorType [_] (.getCursorType cursor))
  (getChildCursors [_] (.getChildCursors cursor))

  (tryAdvance [this c]
    (let [!out-rel (volatile! nil)
          !rows-appended (volatile! 0)]
      (try
        (loop []
          (let [!passed-on? (volatile! false)
                advanced? (.tryAdvance cursor
                                       (fn [read-rels]
                                         (doseq [^RelationReader read-rel read-rels]
                                           (let [row-count (.getRowCount read-rel)
                                                 seen-rows (.seen-rows this)]
                                             (cond
                                               ;; haven't seen many rows yet, send this one straight through
                                               (< seen-rows pass-through)
                                               (do
                                                 (set! (.seen-rows this) (+ seen-rows row-count))
                                                 (.accept c [read-rel])
                                                 (vreset! !passed-on? true))

                                               ;; this page is big enough, and we don't have rows waiting
                                               ;; send it straight through, no copy.
                                               (and (>= row-count ideal-min-page-size)
                                                    (nil? @!out-rel))
                                               (do
                                                 (.accept c [read-rel])
                                                 (vreset! !passed-on? true))

                                               ;; otherwise, add it to the pending rows.
                                               :else
                                               (let [out-rel (vswap! !out-rel #(or % (Relation. allocator)))]
                                                 (vw/append-rel out-rel read-rel)
                                                 (vswap! !rows-appended + row-count)))))))
                rows-appended @!rows-appended]

            (cond
              ;; we've already passed on a page, return straight out
              (true? @!passed-on?) true

              ;; not enough rows yet, but more in the source - go around again
              (and advanced? (< rows-appended ideal-min-page-size)) (recur)

              ;; we've got rows, and either the source is done or there's enough already - send them through
              (pos? rows-appended)
              (do
                (.accept c [@!out-rel])
                true)

              ;; no more rows in input, and none to pass through, we're done
              :else false)))

        (finally
          (util/try-close @!out-rel)))))

  (close [_]
    (.close cursor)))

(defn ->coalescing-cursor
  (^xtdb.ICursor [cursor allocator] (->coalescing-cursor cursor allocator {}))

  (^xtdb.ICursor [cursor allocator {:keys [pass-through ideal-min-page-size]
                                    :or {pass-through 100, ideal-min-page-size 100}}]
   (CoalescingCursor. cursor allocator pass-through ideal-min-page-size 0)))
