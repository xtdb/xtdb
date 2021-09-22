(ns core2.coalesce
  (:require [core2.relation :as rel]
            [core2.util :as util])
  (:import core2.ICursor
           [core2.relation IRelationWriter IRelationReader]
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator))

;; We pass the first 100 results through immediately, so that any limit-like queries don't need to wait for a full block to return rows.
;; Then, we coalesce small blocks together into blocks of at least 100, to share the per-block costs.

(deftype CoalescingCursor [^ICursor cursor
                           ^BufferAllocator allocator
                           ^int pass-through
                           ^int ideal-min-block-size
                           ^:unsynchronized-mutable ^int seen-rows]
  ICursor
  (tryAdvance [this c]
    (let [!rel-writer (volatile! nil)
          !rows-appended (volatile! 0)]
      (try
        (loop []
          (let [!passed-on? (volatile! false)
                advanced? (.tryAdvance cursor (reify Consumer
                                                (accept [_ read-rel]
                                                  (let [^IRelationReader read-rel read-rel
                                                        row-count (.rowCount read-rel)
                                                        seen-rows (.seen-rows this)]
                                                    (cond
                                                      ;; haven't seen many rows yet, send this one straight through
                                                      (< seen-rows pass-through)
                                                      (do
                                                        (set! (.seen-rows this) (+ seen-rows row-count))
                                                        (.accept c read-rel)
                                                        (vreset! !passed-on? true))

                                                      ;; this block is big enough, and we don't have rows waiting
                                                      ;; send it straight through, no copy.
                                                      (and (>= row-count ideal-min-block-size)
                                                           (nil? @!rel-writer))
                                                      (do
                                                        (.accept c read-rel)
                                                        (vreset! !passed-on? true))

                                                      ;; otherwise, add it to the pending rows.
                                                      :else
                                                      (let [rel-writer (vswap! !rel-writer #(or % (rel/->rel-writer allocator)))]
                                                        (rel/append-rel rel-writer read-rel)
                                                        (vswap! !rows-appended + row-count)))))))
                rows-appended @!rows-appended]

            (cond
              ;; we've already passed on a block, return straight out
              (true? @!passed-on?) true

              ;; not enough rows yet, but more in the source - go around again
              (and advanced? (< rows-appended ideal-min-block-size)) (recur)

              ;; we've got rows, and either the source is done or there's enough already - send them through
              (pos? rows-appended) (do
                                     (.accept c (rel/rel-writer->reader @!rel-writer))
                                     true)

              ;; no more rows in input, and none to pass through, we're done
              :else false)))

        (finally
          (util/try-close @!rel-writer)))))

  (close [_]
    (.close cursor)))

(defn ^core2.ICursor ->coalescing-cursor
  ([cursor allocator] (->coalescing-cursor cursor allocator {}))

  ([cursor allocator {:keys [pass-through ideal-min-block-size]
                      :or {pass-through 100, ideal-min-block-size 100}}]
   (CoalescingCursor. cursor allocator pass-through ideal-min-block-size 0)))
