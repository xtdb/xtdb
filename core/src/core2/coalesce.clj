(ns core2.coalesce
  (:require [core2.relation :as rel]
            [core2.util :as util])
  (:import core2.ICursor
           [core2.relation IAppendRelation IReadRelation]
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.util.AutoCloseables))

;; We pass the first 100 results through immediately, so that any limit-like queries don't need to wait for a full block to return rows.
;; Then, we coalesce small blocks together into blocks of at least 100, to share the per-block costs.

(deftype CoalescingCursor [^ICursor cursor
                           ^BufferAllocator allocator
                           ^int pass-through
                           ^int ideal-min-block-size
                           ^:unsynchronized-mutable ^int seen-rows]
  ICursor
  (tryAdvance [this c]
    (let [!append-rel (volatile! nil)
          !rows-appended (volatile! 0)]
      (try
        (loop []
          (let [!passed-on? (volatile! false)
                advanced? (.tryAdvance cursor (reify Consumer
                                                (accept [_ read-rel]
                                                  (let [^IReadRelation read-rel read-rel
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
                                                           (nil? @!append-rel))
                                                      (do
                                                        (.accept c read-rel)
                                                        (vreset! !passed-on? true))

                                                      ;; otherwise, add it to the pending rows.
                                                      :else
                                                      (let [^IAppendRelation append-rel (vswap! !append-rel #(or % (rel/->append-relation allocator)))]
                                                        (.appendRelation append-rel read-rel)
                                                        (vswap! !rows-appended + row-count)))))))
                rows-appended @!rows-appended]

            (cond
              ;; we've already passed on a block, return straight out
              (true? @!passed-on?) true

              ;; not enough rows yet, but more in the source - go around again
              (and advanced? (< rows-appended ideal-min-block-size)) (recur)

              ;; we've got rows, and either the source is done or there's enough already - send them through
              (pos? rows-appended) (do
                                     (.accept c (.read ^IAppendRelation @!append-rel))
                                     true)

              ;; no more rows in input, and none to pass through, we're done
              :else false)))

        (finally
          (util/try-close @!append-rel)))))

  (close [_]
    (.close cursor)))

(defn ^core2.ICursor ->coalescing-cursor
  ([cursor allocator] (->coalescing-cursor cursor allocator {}))

  ([cursor allocator {:keys [pass-through ideal-min-block-size]
                      :or {pass-through 100, ideal-min-block-size 100}}]
   (CoalescingCursor. cursor allocator pass-through ideal-min-block-size 0)))
