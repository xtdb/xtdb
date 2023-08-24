(ns xtdb.trie.leaf-merge-queue-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (org.apache.arrow.memory RootAllocator)
           (xtdb.trie LeafMergeQueue LeafMergeQueue$LeafPointer)
           xtdb.vector.IVectorReader))

(deftest test-leaf-merge-queue-missing-tries-2714
  (with-open [allocator (RootAllocator.)
              rel1 (vw/->rel-writer allocator)
              iid-wrt1 (.writerForName rel1 "xt$iid" [:fixed-size-binary 16])
              rel2 (vw/->rel-writer allocator)
              iid-wrt2 (.writerForName rel2 "xt$iid" [:fixed-size-binary 16])]
    (.writeBytes iid-wrt1 (util/uuid->byte-buffer #uuid "00000000-0000-0000-0000-000000000000"))
    (.writeBytes iid-wrt2 (util/uuid->byte-buffer #uuid "01000000-0000-0000-0000-000000000000"))
    (.writeBytes iid-wrt1 (util/uuid->byte-buffer #uuid "02000000-0000-0000-0000-000000000000"))
    (.writeBytes iid-wrt2 (util/uuid->byte-buffer #uuid "03000000-0000-0000-0000-000000000000"))
    (let [lmq (LeafMergeQueue. (byte-array 0)
                               (into-array IVectorReader
                                           [nil
                                            (vw/vec-wtr->rdr iid-wrt1)
                                            nil
                                            (vw/vec-wtr->rdr iid-wrt2)])
                               [nil
                                (LeafMergeQueue$LeafPointer. 1)
                                nil
                                (LeafMergeQueue$LeafPointer. 3)])]
      (letfn [(poll-and-advance []
                (let [leaf-ptr (.poll lmq)]
                  (when leaf-ptr
                    (.advance lmq leaf-ptr)
                    (.getOrdinal leaf-ptr))))]
        (t/is (= '(1 3 1 3 nil)
                 (repeatedly 5 poll-and-advance)))))))
