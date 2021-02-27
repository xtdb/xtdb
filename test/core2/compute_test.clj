(ns core2.compute-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.compute :as cc])
  (:import [org.apache.arrow.memory RootAllocator]
           [org.apache.arrow.vector BigIntVector Float8Vector ValueVector]))

(t/deftest can-compute-vectors
  (with-open [a (RootAllocator.)]
    (binding [cc/*allocator* a]
      (with-open [is (doto (BigIntVector. "" cc/*allocator*)
                       (.allocateNew)
                       (.setSafe 0 1)
                       (.setSafe 1 2)
                       (.setSafe 2 3)
                       (.setValueCount 3))
                  fs (doto (Float8Vector. "" cc/*allocator*)
                       (.allocateNew)
                       (.setSafe 0 1.0)
                       (.setSafe 1 2.0)
                       (.setSafe 2 3.0)
                       (.setValueCount 3))
                  out-fs (Float8Vector. "" cc/*allocator*)
                  is+f ^ValueVector (cc/op :+ is 2.0 {:out out-fs})
                  is+i ^ValueVector (cc/op :+ is 2)
                  is+fs ^ValueVector (cc/op :+ is fs)
                  is+is ^ValueVector (cc/op :+ is is)
                  fs+i ^ValueVector (cc/op :+ fs 2)
                  fs+f ^ValueVector (cc/op :+ fs 2.0)
                  fs+is ^ValueVector (cc/op :+ fs is)
                  fs+fs ^ValueVector (cc/op :+ fs fs)]

        (t/is (identical? is+f out-fs))

        (t/is (= [3.0 4.0 5.0] (tu/->list is+f)))
        (t/is (= [3 4 5] (tu/->list is+i)))
        (t/is (= [2.0 4.0 6.0] (tu/->list is+fs)))
        (t/is (= [2 4 6] (tu/->list is+is)))

        (t/is (= [3.0 4.0 5.0] (tu/->list fs+i)))
        (t/is (= [3.0 4.0 5.0] (tu/->list fs+f)))
        (t/is (= [2.0 4.0 6.0] (tu/->list fs+is)))
        (t/is (= [2.0 4.0 6.0] (tu/->list fs+fs)))

        (t/is (= 6 (cc/op :sum is)))
        (t/is (= 6 (cc/op :sum 0 is)))
        (t/is (= 6 (cc/op :sum 0.0 is)))
        (t/is (= 6.0 (cc/op :sum 0 fs)))
        (t/is (= 6.0 (cc/op :sum 0.0 fs)))

        (t/is (= 1.0 (cc/op :min fs)))
        (t/is (= 1 (cc/op :min is)))
        (t/is (= 3.0 (cc/op :max fs)))
        (t/is (= 3 (cc/op :max is)))

        (t/is (== 2.0 (cc/op :avg is)))
        (t/is (== 2.0 (cc/op :avg fs)))

        (t/is (= 1.0 (cc/op :min Double/MAX_VALUE fs)))
        (t/is (= 1 (cc/op :min Long/MAX_VALUE is)))
        (t/is (= 3.0 (cc/op :max Double/MIN_VALUE fs)))
        (t/is (= 3 (cc/op :max Long/MIN_VALUE is)))
        (t/is (= 3.0 (cc/op :max Long/MIN_VALUE fs)))

        (t/is (= 3 (cc/op :count fs)))))))
