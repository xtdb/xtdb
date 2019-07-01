(ns crux.byte-utils-test
  (:require [clojure.test :as t]
            [crux.byte-utils :as bu]))

(t/deftest test-convert-bytes-to-hex-and-back
  (let [bs (bu/sha1 (.getBytes "Hello World"))
        hex "0a4d55a8d778e5022fab701977c5d840bbc486d0"]
    (t/is (= hex (bu/bytes->hex bs)))
    (t/is (bu/bytes=? bs (bu/hex->bytes hex))))

  (let [bs (bu/sha1 (.getBytes "Crux"))
        hex "0667c714c6512ac8d807d4e508327f3f9f8b3f7e"]
    (t/is (= hex (bu/bytes->hex bs)))
    (t/is (bu/bytes=? bs (bu/hex->bytes hex))))

  (t/testing "handles bytes with leading zero"
    (let [bs (doto (bu/sha1 (.getBytes "Hello World"))
               (aset 0 (byte 0)))
          hex "004d55a8d778e5022fab701977c5d840bbc486d0"]
      (t/is (= hex (bu/bytes->hex bs)))
      (t/is (bu/bytes=? bs (bu/hex->bytes hex))))))

(t/deftest test-unsigned-bytes-comparator
  (t/is (neg? (.compare bu/bytes-comparator
                        (bu/hex->bytes "00018d44296f8cff6850a017541a35d73ffa3bc038ef5263646e71017f32a1601522d913918ae3e61ac5d0d029b5f016")
                        (bu/hex->bytes "0001a86185daa0c4e116a65f0f037b2ddc192c7aaa856f636e67017f32a1601522d913918ae3e61ac5d0d029b5f016"))))

  (t/is (pos? (.compare bu/bytes-comparator
                        (bu/hex->bytes "00007f32a1601522d913918ae3e61ac5d0d029b5f016")
                        (bu/hex->bytes "00003e25db88c4ecae5e979af0b2e6d30fcb3a0da1ef")))))

(t/deftest test-inc-unsigned-bytes
  (t/is (bu/bytes=? (byte-array [1])
                    (bu/inc-unsigned-bytes! (byte-array [0]))))
  (t/is (bu/bytes=? (byte-array [1 0])
                    (bu/inc-unsigned-bytes! (byte-array [0 0xff]))))
  (t/is (bu/bytes=? (byte-array [3 0 0])
                    (bu/inc-unsigned-bytes! (byte-array [2 0xff 0xff]))))
  (t/is (bu/bytes=? (byte-array [2 0 0xff])
                    (bu/inc-unsigned-bytes! (byte-array [1 0xff 0xff]) 2)))
  (t/testing "increasing max value returns nil"
    (t/is (nil? (bu/inc-unsigned-bytes! (byte-array [0xff]))))))
