(ns crux.memory-test
  (:require [clojure.test :as t]
            [crux.memory :as mem])
  (:import java.security.MessageDigest))

(defn- sha1 ^bytes [^bytes bytes]
  (.digest (MessageDigest/getInstance "SHA-1") bytes))

(t/deftest test-convert-bytes-to-hex-and-back
  (let [bs (mem/as-buffer (sha1 (.getBytes "Hello World")))
        hex "0a4d55a8d778e5022fab701977c5d840bbc486d0"]
    (t/is (= hex (mem/buffer->hex bs)))
    (t/is (mem/buffers=? bs (mem/hex->buffer hex))))

  (let [bs (mem/as-buffer (sha1 (.getBytes "Crux")))
        hex "0667c714c6512ac8d807d4e508327f3f9f8b3f7e"]
    (t/is (= hex (mem/buffer->hex bs)))
    (t/is (mem/buffers=? bs (mem/hex->buffer hex))))

  (t/testing "handles bytes with leading zero"
    (let [bs (mem/as-buffer (doto (sha1 (.getBytes "Hello World"))
                              (aset 0 (byte 0))))
          hex "004d55a8d778e5022fab701977c5d840bbc486d0"]
      (t/is (= hex (mem/buffer->hex bs)))
      (t/is (mem/buffers=? bs (mem/hex->buffer hex))))))

(t/deftest test-unsigned-bytes-comparator
  (t/is (neg? (mem/compare-buffers
               (mem/hex->buffer "00018d44296f8cff6850a017541a35d73ffa3bc038ef5263646e71017f32a1601522d913918ae3e61ac5d0d029b5f016")
               (mem/hex->buffer "0001a86185daa0c4e116a65f0f037b2ddc192c7aaa856f636e67017f32a1601522d913918ae3e61ac5d0d029b5f016"))))

  (t/is (pos? (mem/compare-buffers
               (mem/hex->buffer "00007f32a1601522d913918ae3e61ac5d0d029b5f016")
               (mem/hex->buffer "00003e25db88c4ecae5e979af0b2e6d30fcb3a0da1ef")))))

(t/deftest test-inc-unsigned-bytes
  (t/is (mem/buffers=? (mem/as-buffer (byte-array [1]))
                       (mem/inc-unsigned-buffer! (mem/as-buffer (byte-array [0])))))
  (t/is (mem/buffers=? (mem/as-buffer (byte-array [1 0]))
                       (mem/inc-unsigned-buffer! (mem/as-buffer (byte-array [0 0xff])))))
  (t/is (mem/buffers=? (mem/as-buffer (byte-array [3 0 0]))
                       (mem/inc-unsigned-buffer! (mem/as-buffer (byte-array [2 0xff 0xff])))))
  (t/is (mem/buffers=? (mem/as-buffer (byte-array [2 0 0xff]))
                       (mem/inc-unsigned-buffer! (mem/as-buffer (byte-array [1 0xff 0xff])) 2)))
  (t/testing "increasing max value returns nil"
    (t/is (nil? (mem/inc-unsigned-buffer! (mem/as-buffer (byte-array [0xff])))))))
