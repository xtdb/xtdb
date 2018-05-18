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
