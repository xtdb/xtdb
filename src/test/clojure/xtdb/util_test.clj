(ns xtdb.util-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.util :as util])
  (:import java.nio.ByteBuffer))

(deftest uuid-conversion-utils-test
  (let [uuid (random-uuid)]
    (t/is (= uuid 
             (util/byte-buffer->uuid
               (util/uuid->byte-buffer uuid))
             (util/byte-buffer->uuid
               (ByteBuffer/wrap
                 (util/uuid->bytes uuid)))))))
