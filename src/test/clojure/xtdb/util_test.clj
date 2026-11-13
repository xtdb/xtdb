(ns xtdb.util-test
  (:require [clojure.test :as t :refer [deftest]]
            [clojure.tools.logging.test :as tlt]
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

(deftest uncaught-exception-handler-names-the-thread
  (let [ex (OutOfMemoryError. "Java heap space")]
    (tlt/with-log
      (.uncaughtException util/uncaught-exception-handler (Thread. "dying-thread") ex)
      (t/is (tlt/logged? 'xtdb.util :error ex #"thread dying-thread")))))
