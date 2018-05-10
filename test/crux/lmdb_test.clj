(ns crux.lmdb-test
  (:require [clojure.test :as t]
            [crux.byte-utils :as bu]
            [crux.test-utils :as tu]
            [crux.lmdb :as lmdb]))

;; Based on
;; https://github.com/LWJGL/lwjgl3/blob/master/modules/samples/src/test/java/org/lwjgl/demo/util/lmdb/LMDBDemo.java
(t/deftest test-lmdb-demo []
  (let [db-dir (tu/create-tmpdir "lmdb")
        env (lmdb/env-create)]
    (try
      (lmdb/env-open env db-dir)
      (let [dbi (lmdb/dbi-open env)]
        (lmdb/put-bytes->bytes env
                               dbi
                               (bu/long->bytes 1)
                               (.getBytes "LMDB"))
        (t/is (= "LMDB" (String. (lmdb/get-bytes->bytes env dbi (bu/long->bytes 1))))))
      (finally
        (tu/delete-dir db-dir)
        (lmdb/env-close env)))))
