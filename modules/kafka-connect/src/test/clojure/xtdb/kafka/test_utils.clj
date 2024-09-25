(ns xtdb.kafka.test-utils
  (:import (xtdb.kafka.connect XtdbSinkConfig)))

(defn ->config [config]
  (XtdbSinkConfig/parse config))

(comment
  (->config {"jdbcUrl" "test"
             "id.mode" "record_key"}))
