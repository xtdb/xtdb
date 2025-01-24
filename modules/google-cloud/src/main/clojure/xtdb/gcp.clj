(ns xtdb.gcp
  (:require [xtdb.buffer-pool :as bp]
            [xtdb.util :as util])
  (:import [xtdb.api.storage GoogleCloudStorage]))

(defmethod bp/->object-store-factory ::object-store [_ {:keys [project-id bucket prefix]}]
  (cond-> (GoogleCloudStorage/googleCloudStorage project-id bucket)
    prefix (.prefix (util/->path prefix))))
