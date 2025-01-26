(ns xtdb.gcp
  (:require [xtdb.buffer-pool :as bp]
            [xtdb.util :as util])
  (:import [xtdb.gcp CloudStorage]))

(defmethod bp/->object-store-factory ::object-store [_ {:keys [project-id bucket prefix]}]
  (cond-> (CloudStorage/googleCloudStorage project-id bucket)
    prefix (.prefix (util/->path prefix))))
