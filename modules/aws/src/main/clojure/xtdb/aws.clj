(ns xtdb.aws
  (:require [xtdb.buffer-pool :as bp]
            [xtdb.util :as util])
  (:import [xtdb.aws S3]
           [xtdb.aws.s3 S3Configurator]))

(defmethod bp/->object-store-factory ::s3 [_ {:keys [bucket ^S3Configurator configurator prefix credentials endpoint]}]
  (cond-> (S3/s3 bucket)
    configurator (.s3Configurator configurator)
    prefix (.prefix (util/->path prefix))
    credentials (.credentials (:access-key credentials) (:secret-key credentials))
    endpoint (.endpoint endpoint)))
