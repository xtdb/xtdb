(ns xtdb.bench.util
  (:import [java.nio.file Path Files]
           java.nio.file.attribute.FileAttribute
           software.amazon.awssdk.services.s3.S3Client
           software.amazon.awssdk.services.s3.model.GetObjectRequest))

(defn tmp-file-path ^java.nio.file.Path [prefix suffix]
  (doto (Files/createTempFile prefix suffix (make-array FileAttribute 0))
    (Files/delete)))

(def !s3-client (delay (S3Client/create)))

(defn download-s3-dataset-file [s3-key ^Path tmp-path]
  (.getObject ^S3Client @!s3-client
              (-> (GetObjectRequest/builder)
                  (.bucket "xtdb-datasets")
                  (.key s3-key)
                  ^GetObjectRequest (.build))
              tmp-path)
  tmp-path)
