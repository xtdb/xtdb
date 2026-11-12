(ns xtdb.bench.util
  (:import software.amazon.awssdk.services.s3.S3Client))

(def !s3-client (delay (S3Client/create)))
