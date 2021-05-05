(ns core2.json
  (:require [clojure.java.io :as io]
            [core2.util :as util])
  (:import java.io.File
           java.nio.ByteBuffer
           java.nio.channels.FileChannel
           [java.nio.file OpenOption StandardOpenOption]
           org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector.ipc ArrowFileReader ArrowStreamReader JsonFileWriter]
           org.apache.arrow.vector.VectorSchemaRoot))

(set! *unchecked-math* :warn-on-boxed)

(defn- file->json-file ^java.io.File [^File file]
  (io/file (.getParentFile file) (format "%s.json" (.getName file))))

(defn write-arrow-json-files
  ([^File arrow-dir]
   (write-arrow-json-files arrow-dir #".*"))
  ([^File arrow-dir file-pattern]
   (with-open [allocator (RootAllocator.)]
     (doseq [^File file (.listFiles arrow-dir)
             :when (and (.endsWith (.getName file) ".arrow") (re-matches file-pattern (.getName file)))]
       (with-open [file-ch (FileChannel/open (.toPath file)
                                             (into-array OpenOption #{StandardOpenOption/READ}))
                   file-reader (ArrowFileReader. file-ch allocator)
                   file-writer (JsonFileWriter. (file->json-file file)
                                                (.. (JsonFileWriter/config) (pretty true)))]
         (let [root (.getVectorSchemaRoot file-reader)]
           (.start file-writer (.getSchema root) nil)
           (while (.loadNextBatch file-reader)
             (.write file-writer root))))))))

(defn arrow-streaming->json ^String [^ByteBuffer buf]
  (let [json-file (File/createTempFile "arrow" "json")]
    (try
      (with-open [allocator (RootAllocator.)
                  in-ch (util/->seekable-byte-channel buf)
                  file-reader (ArrowStreamReader. in-ch allocator)
                  file-writer (JsonFileWriter. json-file (.. (JsonFileWriter/config) (pretty true)))]
        (let [root (.getVectorSchemaRoot file-reader)]
          (.start file-writer (.getSchema root) nil)
          (while (.loadNextBatch file-reader)
            (.write file-writer root))))
      (slurp json-file)
      (finally
        (.delete json-file)))))

(defn vector-schema-root->json ^String [^VectorSchemaRoot root]
  (let [json-file (File/createTempFile "arrow" "json")]
    (try
      (with-open [file-writer (JsonFileWriter. json-file (.. (JsonFileWriter/config) (pretty true)))]
        (.start file-writer (.getSchema root) nil)
        (.write file-writer root))
      (slurp json-file)
      (finally
        (.delete json-file)))))
