(ns core2.json
  (:require [clojure.java.io :as io])
  (:import java.io.File
           java.nio.channels.FileChannel
           [java.nio.file OpenOption StandardOpenOption]
           org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector.ipc ArrowFileReader JsonFileWriter]))

(defn write-arrow-json-files [^File arrow-dir]
  (with-open [allocator (RootAllocator. Long/MAX_VALUE)]
    (doseq [^File
            file (->> (.listFiles arrow-dir)
                      (filter #(.endsWith (.getName ^File %) ".arrow")))]
      (with-open [file-ch (FileChannel/open (.toPath file)
                                            (into-array OpenOption #{StandardOpenOption/READ}))
                  file-reader (ArrowFileReader. file-ch allocator)
                  file-writer (JsonFileWriter. (io/file arrow-dir (format "%s.json" (.getName file)))
                                               (.. (JsonFileWriter/config) (pretty true)))]
        (let [root (.getVectorSchemaRoot file-reader)]
          (.start file-writer (.getSchema root) nil)
          (while (.loadNextBatch file-reader)
            (.write file-writer root)))))))
