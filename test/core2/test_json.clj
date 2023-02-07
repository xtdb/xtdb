(ns core2.test-json
  (:require [cheshire.core :as json]
            [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.util :as util]
            [clojure.string :as str])
  (:import java.io.File
           java.nio.ByteBuffer
           java.nio.channels.FileChannel
           [java.nio.file CopyOption FileVisitOption Files OpenOption Path StandardCopyOption StandardOpenOption]
           org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector.ipc ArrowFileReader ArrowStreamReader JsonFileWriter]))

(defn- file->json-file ^java.nio.file.Path [^Path file]
  (.resolve (.getParent file) (format "%s.json" (.getFileName file))))

(defn write-arrow-json-file ^java.nio.file.Path [^Path file]
  (let [json-file (file->json-file file)]
    (with-open [file-ch (FileChannel/open file (into-array OpenOption #{StandardOpenOption/READ}))
                file-reader (ArrowFileReader. file-ch tu/*allocator*)
                file-writer (JsonFileWriter. (.toFile json-file)
                                             (.. (JsonFileWriter/config) (pretty true)))]
      (let [root (.getVectorSchemaRoot file-reader)]
        (.start file-writer (.getSchema root) nil)
        (while (.loadNextBatch file-reader)
          (.write file-writer root)))

      json-file)))

(defn check-json-file [^Path expected, ^Path actual]
  (t/is (= (json/parse-string (Files/readString expected))
           (json/parse-string (Files/readString actual)))
        actual))

#_{:clj-kondo/ignore [:unused-private-var]}
(defn- copy-expected-file [^Path file, ^Path expected-dir, ^Path actual-dir]
  (Files/copy file (doto (.resolve expected-dir (.relativize actual-dir file))
                     (-> (.getParent) (util/mkdirs)))
              ^"[Ljava.nio.file.CopyOption;" (into-array CopyOption #{StandardCopyOption/REPLACE_EXISTING})))

(defn check-json
  ([expected-dir actual-dir] (check-json expected-dir actual-dir nil))

  ([^Path expected-dir, ^Path actual-dir, file-pattern]
   (doseq [^Path path (iterator-seq (.iterator (Files/walk actual-dir (make-array FileVisitOption 0))))
           :let [file-name (str (.getFileName path))]
           :when (and (str/ends-with? file-name ".arrow")
                      (or (nil? file-pattern)
                          (re-matches file-pattern file-name)))]
     (doto (write-arrow-json-file path)
       #_ ; uncomment this to reset the expected file (but don't commit it)
       (copy-expected-file expected-dir actual-dir)))

   (doseq [^Path expected (iterator-seq (.iterator (Files/walk expected-dir (make-array FileVisitOption 0))))
           :when (.endsWith (str (.getFileName expected)) ".json")]
     (check-json-file expected (.resolve actual-dir (.relativize expected-dir expected))))))

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
