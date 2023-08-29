(ns xtdb.test-json
  (:require [cheshire.core :as json]
            [clojure.test :as t]
            [cognitect.transit :as transit]
            [xtdb.test-util :as tu]
            [xtdb.transit :as xt.transit]
            [xtdb.util :as util]
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

(defn check-arrow-json-file [^Path expected, ^Path actual]
  (t/is (= (json/parse-string (Files/readString expected))
           (json/parse-string (Files/readString actual)))
        actual))

(defn- read-transit-obj [stream]
  (transit/read (transit/reader stream :json {:handlers xt.transit/tj-read-handlers})))

(defn check-transit-json-file [^Path expected, ^Path actual]
  (with-open [expected-stream (Files/newInputStream expected (into-array OpenOption #{StandardOpenOption/READ}))
              actual-stream (Files/newInputStream expected (into-array OpenOption #{StandardOpenOption/READ}))]
    (t/is (= (read-transit-obj expected-stream)
             (read-transit-obj actual-stream))
          actual)))

#_{:clj-kondo/ignore [:unused-private-var]}
(defn- copy-expected-file [^Path file, ^Path expected-dir, ^Path actual-dir]
  (Files/copy file (doto (.resolve expected-dir (.relativize actual-dir file))
                     (-> (.getParent) (util/mkdirs)))
              ^"[Ljava.nio.file.CopyOption;" (into-array CopyOption #{StandardCopyOption/REPLACE_EXISTING})))

#_{:clj-kondo/ignore [:unused-private-var]}
(defn- delete-and-recreate-dir [^Path path]
  (when (.. path toFile isDirectory)
    (util/delete-dir path)
    (.mkdirs (.toFile path))))

(defn check-json
  ([expected-dir actual-dir] (check-json expected-dir actual-dir nil))

  ([^Path expected-dir, ^Path actual-dir, file-pattern]
   ;; uncomment if you want to remove files
   #_(delete-and-recreate-dir expected-dir) ;; <<no-commit>>
   (doseq [^Path path (iterator-seq (.iterator (Files/walk actual-dir (make-array FileVisitOption 0))))
           :let [file-name (str (.getFileName path))
                 file-type (cond
                             (str/ends-with? file-name ".arrow") :arrow
                             (str/ends-with? file-name ".transit.json") :transit)]
           :when (and file-type
                      (or (nil? file-pattern)
                          (re-matches file-pattern file-name)))]
     (doto (case file-type
             :arrow (write-arrow-json-file path)
             :transit path)
       ;; uncomment this to reset the expected file (but don't commit it)
       #_(copy-expected-file expected-dir actual-dir))) ;; <<no-commit>>

   (doseq [^Path expected (iterator-seq (.iterator (Files/walk expected-dir (make-array FileVisitOption 0))))
           :let [actual (.resolve actual-dir (.relativize expected-dir expected))
                 file-name (str (.getFileName expected))]
           :when (or (nil? file-pattern)
                     (re-matches file-pattern file-name))]
     (cond
       (.endsWith file-name ".arrow.json")
       (check-arrow-json-file expected actual)

       (.endsWith file-name ".transit.json")
       (check-transit-json-file expected actual)))))

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
