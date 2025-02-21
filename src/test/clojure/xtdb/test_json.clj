(ns xtdb.test-json
  (:require [clojure.string :as str]
            [clojure.test :as t]
            [clojure.walk :as walk]
            [cognitect.transit :as transit]
            [jsonista.core :as json]
            [xtdb.metadata :as meta]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import clojure.lang.MapEntry
           java.io.File
           java.nio.ByteBuffer
           java.nio.channels.FileChannel
           [java.nio.file CopyOption FileVisitOption Files OpenOption Path StandardCopyOption StandardOpenOption]
           org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector.ipc ArrowFileReader ArrowStreamReader JsonFileWriter]))

(defn sort-arrow-json
  "For use in tests to provide consitent ordering to where ordering is undefined"
  [arrow-json-tree-as-clojure]
  (walk/postwalk
   (fn [node]
     (if (and (map-entry? node)
              (#{"children" "fields" "columns"} (key node)))
       (MapEntry/create (key node) (sort-by #(get % "name") (val node)))
       node))
   arrow-json-tree-as-clojure))

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

(comment
  (with-open [al (RootAllocator.)]
    (binding [tu/*allocator* al]
      (write-arrow-json-file (util/->path "/tmp/test.arrow")))))

(defn check-arrow-json-file [^Path expected, ^Path actual]
  (t/is (= (sort-arrow-json (json/read-value (Files/readString expected)))
           (sort-arrow-json (json/read-value (Files/readString actual))))
        actual))

(defn- read-transit-obj [stream]
  (transit/read (transit/reader stream :json {:handlers meta/metadata-read-handler-map})))

(defn check-transit-json-file [^Path expected, ^Path actual]
  (with-open [expected-stream (Files/newInputStream expected (into-array OpenOption #{StandardOpenOption/READ}))
              actual-stream (Files/newInputStream actual (into-array OpenOption #{StandardOpenOption/READ}))]
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
                             (str/ends-with? file-name ".transit.json") :transit
                             ;; TODO this should like not go in test-json
                             (str/ends-with? file-name ".binpb") :protobuf)]
           :when (and file-type
                      (or (nil? file-pattern)
                          (re-matches file-pattern file-name)))]
     (doto (case file-type
             :arrow (write-arrow-json-file path)
             (:transit :protobuf) path)
       ;; uncomment this to reset the expected file (but don't commit it)
       #_(copy-expected-file expected-dir actual-dir))) ;; <<no-commit>>

   (doseq [^Path expected (iterator-seq (.iterator (Files/walk expected-dir (make-array FileVisitOption 0))))
           :let [actual (.resolve actual-dir (.relativize expected-dir expected))
                 file-name (str (.getFileName expected))]]
     (cond
       (.endsWith file-name ".arrow.json")
       (check-arrow-json-file expected actual)

       (.endsWith file-name ".transit.json")
       (check-transit-json-file expected actual)

       (.endsWith file-name ".binpb")
       (= (seq (Files/readAllBytes expected)) (seq (Files/readAllBytes actual)))))))

(defn arrow-streaming->json ^String [^ByteBuffer buf]
  (let [json-file (File/createTempFile "arrow" "json")]
    (try
      (util/with-open [allocator (RootAllocator.)
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
