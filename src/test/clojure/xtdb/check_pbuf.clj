(ns xtdb.check-pbuf
  (:require [clojure.string :as str]
            [xtdb.test-json :as tj])
  (:import [java.nio.file FileVisitOption Files Path]
           (xtdb.block.proto Block TableBlock)))

(defn multi-parse-fn
  "Tries to parse a byte array from a given list of parse-fns. Throws if non parses."
  [parse-fns]
  (fn [& args]
    (or (some #(try (apply % args) (catch Exception _e nil)) parse-fns)
        (throw (RuntimeException. "No matching parse-fn!")))))

(defn check-pbuf
  ([expected-dir actual-dir] (check-pbuf expected-dir actual-dir (multi-parse-fn [#(Block/parseFrom ^bytes %)
                                                                                  #(TableBlock/parseFrom ^bytes %)])))
  ([expected-dir actual-dir parse-fn] (check-pbuf expected-dir actual-dir parse-fn nil))

  ([^Path expected-dir, ^Path actual-dir, parse-fn, file-pattern]
   ;; uncomment if you want to remove files
   #_(tj/delete-and-recreate-dir expected-dir) ;; <<no-commit>>
   (doseq [^Path path (iterator-seq (.iterator (Files/walk actual-dir (make-array FileVisitOption 0))))
           :let [file-name (str (.getFileName path))
                 file-type (cond
                             (str/ends-with? file-name ".binpb") :protobuf)]
           :when (and file-type
                      (or (nil? file-pattern)
                          (re-matches file-pattern file-name)))]
     (doto (case file-type
             (:protobuf) path)
       ;; uncomment this to reset the expected file (but don't commit it)
       #_(tj/copy-expected-file expected-dir actual-dir))) ;; <<no-commit>>

   (doseq [^Path expected (iterator-seq (.iterator (Files/walk expected-dir (make-array FileVisitOption 0))))
           :let [actual (.resolve actual-dir (.relativize expected-dir expected))
                 file-name (str (.getFileName expected))]]
     (cond
       (.endsWith file-name ".binpb")
       (= (parse-fn (Files/readAllBytes expected)) (parse-fn (Files/readAllBytes actual)))))))
