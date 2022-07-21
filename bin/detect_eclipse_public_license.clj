#!/usr/bin/env bb

;; Run core2/bin/detect-and-create-third-party-notices.sh before running this script

(ns detect-eclipse-public-license
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.data.xml :as xml]
            [clojure.tools.logging :as log]
            [clojure.java.shell]
            [babashka.fs :as fs]))

(defn root-file-path [file]
  (fs/normalize (fs/path (fs/parent *file*) ".." (fs/path file))))

(defn get-xml []
  (let [path (root-file-path "THIRD_PARTY_NOTICES.xml")
        file (io/file (str path))
        stream (io/input-stream file)]
    (xml/parse stream)))

(defn get-artifacts [x]
  (->> (:content x)
       (filter #(= (:tag %) :artifacts))
       first
       :content
       vec
       (filter #(= (:tag %) :artifact))))

(defn epl? [a]
  ;; this is the point where a person really wishes he'd bothered to grab a zipper? -sd
  ;; (first (:content (first (filter #(= (:tag %) :license) (:content (first artifacts))))))
  (some->> a :content
           (filter #(= (:tag %) :license))
           first
           :content
           first
           (re-matches #".*Eclipse.*")))

(defn libbify [s]
  (let [parts (clojure.string.split s #":")]
    (str (first parts) "/" (second parts))))

(defn get-epl-libs [as]
  (->> as
       (map (fn [a] (-> a :attrs :id)))
       (map libbify)))

(defn ->grep [s]
  (re-pattern (str "(?s).*"
                   (-> s
                       (clojure.string/replace "/" "\\/")
                       (clojure.string/replace "." "\\."))
                   ".*")))

(defn get-missing-exceptions [libs]
  (let [license (slurp (str (root-file-path "LICENSE")))]
    (remove #(re-matches (->grep %) license) libs)))

(defn print-license-list [l]
  (doseq [e l] (println (str "* " e))))

(def ADDITIONAL-PERMISSIONS-FILE "target/agplv3-additional-permissions.txt")

(def section7block
  "
If you modify this Program, or any covered work, by linking or combining it with
LIBRARY_NAME (or a modified version of that library),
containing parts covered by the terms of the Eclipse Public License 1.0
or Eclipse Public License 2.0, the licensors of this Program grant you
additional permission to convey the resulting work.
")

(defn additional-permissions [libs]
  (str "
Additional permissions under GNU AGPL version 3 section 7
"
       (clojure.string/join ""
        (for [lib libs]
          (clojure.string/replace section7block "LIBRARY_NAME" lib)))))

(defn write-additional-permissions-file [libs]
  (let [section7 (additional-permissions libs)]
    (spit (str (root-file-path ADDITIONAL-PERMISSIONS-FILE))
          section7)))

(def ERROR 1)

(defn main [& args]
  (let [x (get-xml)
        artifacts (get-artifacts x)
        epl-artifacts (filter epl? artifacts)
        epl-libs (get-epl-libs epl-artifacts)
        missing-exceptions (get-missing-exceptions epl-libs)]
    (when (seq epl-libs)
      (println "\nLibraries under Eclipse Public License, which require AGPLv3 exceptions:")
      (print-license-list epl-libs)
      (write-additional-permissions-file epl-libs)
      (println "\nRequired exceptions have been printed to "
               (str (root-file-path ADDITIONAL-PERMISSIONS-FILE))
               "\nYou can copy/paste these into LICENSE if changes are required."))
    (when (seq missing-exceptions)
      (println "\nLICENSE is missing the follow exceptions: ")
      (print-license-list missing-exceptions)
      (System/exit ERROR))
    (println "\n...all required exceptions were found in LICENSE. No changes required.")))

(defn -main [& args]
  (main args))

(main)
