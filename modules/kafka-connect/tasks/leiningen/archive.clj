(ns ^:no-doc leiningen.archive
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as string]
            [clojure.pprint]
            [leiningen.core.eval]
            [leiningen.licenses :as licenses]))

(defn archive [{:keys [version target-path confluent-hub-manifest group name] :as project} & args]
  (let [manifest  (leiningen.core.eval/eval-in-project
                   (assoc project :eval-in :leiningen)
                   `(juxt.clojars-mirrors.cheshire.v5v10v0.cheshire.core/generate-string ~confluent-hub-manifest)
                   '(require 'juxt.clojars-mirrors.cheshire.v5v10v0.cheshire.core))
        archive-dir (io/file target-path (str group "-" (:name confluent-hub-manifest) "-" version))
        lib-dir (io/file archive-dir "lib")
        etc-dir (io/file archive-dir "etc")
        assets-dir (io/file archive-dir "assets")
        doc-dir (io/file archive-dir "doc")
        jar-filename (str name "-" version "-standalone.jar")
        licenses (with-out-str (licenses/licenses project))
        sorted-licenses (string/join "\n" (sort (string/split licenses #"\n")))]

    (.mkdirs lib-dir)
    (.mkdirs etc-dir)
    (.mkdirs assets-dir)
    (.mkdirs doc-dir)

    (io/copy (io/file target-path jar-filename)
             (io/file lib-dir jar-filename))
    (io/copy (io/file "README.adoc")
             (io/file doc-dir "README.adoc"))
    (io/copy (io/file "../LICENSE")
             (io/file doc-dir "LICENSE"))
    (io/copy (io/file "test-resources/local-xtdb-source.properties")
             (io/file etc-dir "local-xtdb-source.properties"))
    (io/copy (io/file "test-resources/local-xtdb-sink.properties")
             (io/file etc-dir "local-xtdb-sink.properties"))
    (io/copy (io/file "resources/crux-logo.svg")
             (io/file assets-dir "crux-logo.svg"))

    (spit (io/file archive-dir "manifest.json") manifest)
    (spit (io/file doc-dir "version.txt") version)
    (spit (io/file doc-dir "licenses.txt") sorted-licenses)
    (sh/sh "zip"
           "-r"
           (str (.getName archive-dir) ".zip")
           (str (.getName archive-dir))
           :dir (.getParent archive-dir)))

  (println "Connector packaged successfully."))
