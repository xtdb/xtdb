(ns javadoc
  (:require [clojure.java.shell :as sh]))

(defn generate-javadoc [target-dir]
  (let [{:keys [out err exit]}
        (sh/sh "javadoc"
               "-cp" (System/getProperty "java.class.path")
               "-d" target-dir
               "-sourcepath" "src/"
               "-exclude" "crux"
               "crux.api")]
    (when err
      (println err))
    (println out)
    exit))

(defn -main [target-dir & args]
  (let [exit (generate-javadoc (or target-dir "docs/javadoc"))]
    (System/exit exit)))
