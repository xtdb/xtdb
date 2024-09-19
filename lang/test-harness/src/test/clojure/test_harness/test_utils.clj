(ns test-harness.test-utils
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(defn- get-root-path
  "Returns a path to the root of the project (with a .git directory)"
  []
  (let [path (.getCanonicalFile (io/file "."))]
    (if (str/ends-with? (str path) "/lang/test-harness")
      (.getParentFile (.getParentFile path))
      path)))

(def root-path (delay (get-root-path)))
