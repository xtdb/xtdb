(ns leiningen.project-version
  (:require [clojure.java.shell :as sh]
            [clojure.string]
            [clojure.walk :refer [postwalk]]))

(defn project-version [project & args]
  (println (:version project)))

(defn- version-from-git []
  (let [{:keys [exit out]} (sh/sh "git" "describe" "--tags" "--dirty" "--long")]
    (assert (= 0 exit))
    ;;"19.04-1.0.2-alpha-22-g9452e91b-dirty"
    (let [[_ tag ahead sha dirty] (re-find #"(.*)\-(\d+)\-([0-9a-z]*)(\-dirty)?$" (clojure.string/trim out))
          ahead? (not= ahead "0")
          dirty? (not (empty? dirty))]
      (assert (re-find #"^\d+\.\d+\-\d+\.\d+\.\d+(\-(alpha|beta))?$" tag) "Tag format unexpected.")
      (if (and (not ahead?) (not dirty?))
        tag
        (let [[_ prefix minor-version suffix] (re-find #"^(.*\.)(\d)+(\-(alpha|beta))?$" tag)]
          (format "%s%s%s-SNAPSHOT" prefix (inc (Integer/parseInt minor-version)) suffix))))))

(defn middleware [project]
  (let [project-version (version-from-git)]
    (postwalk (fn [x] (if (= "derived-from-git" x)
                        project-version x))
              project)))
