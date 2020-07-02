(ns leiningen.project-version
  (:require [clojure.java.shell :as sh]
            [clojure.string]
            [clojure.walk :refer [postwalk]]))

(defn project-version [project & args]
  (println (:version project)))

(defn- version-from-git []
  (let [{:keys [exit out]} (sh/sh "git" "describe" "--tags" "--dirty" "--long" "--match" "*.*-*")]
    (assert (= 0 exit))
    ;;"20.06-1.9.0-22-g9452e91b-dirty"
    (let [[_ tag ahead sha dirty] (re-find #"(.*)\-(\d+)\-([0-9a-z]*)(\-dirty)?$" (clojure.string/trim out))
          ahead? (not= ahead "0")
          dirty? (not (empty? dirty))]
      (assert (re-find #"^\d+\.\d+\-\d+\.\d+\.\d+(-(alpha|beta))?$" tag) (str "Tag format unexpected: " tag))
      (if (and (not ahead?) (not dirty?))
        {:prefix tag}
        (let [[_ prefix minor-version suffix] (re-find #"^(.*\.)(\d+)(-(alpha|beta))?$" tag)]
          {:prefix (str prefix (inc (Integer/parseInt minor-version)))
           :suffix "-SNAPSHOT"})))))

(def mvn-group-override
  (System/getenv "CRUX_MVN_GROUP"))

(defn middleware [project]
  (let [git-version (version-from-git)
        prefix (or (System/getenv "CRUX_GIT_VERSION_PREFIX") (:prefix git-version))
        suffix (or (System/getenv "CRUX_GIT_VERSION_SUFFIX") (:suffix git-version))]
    (-> project
        (->> (postwalk (fn [x]
                         (case x
                           "crux-git-version" (str prefix suffix)
                           "crux-git-version-alpha" (str prefix "-alpha" suffix)
                           "crux-git-version-beta" (str prefix "-beta" suffix)
                           x))))
        (cond-> mvn-group-override (-> (assoc :group mvn-group-override)
                                       (->> (postwalk (fn [x]
                                                        (if (and (symbol? x) (= "juxt" (namespace x)))
                                                          (symbol mvn-group-override (name x))
                                                          x)))))))))
