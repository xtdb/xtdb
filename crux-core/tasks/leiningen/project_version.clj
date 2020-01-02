(ns leiningen.project-version
  (:require [clojure.java.shell :as sh]
            [clojure.string]
            [clojure.walk :refer [postwalk]]))

(defn project-version [project & args]
  (println (:version project)))

(defn- version-from-git []
  (let [{:keys [exit out]} (sh/sh "git" "describe" "--tags" "--dirty" "--long" "--match" "*.*-*")]
    (assert (= 0 exit))
    ;;"19.04-1.0.2-alpha-22-g9452e91b-dirty"
    (let [[_ tag ahead sha dirty] (re-find #"(.*)\-(\d+)\-([0-9a-z]*)(\-dirty)?$" (clojure.string/trim out))
          ahead? (not= ahead "0")
          dirty? (not (empty? dirty))]
      (assert (re-find #"^\d+\.\d+\-\d+\.\d+\.\d+(\-(alpha|beta))?$" tag) (str "Tag format unexpected: " tag))
      (if (and (not ahead?) (not dirty?))
        tag
        (let [[_ prefix minor-version suffix] (re-find #"^(.*\.)(\d)+(\-(alpha|beta))?$" tag)]
          (format "%s%s%s-SNAPSHOT" prefix (inc (Integer/parseInt minor-version)) suffix))))))

(def mvn-group-override
  (System/getenv "CRUX_MVN_GROUP"))

(defn middleware [project]
  (let [project-version (version-from-git)]
    (-> project
        (->> (postwalk (fn [x]
                         (if (= "derived-from-git" x) project-version x))))
        (cond-> mvn-group-override (-> (assoc :group mvn-group-override)
                                       (->> (postwalk (fn [x]
                                                        (if (and (symbol? x) (= "juxt" (namespace x)))
                                                          (symbol mvn-group-override (name x))
                                                          x)))))))))
