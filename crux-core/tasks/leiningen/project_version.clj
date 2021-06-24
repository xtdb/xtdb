(ns leiningen.project-version
  (:require [clojure.java.shell :as sh]
            [clojure.walk :refer [postwalk]]
            [clojure.string :as str]))

(defn project-version [project & args]
  (println (:version project)))

(defn- parse-git-describe [out]
  (when-let [[_ tag ahead dirty] (re-find #"(.*)\-(\d+)\-(?:[0-9a-z]*)(\-dirty)?$" (clojure.string/trim out))]
    {:tag tag
     :ahead? (not= ahead "0")
     :dirty? (not (empty? dirty))}))

(defn- version-from-git []
  (let [{:keys [exit out]} (sh/sh "git" "describe" "--tags" "--dirty" "--long" "--match" "*.*-*")]
    (assert (zero? exit))
    (when-let [{:keys [tag ahead? dirty?]} (parse-git-describe (str/trim out))]
      (when-not (or ahead? dirty?)
        (assert (re-find #"^\d+\.\d+\-\d+\.\d+\.\d+(-(alpha|beta))?$" tag) (str "Tag format unexpected: " tag))
        tag))))

(def mvn-group-override
  (System/getenv "CRUX_MVN_GROUP"))

(defn middleware [project]
  (let [git-version (or (System/getenv "CRUX_VERSION")
                        (version-from-git))]
    (-> project
        (->> (postwalk (fn [x]
                         (case x
                           "crux-git-version" (or git-version "dev-SNAPSHOT")
                           "crux-git-version-alpha" (or (some-> git-version (str "-alpha"))
                                                        "dev-SNAPSHOT")
                           "crux-git-version-beta" (or (some-> git-version (str "-beta"))
                                                       "dev-SNAPSHOT")
                           x))))
        (cond-> mvn-group-override (-> (assoc :group mvn-group-override)
                                       (->> (postwalk (fn [x]
                                                        (if (and (symbol? x) (= "pro.juxt.crux" (namespace x)))
                                                          (symbol mvn-group-override (name x))
                                                          x)))))))))
