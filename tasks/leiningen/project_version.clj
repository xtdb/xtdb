(ns leiningen.project-version)

(defn project-version [project & args]
  (println (:version project)))
