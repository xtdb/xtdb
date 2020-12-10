(ns crux.fixtures.lucene
  (:require [crux.fixtures :as fix :refer [with-tmp-dir]]
            [crux.fixtures :refer [*api*]]
            [crux.lucene :as l]))

(defn with-lucene-module [f]
  (with-tmp-dir "lucene" [db-dir]
    (fix/with-opts {::l/lucene-store {:db-dir db-dir}}
      f)))

(defn with-lucene-opts [lucene-opts]
  (fn [f]
    (with-tmp-dir "lucene" [db-dir]
      (fix/with-opts {::l/lucene-store (merge {:db-dir db-dir} lucene-opts)}
        f))))

(defn ^crux.api.ICursor search [f & args]
  (let [analyzer (:analyzer (:crux.lucene/lucene-store @(:!system *api*)))
        q (apply f analyzer args)]
    (l/search (:crux.lucene/lucene-store @(:!system *api*)) q)))
