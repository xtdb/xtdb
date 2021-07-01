(ns crux.fixtures.lucene
  (:require [crux.fixtures :as fix :refer [*api*]]
            [crux.lucene :as l])
  (:import [org.apache.lucene.index DirectoryReader IndexReader IndexWriter]))

(defn with-lucene-opts [lucene-opts]
  (fn [f]
    (fix/with-tmp-dirs #{db-dir}
      (fix/with-opts {::l/lucene-store (merge {:db-dir db-dir} lucene-opts)}
        f))))

(defn- lucene-store []
  (:crux.lucene/lucene-store @(:!system *api*)))

(defn ^crux.api.ICursor search [f & args]
  (let [{:keys [analyzer searcher-manager]} (lucene-store)
        q (apply f analyzer args)]
    (l/search searcher-manager q)))

(defn doc-count []
  (let [{:keys [^IndexWriter index-writer]} (lucene-store)
        index-reader (DirectoryReader/open index-writer)]
    (.numDocs index-reader)))
