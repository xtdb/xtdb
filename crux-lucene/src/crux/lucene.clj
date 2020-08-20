(ns crux.lucene
  (:import [org.apache.lucene.store Directory FSDirectory]))

(defrecord LuceneNode [directory]
  java.io.Closeable
  (close [this]
    (.close ^Directory directory)))

(defn- start-lucene-node [_ {::keys [db-dir]}]
  (let [directory (FSDirectory/open db-dir)]
    (LuceneNode. directory)))

(def module {::node {:start-fn start-lucene-node
                     :args {::db-dir {:doc "Lucene DB Dir"
                                      :crux.config/type :crux.config/path}}}})
