(ns xtdb.trie-catalog
  (:require [clojure.tools.logging :as clj-log]
            [integrant.core :as ig]
            [xtdb.util :as util])
  (:import [java.util Set]
           [java.util.concurrent ConcurrentSkipListSet]
           (xtdb BufferPool)))

(defmethod ig/prep-key :xtdb/trie-catalog [_ opts]
  (into {:buffer-pool (ig/ref :xtdb/buffer-pool)} opts))

(defrecord TrieCatalog [^Set table-names]
  xtdb.trie.TrieCatalog
  (addTrie [_ table-name trie-key]
    (.add table-names table-name)
    (clj-log/debug "addTrie" table-name trie-key))

  (getTableNames [_] (set table-names)))

(defmethod ig/init-key :xtdb/trie-catalog [_ {:keys [^BufferPool buffer-pool]}]
  (->TrieCatalog (doto (ConcurrentSkipListSet.)
                   (.addAll (map str (.listObjects buffer-pool util/tables-dir))))))
