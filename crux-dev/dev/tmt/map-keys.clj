(ns tmt.map-keys
  (:require [crux.api :as c]
            [taoensso.nippy :as nippy]))

(def node-mem
  (c/start-node {:crux.node/topology 'crux.standalone/topology
                 :crux.node/kv-store 'crux.kv.memdb/kv
                 :crux.kv/db-dir "data/db-dir-mem"
                 :crux.standalone/event-log-kv-store 'crux.kv.memdb/kv
                 :crux.standalone/event-log-dir "data/eventlog-mem"}))

(c/submit-tx node-mem [[:crux.tx/put {:crux.db/id {:a 1
                                                   :b 1}}]])

(c/q (c/db node-mem)
     {:find ['q]
      :where [['q :crux.db/id '_]]
      :full-results? true})

(c/entity (c/db node-mem)
          {:b 1 :a 1})

(.close node-mem)

(defn sha1-str [s]
  (->> (-> "sha1"
           java.security.MessageDigest/getInstance
           (.digest (.getBytes s)))
       (map #(.substring
              (Integer/toString
               (+ (bit-and % 0xff) 0x100) 16) 1))
       (apply str)))
