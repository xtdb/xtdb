(ns tmt.hash_map
  (:require [taoensso.nippy :as nippy]
            [crux.api :as api]
            [crux.hash :as hash]
            [crux.codec :as codec]
            [clojure.core.async :as async]))


(defn rand-str [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 65))))))

(defn rand-map [len]
  (into (hash-map) (take len (repeatedly (fn [] [(keyword (rand-str 10)) (keyword (rand-str 10))])))))

(def node (api/start-node {:crux.node/topology :crux.standalone/topology
                           :crux.node/kv-store "crux.kv.lmdb/kv"
                           :crux.kv/db-dir "data/db-dir-1"
                           :crux.standalone/event-log-dir "data/eventlog-1"
                           :crux.standalone/event-log-kv-store "crux.kv.lmdb/kv"}))

(def map-id {:QRGGYDOUXN :YEEWHBVFBF
             :SWPAKPXXDP :CZZQJXSSEP
             :OFGBMBKTFB :RUMBXIQJJO
             :SAYQXOOIDW :PMBGJFXKXB
             :CHFNUWLFJD :FMGJQABHPE
             :DEYBVUPPNU :KPDHWFGQUR
             :WEYYOYDHJI :CAOZEORBMH
             :PAYCSXWFHQ :DAYAWTUAGP
             :IPVSCPSEZS :JCZJKYPHFG
             :ECLHMXYOFU :LFZBLDOIOT})

(api/submit-tx node [[:crux.tx/put {:crux.db/id map-id :val :thing}]])

;; THIS PASSES???
(api/q (api/db node) {:find ['v] :where [['e :val 'v]
                                         ['e :crux.db/id {:QRGGYDOUXN :YEEWHBVFBF
                                                          :SWPAKPXXDP :CZZQJXSSEP
                                                          :OFGBMBKTFB :RUMBXIQJJO
                                                          :SAYQXOOIDW :PMBGJFXKXB
                                                          :CHFNUWLFJD :FMGJQABHPE
                                                          :DEYBVUPPNU :KPDHWFGQUR
                                                          :WEYYOYDHJI :CAOZEORBMH
                                                          :PAYCSXWFHQ :DAYAWTUAGP
                                                          :IPVSCPSEZS :JCZJKYPHFG
                                                          :ECLHMXYOFU :LFZBLDOIOT}]]})
(api/q (api/db node) {:find ['v] :where [['e :val 'v]
                                         ['e :crux.db/id {:SWPAKPXXDP :CZZQJXSSEP
                                                          :QRGGYDOUXN :YEEWHBVFBF
                                                          :CHFNUWLFJD :FMGJQABHPE
                                                          :SAYQXOOIDW :PMBGJFXKXB
                                                          :OFGBMBKTFB :RUMBXIQJJO
                                                          :DEYBVUPPNU :KPDHWFGQUR
                                                          :WEYYOYDHJI :CAOZEORBMH
                                                          :PAYCSXWFHQ :DAYAWTUAGP
                                                          :IPVSCPSEZS :JCZJKYPHFG
                                                          :ECLHMXYOFU :LFZBLDOIOT}]]})

;; ????????????????????
(api/entity (api/db node) {:SWPAKPXXDP :CZZQJXSSEP
                           :QRGGYDOUXN :YEEWHBVFBF
                           :CHFNUWLFJD :FMGJQABHPE
                           :SAYQXOOIDW :PMBGJFXKXB
                           :OFGBMBKTFB :RUMBXIQJJO
                           :DEYBVUPPNU :KPDHWFGQUR
                           :WEYYOYDHJI :CAOZEORBMH
                           :PAYCSXWFHQ :DAYAWTUAGP
                           :IPVSCPSEZS :JCZJKYPHFG
                           :ECLHMXYOFU :LFZBLDOIOT})

(api/submit-tx node [[:crux.tx/put {:crux.db/id {:a :aa :b :bb :c :cc :d :dd :e :ee :f :ff :g :gg :h :hh :i :ii} :val :thing}]])
(api/entity (api/db node) {:b :bb :a :aa :c :cc :d :dd :e :ee :f :ff :g :gg :h :hh :i :ii})

(clojure.lang.PersistentHashMap/create {:b :bb :a :aa :c :cc :d :dd :e :ee :f :ff :g :gg :h :hh})

(.close node)

(defn get-time
  [expr]
  (let [start# (. System (nanoTime))
        ret (doall (expr))]
    (/ (double (- (. System (nanoTime)) start#)) 1000000.0)))

(defn bench
  [n size f]
  (let [maps (take n (repeatedly #(rand-map size)))]
    (get-time #(map f maps))))

;; Sort looks better
(/ (reduce + (doall (take 30 (repeatedly #(bench 20000 9 sort))))) 30)
(/ (reduce + (doall (take 30 (repeatedly #(bench 20000 9 (fn [e] (clojure.lang.PersistentHashMap/create e))))))) 30)

