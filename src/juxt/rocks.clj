(ns juxt.rocks
  (:require [taoensso.nippy :as nippy])
  (:import [org.rocksdb RocksDB Options]))

;; The single, unconfigurable schema to rule them all..
(def schema {:foo 1})

(defn ->bytes [v]
  ;;(nippy/freeze v)
  (.getBytes v java.nio.charset.StandardCharsets/UTF_8))

(defn bytes-> [b]
  ;;(nippy/thaw b)
  (String. b java.nio.charset.StandardCharsets/UTF_8))

(defn- make-key [k ts]
  (let [a-id (schema k)]
    (byte-array (mapcat seq [(.toByteArray (biginteger a-id)) (.toByteArray (biginteger (.getTime ts)))]))))

(defn open-db [db-name]
  ;; Open database
  (RocksDB/loadLibrary)
  (let [opts (doto (Options.)
               (.setCreateIfMissing true))]
    (RocksDB/open opts (str "/tmp/" (name db-name) ".db"))))

(defn -put
  ([db k v]
   (-put db k v (java.util.Date.)))
  ([db k v ts]
   (.put db (make-key k ts) (->bytes v))))

(defn rocks-iterator->seq [i]
  (lazy-seq
   (when (.isValid i)
     (let [k (.key i)
           v (.value i)]
       (.next i)
       (cons [(bytes-> k) (bytes-> v)] (rocks-iterator->seq i))))))

;; Some prefix thing needed?
(defn -get-at
  ([c k] (-get-at c k (java.util.Date.)))
  ([c k ts]
   (let [i (.newIterator c)]
     (try
       (.seekForPrev i (make-key k ts))
       (when (.isValid i)
         (bytes-> (.value i)))
       (finally
         (.close i))))))

(comment
  (defn print-all []
    (let [i (.newIterator c)]
      (try
        (.seekToFirst i)
        (doseq [v (rocks-iterator->seq i)]
          (println v))
        (finally
          (.close i))))))

(comment
  (do
    (def c (open-db "comment"))
    (-put c "Foo" "Bar4")
    (-put c "Foo" "Bar5")
    (-put c "Foo" "Bar6")
    (-put c "Tar" "Tar4")
    (-put c "Tar" "Tar5")
    (-put c "Tar" "Tar6")
    (-put c "Tar" "Tar7")
    (-put c "Tar" "Tar8")
    (-get c "Foo")
    (-get-at c "Foo")
    (-put c "Foo" "Bar7")
    (-put c "Foo" "Bar8")
    (-get-at c "Foo")
    (-get-at c "Tar"))
  (.close c))
