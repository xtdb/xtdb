(ns juxt.rocks
  (:require [taoensso.nippy :as nippy])
  (:import [org.rocksdb RocksDB Options]))

(defn ->bytes [v]
  ;;(nippy/freeze v)
  (.getBytes v java.nio.charset.StandardCharsets/UTF_8))

(defn bytes-> [b]
  ;;(nippy/thaw b)
  (String. b java.nio.charset.StandardCharsets/UTF_8))

(defn open-db []
  ;; Open database
  (RocksDB/loadLibrary)
  (let [opts (doto (Options.)
               (.setCreateIfMissing true))]
    (RocksDB/open opts "/tmp/db.foo")))

(defn -get [db k]
  (some-> db (.get (->bytes k)) bytes->))

(defn -put
  ([db k v]
   (-put db k v (java.util.Date.)))
  ([db k v ts]
   (.put db (->bytes (str (name k) "-" (.getTime ts))) (->bytes v))))

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
       (.seekForPrev i (->bytes (str k "-" (.getTime ts))))
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
    (def c (open-db))
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
