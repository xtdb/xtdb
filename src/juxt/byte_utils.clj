(ns juxt.byte-utils)

(defn hash-keyword [k]
  (hash (str (namespace k) (name k))))

(defn str->bytes [v]
  ;;(nippy/freeze v)
  (.getBytes v java.nio.charset.StandardCharsets/UTF_8))

(defn bytes->str [b]
  (String. b java.nio.charset.StandardCharsets/UTF_8))

(defn long->bytes [l]
  (->(-> (java.nio.ByteBuffer/allocate 8)
         (.order (java.nio.ByteOrder/nativeOrder)))
     (.putLong l)
     (.array)))

(defn bytes->long [data]
  (.getLong (doto (-> (java.nio.ByteBuffer/allocate 8)
                      (.order (java.nio.ByteOrder/nativeOrder)))
              (.put data 0 8)
              (.flip))))

(defn bytes->int [data]
  (.getInt (doto (-> (java.nio.ByteBuffer/allocate 4)
                     (.order (java.nio.ByteOrder/nativeOrder)))
             (.put data 0 4)
             (.flip))))

(comment
  (bytes->int (-> (java.nio.ByteBuffer/allocate 4)
                  (.putInt 1)
                  (.array)))

  (biginteger (-> (java.nio.ByteBuffer/allocate 8)
                  (.putLong 1)
                  (.array))))
