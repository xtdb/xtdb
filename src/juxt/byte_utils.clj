(ns juxt.byte-utils)

(defn hash-keyword [k]
  (hash (str (namespace k) (name k))))

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
