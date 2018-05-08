(ns crux.codecs
  (:require [crux.byte-utils :refer [md5 to-byte-array]])
  (:import [java.nio ByteBuffer]))

(def max-timestamp (.getTime #inst "9999-12-30"))

(defn- encode-bytes [^ByteBuffer bb #^bytes bs]
  (.put bb bs))

(defn encode-string [^ByteBuffer bb ^String s]
  (.put bb #^bytes (.getBytes s)))

(defn decode-string [^ByteBuffer bb]
  (String. (.array (.get bb (byte-array (.remaining bb))))))

(defn- keyword->string [k]
  (-> k str (subs 1)))

(def binary-types {:int32 [(constantly 4) #(.putInt ^ByteBuffer %1 %2) #(.getInt ^ByteBuffer %)]
                   :id [(constantly 4) #(.putInt ^ByteBuffer %1 %2) #(.getInt ^ByteBuffer %)]
                   :reverse-ts [(constantly 8) (fn [^ByteBuffer b x] (.putLong b (- max-timestamp x))) #(.getLong ^ByteBuffer %)]
                   :string [(fn [^String s] (alength (.getBytes s))) encode-string decode-string]
                   :keyword [(fn [k] (alength (.getBytes ^String (keyword->string k))))
                             (fn [^ByteBuffer bb k] (encode-string bb (keyword->string k)))
                             #(-> % decode-string keyword)]
                   :md5 [(constantly 16)
                         (fn [b x] (encode-bytes b (-> x to-byte-array md5)))
                         (fn [^ByteBuffer b] (.get b (byte-array 16)))]})

(defprotocol Codec
  (encode [this v])
  (decode [this v]))

(defn- resolve-data-type [t]
  (or (and (vector? t) t)
      (get binary-types t)))

(defn compile-frame [& args]
  (let [length-fn (frame-length-fn args)
        pairs (->> args
                   (partition 2)
                   (map (fn [[k t]]
                          (let [datatype (resolve-data-type t)]
                            (when-not datatype
                              (throw (IllegalArgumentException. (str "Unknown datatype: " t))))
                            [k datatype]))))]
    (reify Codec
      (encode [this m]
        (let [length (reduce + (map (fn [[k [length-f]]] (length-f (get m k))) pairs))
              b (ByteBuffer/allocate length)]
          (doseq [[k [_ f]] pairs
                  :let [v (get m k)]]
            (f b v))
          b))
      (decode [_ v]
        (let [b (ByteBuffer/wrap #^bytes v)]
          (into {}
                (for [[k [_ _ f]] pairs]
                  [k (f b)])))))))

(defn compile-header-frame [[header-k header-frame] frames]
  (reify Codec
    (encode [_ v]
      (let [frame (get frames (get v header-k))]
        (encode frame v)))
    (decode [_ v]
      ;; Todo could perhaps eliminate the double read of the prefix
      (let [b (ByteBuffer/wrap v)
            [_ _ f] (if (vector? header-frame)
                      header-frame
                      (get binary-types header-frame))
            codec (get frames (f b))]
        (decode codec v)))))

(defn compile-enum [& vals]
  (let [keyword->vals (into {} (map-indexed (fn [i v] [v (byte i)]) vals))
        vals->keyword (into {} (map-indexed (fn [i v] [(byte i) v]) vals))]
    [(constantly 1)
     (fn [^ByteBuffer bb k] (.put bb ^Byte (get keyword->vals k)))
     (fn [^ByteBuffer bb] (get vals->keyword (.get bb)))]))
