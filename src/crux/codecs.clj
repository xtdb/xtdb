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

(def binary-types {:int32 [4 #(.putInt ^ByteBuffer %1 %2) #(.getInt ^ByteBuffer %)]
                   :id [4 #(.putInt ^ByteBuffer %1 %2) #(.getInt ^ByteBuffer %)]
                   :reverse-ts [8 (fn [^ByteBuffer b x] (.putLong b (- max-timestamp x))) #(.getLong ^ByteBuffer %)]
                   :string [(fn [^String s] (alength (.getBytes s))) encode-string decode-string]
;;                   :keyword [4 '.putInt '.getInt]
                   :md5 [16
                         (fn [b x] (encode-bytes b (-> x to-byte-array md5)))
                         (fn [^ByteBuffer b] (.get b (byte-array 16)))]})

(defn- frame-length-fn [frame]
  (let [sizes (->> frame
                   (partition 2)
                   (map (fn [[k f]]
                          (if (vector? f)
                            1
                            (let [size-or-fn (-> f binary-types first)]
                              (if (fn? size-or-fn)
                                (fn [v] (size-or-fn (get v k)))
                                size-or-fn))))))]
    (fn [v]
      (reduce + (map (fn [size-or-fn] (if (fn? size-or-fn)
                                        (size-or-fn v) size-or-fn)) sizes)))))

(defprotocol Codec
  (encode [this v])
  (decode [this v])
  (length [this v]))

(defn compile-frame [& args]
  (let [length-fn (frame-length-fn args)
        pairs (->> args
                   (partition 2))]
    (reify Codec
      (encode [this m]
        (let [b (ByteBuffer/allocate (length-fn m))]
          (doseq [[k t] pairs
                  :let [v (get m k)]]
            (if (vector? t)
              (.put b ^Byte (get (first t) v))
              (let [[_ f] (get binary-types t)]
                (f b v))))
          b))
      (decode [_ v]
        (let [b (ByteBuffer/wrap #^bytes v)]
          (into {}
                (for [[k t] pairs]
                  [k (if (vector? t)
                       (get (second t) (.get b))
                       (let [[_ _ f] (get binary-types t)]
                         (f b)))])))))))

(defn compile-header-frame [[header-k header-frame] frames]
  (reify Codec
    (encode [_ v]
      (let [frame (get frames (get v header-k))]
        (encode frame v)))
    (decode [_ v]
      ;; Todo could perhaps eliminate the double read of the prefix
      (let [b (ByteBuffer/wrap v)
            [_ _ f] (get binary-types header-frame)
            codec (get frames (f b))]
        (decode codec v)))))

(defn compile-enum [& vals]
  [(into {} (map-indexed (fn [i v] [v (byte i)]) vals))
   (into {} (map-indexed (fn [i v] [(byte i) v]) vals))])
