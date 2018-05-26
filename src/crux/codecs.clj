(ns crux.codecs
  (:require [crux.byte-utils :as bu]
            [taoensso.nippy :as nippy])
  (:import java.nio.ByteBuffer
           java.util.Date))

(set! *unchecked-math* :warn-on-boxed)

(def ^{:tag 'long}
  max-timestamp (.getTime #inst "9999-12-30"))

(defn- encode-string [^ByteBuffer bb ^String s]
  (.put bb ^bytes (.getBytes s)))

(defn- decode-string ^String [^ByteBuffer bb]
  (let [ba (byte-array (.remaining bb))]
    (.get bb ba)
    (String. ba)))

(defn- keyword->string [k]
  (-> k str (subs 1)))

(def binary-types {:id [4 #(.putInt ^ByteBuffer %1 %2) #(.getInt ^ByteBuffer %)]
                   :reverse-ts [8
                                (fn [^ByteBuffer b ^Date x] (.putLong b (- max-timestamp (.getTime x))))
                                #(Date. (- max-timestamp (.getLong ^ByteBuffer %)))]
                   :keyword [(fn [k] (alength (.getBytes ^String (keyword->string k))))
                             (fn [^ByteBuffer bb k] (encode-string bb (keyword->string k)))
                             #(-> % decode-string keyword)]
                   :md5 [16
                         (fn [^ByteBuffer b x]
                           (let [x (bu/md5 (nippy/freeze x))]
                             (.put b x)))
                         ;; Simply return the md5
                         (fn [^ByteBuffer b] (.get b (byte-array 16)))]
                   :long [8
                          #(.putLong ^ByteBuffer %1 (bit-xor ^long %2 Long/MIN_VALUE))
                          #(bit-xor Long/MIN_VALUE (.getLong ^ByteBuffer %))]
                   :double [8
                            (fn [^ByteBuffer bb d]
                              (let [l (Double/doubleToLongBits d)
                                    l (inc (bit-xor l (bit-or (bit-shift-right l (dec Long/SIZE)) Long/MIN_VALUE)))]
                                (.putLong bb l)))
                            (fn [^ByteBuffer bb]
                              (let [l (dec (.getLong bb))
                                    l (bit-xor l (bit-or (bit-shift-right (bit-not l) (dec Long/SIZE)) Long/MIN_VALUE))]
                                (Double/longBitsToDouble l)))]
                   :string [(fn [^String s] (alength (.getBytes s))) encode-string decode-string]})

(defn variable-data-type [& data-types]
  (let [data-type->byte (into {} (for [[i k] (map-indexed vector data-types)]
                                   [k (byte i)]))
        byte->data-type (into {} (for [[i k] (map-indexed vector data-types)]
                                   [(byte i) k]))]
    [(fn [[data-type v]]
       (inc ^long (first (get binary-types data-type))))

     (fn [^ByteBuffer bb [data-type v]]
       (.put bb ^byte (data-type->byte data-type))
       ((second (get binary-types data-type)) bb v))

     (fn [^ByteBuffer bb]
       (let [data-type ^byte (.get bb)]
         ((nth (get binary-types (byte->data-type data-type)) 2) bb)))]))

(defprotocol Codec
  (encode [this v])
  (decode [this v]))

(defn- resolve-data-type [t]
  (or (and (vector? t) t)
      (get binary-types t)))

(defn compile-frame [& args]
  (let [pairs (->> args
                   (partition 2)
                   (mapv (fn [[k t]]
                           (let [datatype (resolve-data-type t)]
                             (when-not datatype
                               (throw (IllegalArgumentException. (str "Unknown datatype: " t))))
                             [k datatype]))))
        fixed-length (long (reduce + (for [[k [length]] pairs :when (int? length)]
                                       length)))
        length-fn (first (for [[k [length-f]] pairs :when (fn? length-f)]
                           (fn ^long [m] (length-f (get m k)))))]
    (reify Codec
      (encode [this m]
        (let [length (if length-fn (+ fixed-length ^long (length-fn m)) fixed-length)
              b (ByteBuffer/allocate length)]
          (doseq [[k [_ f]] pairs
                  :let [v (get m k)]]
            (f b v))
          b))
      (decode [_ v]
        (let [b (ByteBuffer/wrap ^bytes v)
              m (transient {})]
          (->> pairs
               (reduce
                (fn [m [k [_ _ f]]]
                  (assoc! m k (f b)))
                (transient {}))
               (persistent! )))))))

(defmacro compile-enum [& vals]
  `[1
    (fn [^ByteBuffer bb# ~'k]
      (let [v# (case ~'k
                 ~@(flatten (for [[i k] (map-indexed vector vals)]
                              [k (byte i)])))]
        (assert v#)
        (.put bb# ^Byte v#)))
    (fn [^ByteBuffer bb#]
      (let [k# (case (.get bb#)
                 ~@(flatten (for [[i k] (map-indexed vector vals)]
                              [(byte i) k])))]
        (assert k#)
        k#))])
