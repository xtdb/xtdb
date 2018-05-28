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

(defn- value->binary-type
  "Determine the binary type to use for the value passed in, for
  subsequent encoding/decoding purposes."
  [v]
  (cond (integer? v) :long
        (double? v) :double
        :else :md5))

(def tag-binary-type (into {} (for [[i k] (map-indexed vector (keys binary-types))] [k (byte i)])))
(def untag-binary-type (into {} (for [[i k] (map-indexed vector (keys binary-types))] [(byte i) k])))

(def all-types (assoc binary-types
                      :tagged [(fn [v]
                                 (inc ^long (first (binary-types (value->binary-type v)))))
                               (fn [^ByteBuffer bb v]
                                 (let [binary-type (value->binary-type v)
                                       [_ encoder] (binary-types (value->binary-type v))]
                                   (.put bb ^byte (tag-binary-type binary-type))
                                   (encoder bb v)))
                               (fn [^ByteBuffer bb]
                                 (let [binary-type ^byte (.get bb)
                                       [_ _ decoder] (binary-types (untag-binary-type binary-type))]
                                   (decoder bb)))]))

(defn- resolve-data-type [t]
  (or (and (vector? t) t)
      (get all-types t)))

(defn frame [& args]
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
    [(fn [m]
       (if length-fn (+ fixed-length ^long (length-fn m)) fixed-length))
     (fn [^ByteBuffer b m]
       (doseq [[k [_ f]] pairs
               :let [v (get m k)]]
         (f b v))
       b)
     (fn [^ByteBuffer b]
       (let [m (transient {})]
         (->> pairs
              (reduce
               (fn [m [k [_ _ f]]]
                 (assoc! m k (f b)))
               (transient {}))
              (persistent!))))]))

(defn encode [frame v]
  (let [[length-fn encoder] frame
        bb (ByteBuffer/allocate (length-fn v))]
    (encoder bb v)
    (.array bb)))

(defn decode [frame v]
  (let [[_ _ decoder] frame]
    (decoder (ByteBuffer/wrap ^bytes v))))

(defmacro enum [& vals]
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
