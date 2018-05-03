(ns crux.codecs
  (:require [crux.byte-utils :refer [md5 to-byte-array]])
  (:import [java.nio ByteBuffer]))

;; TODO, consider direct byte-buffer try

(def max-timestamp (.getTime #inst "9999-12-30"))

(defn- encode-bytes [^ByteBuffer bb #^bytes bs]
  (.put bb bs))

(defn encode-string [^ByteBuffer bb ^String s]
  (.put bb #^bytes (.getBytes s)))

(defn decode-string [^ByteBuffer bb]
  (String. (.array (.get bb (byte-array (.remaining bb))))))

(def binary-types {:int32 [4 '.putInt '.getInt nil nil]
                   :id [4 '.putInt '.getInt nil nil]
                   :reverse-ts [8 '.putLong '.getLong (partial - max-timestamp) identity]
                   :string [(fn [^String s] (alength (.getBytes s))) encode-string decode-string nil nil]
;;                   :keyword [4 '.putInt '.getInt nil nil]
                   :md5 [16 encode-bytes
                         (fn [^ByteBuffer b] (.get b (byte-array 16)))
                         (fn [x] (-> x to-byte-array md5)) nil]})

(defprotocol Codec
  (encode [this v])
  (decode [this v])
  (length [this v]))

(defmacro defenum [name & vals]
  `(def ~name [~(into {} (map-indexed (fn [i v] [v (byte i)]) vals))
               ~(into {} (map-indexed (fn [i v] [(byte i) v]) vals))]))

(defn encode-form
  "Produce a form to perform encoding based on a given type."
  [k t]
  (if (symbol? t)
    ;; handle enum
    `(.put ~'b ^Byte (get (first ~t) (get ~'v ~k)))
    (let [[_ f _ enc] (get binary-types t)]
      (if enc
        `(~f ~'b (~enc (get ~'v ~k)))
        `(~f ~'b (get ~'v ~k))))))

(defn decode-form
  "Produce a form to perform encoding based on a given type."
  [t]
  (if (symbol? t)
    `(get (second ~t) (.get ~'b))
    (let [[_ _ f _ dec] (get binary-types t)]
      (if dec
        `(~dec (~f ~'b))
        `(~f ~'b)))))

(defn frame-length-fn [frame]
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

(defmacro defframe [name & args]
  `(let [length-fn# (frame-length-fn ~(vec args))]
     (def ~name
       (reify Codec
         (encode [~'this ~'v]
           (let [~'b (ByteBuffer/allocate (length-fn# ~'v))]
             ~@(->> args
                    (partition 2)
                    (map (fn [[k# t#]]
                           (encode-form k# t#))))))
         (decode [_ #^bytes ~'v]
           (let [~'b (ByteBuffer/wrap ~'v)]
             (-> {}
                 ~@(->> args
                        (partition 2)
                        (map (fn [[k# t#]]
                               `(assoc ~k# ~(decode-form t#))))))))))))

(defmacro defprefixedframe [name [header-k header-frame] frames]
  `(def ~name
     (reify Codec
       (encode [_ ~'v]
         (let [~'codec (get ~frames (get ~'v ~header-k))]
           (encode ~'codec ~'v)))
       (decode [_ ~'v]
         ;; Todo could perhaps eliminate the double read of the prefix
         (let [~'b (ByteBuffer/wrap ~'v)
               ~'codec (get ~frames ~(decode-form header-frame))]
           (decode ~'codec ~'v))))))

(comment
  ;; For developing:
  (macroexpand '(defframe foo :foo :md5))
  (macroexpand '(defframe foostring :a :string))
  (defframe testframe :a :int32 :b :int32))
