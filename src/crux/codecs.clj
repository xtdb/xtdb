(ns crux.codecs
  (:require [crux.byte-utils :refer [md5 to-byte-array]])
  (:import [java.nio ByteBuffer]))

;; TODO, consider direct byte-buffer try

(defmacro defenum [name & vals]
  `(def ~name [~(into {} (map-indexed (fn [i v] [v (byte i)]) vals))
               ~(into {} (map-indexed (fn [i v] [(byte i) v]) vals))]))

(def binary-types {:int32 [4 '.putInt '.getInt nil nil]
                   :md5 [16 '.put
                         (fn [^ByteBuffer b] (.get b (byte-array 16)))
                         (fn [x] (-> x to-byte-array md5)) identity]})

(defprotocol Codec
  (encode [this v])
  (decode [this v])
  (length [this]))

(defn encode-form
  "Produce a form to perform encoding based on a given type."
  [k t]
  (if (symbol? t)
    ;; handle enum
    `(.put ~'b (get (first ~t) (get ~'v ~k)))
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

(defmacro defframe [name & args]
  (let [pairs# (partition 2 args)
        size# (->> pairs#
                   (map second)
                   (map (fn [f] (cond (satisfies? Codec f)
                                      (length f)

                                      (symbol? f) ;; enum
                                      1

                                      :else
                                      (-> f binary-types first))))
                   (reduce +))]
    `(def ~name
       (reify Codec
         (encode [_ ~'v]
           (let [~'b (ByteBuffer/allocate ~size#)]
             ~@(->> pairs#
                    (map (fn [[k# t#]]
                           (encode-form k# t#))))))
         (decode [_ #^bytes ~'v]
           (let [~'b (ByteBuffer/wrap ~'v)]
             (-> {}
                 ~@(->> pairs#
                        (map (fn [[k# t#]]
                               `(assoc ~k# ~(decode-form t#))))))))
         (length [_]
           ~size#)))))

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
           (decode ~'codec ~'v)))
       (length [_]
         ))))

;;(g/compile-frame (apply g/enum :byte [1 2 3 4]))
;; why are enums needed?
;;

(comment
  ;; For developing:
  (macroexpand '(defframe foo :foo :md5))
  (defframe testframe :a :int32 :b :int32))
