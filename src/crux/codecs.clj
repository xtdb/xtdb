(ns crux.codecs
  (:require [crux.byte-utils :refer [md5 to-byte-array]])
  (:import [java.nio ByteBuffer]))

;; TODO, consider direct byte-buffer try

(def binary-types {:int32 [4 '.putInt '.getInt nil nil]
                   :md5 [16 '.put (fn [b] (.get b (byte-array 16))) (fn [x] (-> x to-byte-array md5)) identity]})

(defprotocol Codec
  (encode [this v])
  (decode [this v])
  (length [this]))

(defmacro defframe [name & args]
  (let [pairs# (partition 2 args)
        size# (->> pairs#
                   (map second)
                   (map (fn [f] (if (satisfies? Codec f)
                                  (length f)
                                  (-> f binary-types first))))
                   (reduce +))]
    `(def ~name
       (reify Codec
         (encode [_ ~'v]
           (let [~'b (ByteBuffer/allocate ~size#)]
             ~@(->> pairs#
                    (map (fn [[k# t#]]
                           (let [[_ f# _ enc#] (-> t# binary-types)]
                             (if enc#
                               `(~f# ~'b (~enc# (get ~'v ~k#)))
                               `(~f# ~'b (get ~'v ~k#)))))))))
         (decode [_ ~'v]
           (let [~'b (ByteBuffer/wrap ~'v)]
             (-> {}
                 ~@(->> pairs#
                        (map (fn [[k# t#]]
                               (let [[_ _ f# _ dec#] (-> t# binary-types)]
                                 (if dec#
                                   `(assoc ~k# (~dec# (~f# ~'b)))
                                   `(assoc ~k# (~f# ~'b))))))))))
         (length [_]
           ~size#)))))

;;(defframe bar [:a :int32])
(defframe foo :a :int32 :b :int32)
(defframe md5f :a :int32 :b :md5)
;;(defframe foobar [:int32 bar])

(decode foo (.array #^bytes (encode foo {:a 1 :b 2})))
(decode foo (.array #^bytes (encode md5f {:a 1 :b 2})))

(macroexpand '(defframe foo :foo :md5))
