(ns crux.codecs
  (:import [java.nio ByteBuffer]))

;; TODO, consider direct byte-buffer try

(def binary-types {:int32 [4 '.putInt '.getInt]})

(defprotocol Codec
  (encode [this v])
  (decode [this v])
  (length [this]))

(defmacro defframe [name & args]
  (let [pairs (partition 2 args)
        size# (->> pairs
                   (map second)
                   (map (fn [f] (if (satisfies? Codec f)
                                  (length f)
                                  (-> f binary-types first))))
                   (reduce +))]
    `(def ~name
       (reify Codec
         (encode [_ ~'v]
           (let [~'b (ByteBuffer/allocate ~size#)]
             ~@(->> pairs
                    (map (fn [[k# t#]]
                           (let [f# (-> t# binary-types second)]
                             `(~f# ~'b (get ~'v ~k#))))))))
         (decode [_ ~'v]
           (let [~'b (ByteBuffer/wrap ~'v)]
             (-> {}
                 ~@(->> pairs
                        (map (fn [[k# t#]]
                               (let [f# (-> t# binary-types (nth 2))]
                                 `(assoc ~k# (~f# ~'b)))))))))
         (length [_]
           ~size#)))))

;;(defframe bar [:a :int32])
(defframe foo :a :int32 :b :int32)
;;(defframe foobar [:int32 bar])

(decode foo (.array #^bytes (encode foo {:a 1 :b 2})))

(macroexpand '(defframe foo [:int32 :int32]))
