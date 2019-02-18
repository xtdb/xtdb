(ns crux.morton)

(def ^:const use-space-filling-curve-index? (Boolean/parseBoolean (System/getenv "CRUX_SPACE_FILLING_CURVE_INDEX")))

(defn- long->binary-str [^long x]
  (.replace (format "%64s" (Long/toBinaryString x)) \space \0))

(defn- unsigned-long->biginteger ^java.math.BigInteger [^long x]
  (cond-> (biginteger (bit-and x Long/MAX_VALUE))
    (neg? x) (.setBit (dec Long/SIZE))))

;; http://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
(defn- bit-spread-int ^long [^long x]
  (let [x (bit-and (bit-or x (bit-shift-left x 16)) 0x0000ffff0000ffff)
        x (bit-and (bit-or x (bit-shift-left x 8)) 0x00ff00ff00ff00ff)
        x (bit-and (bit-or x (bit-shift-left x 4)) 0x0f0f0f0f0f0f0f0f)
        x (bit-and (bit-or x (bit-shift-left x 2)) 0x3333333333333333)
        x (bit-and (bit-or x (bit-shift-left x 1)) 0x5555555555555555)]
    x))

(defn- bit-unspread-int ^long [^long x]
  (let [x (bit-and x 0x5555555555555555)
        x (bit-and (bit-or x (bit-shift-right x 1)) 0x3333333333333333)
        x (bit-and (bit-or x (bit-shift-right x 2)) 0x0f0f0f0f0f0f0f0f)
        x (bit-and (bit-or x (bit-shift-right x 4)) 0x00ff00ff00ff00ff)
        x (bit-and (bit-or x (bit-shift-right x 8)) 0x0000ffff0000ffff)
        x (bit-and (bit-or x (bit-shift-right x 16)) 0xffffffff)]
    x))

;; NOTE: we're putting x before y dimension here.
(defn- bit-interleave-ints ^long [^long x ^long y]
  (bit-or (bit-shift-left (bit-spread-int (Integer/toUnsignedLong (unchecked-int x))) 1)
          (bit-spread-int (Integer/toUnsignedLong (unchecked-int y)))))

(defn- bit-uninterleave-ints [^long x]
  (int-array [(bit-unspread-int (unsigned-bit-shift-right x 1))
              (bit-unspread-int x)]))

(defn interleaved-longs->morton-number ^java.math.BigInteger [^long upper ^long lower]
  (.or (.shiftLeft ^BigInteger (unsigned-long->biginteger upper) Long/SIZE)
       ^BigInteger (unsigned-long->biginteger lower)))

(defn longs->morton-number-parts [^long x ^long y]
  (let [lower (bit-interleave-ints x y)
        upper (bit-interleave-ints (unsigned-bit-shift-right x Integer/SIZE)
                                   (unsigned-bit-shift-right y Integer/SIZE))]
    [upper lower]))

(defn longs->morton-number ^java.math.BigInteger [^long x ^long y]
  (let [[upper lower] (longs->morton-number-parts x y)]
    (interleaved-longs->morton-number upper lower)))

(defn morton-number->interleaved-longs [^BigInteger z]
  [(.longValue (.shiftRight z Long/SIZE)) (.longValue z)])

(defn morton-number->longs [^Number z]
  (let [z (biginteger z)
        lower ^ints (bit-uninterleave-ints (.longValue z))
        upper ^ints (bit-uninterleave-ints (.longValue (.shiftRight z Long/SIZE)))]
    [(bit-or (bit-shift-left (Integer/toUnsignedLong (aget upper 0)) Integer/SIZE)
             (Integer/toUnsignedLong (aget lower 0)))
     (bit-or (bit-shift-left (Integer/toUnsignedLong (aget upper 1)) Integer/SIZE)
             (Integer/toUnsignedLong (aget lower 1)))]))

(def ^:private ^BigInteger morton-2-x-mask (biginteger 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa))

(defn morton-number-within-range? [^Number min ^Number max ^Number z]
  (let [min (biginteger min)
        max (biginteger max)
        z (biginteger z)
        zx (.and z morton-2-x-mask)
        zy (.and (.shiftLeft z 1) morton-2-x-mask)]
    (and (not (pos? (.compareTo (.and min morton-2-x-mask) zx)))
         (not (pos? (.compareTo (.and (.shiftLeft min 1) morton-2-x-mask) zy)))
         (not (pos? (.compareTo zx (.and max morton-2-x-mask))))
         (not (pos? (.compareTo zy (.and (.shiftLeft max 1) morton-2-x-mask)))))))

(defn- bit-spread ^BigInteger [^BigInteger x]
  (let [l1 ^BigInteger (unsigned-long->biginteger (bit-spread-int (.intValue x)))
        l2 ^BigInteger (unsigned-long->biginteger (bit-spread-int (.intValue (.shiftRight x Integer/SIZE))))
        l3 ^BigInteger (unsigned-long->biginteger (bit-spread-int (.intValue (.shiftRight x (* 2 Integer/SIZE)))))
        l4 ^BigInteger (unsigned-long->biginteger (bit-spread-int (.intValue (.shiftRight x (* 3 Integer/SIZE)))))]
    (.or (.shiftLeft l4 (* 3 Long/SIZE))
         (.or (.shiftLeft l3 (* 2 Long/SIZE))
              (.or (.shiftLeft l2 Long/SIZE)
                   l1)))))

(defn- bit-unspread ^BigInteger [^BigInteger x]
  (let [l1 ^BigInteger (unsigned-long->biginteger (bit-unspread-int (.longValue x)))
        l2 ^BigInteger (unsigned-long->biginteger (bit-unspread-int (.longValue (.shiftRight x Long/SIZE))))
        l3 ^BigInteger (unsigned-long->biginteger (bit-unspread-int (.longValue (.shiftRight x (* 2 Long/SIZE)))))
        l4 ^BigInteger (unsigned-long->biginteger (bit-unspread-int (.longValue (.shiftRight x (* 3 Long/SIZE)))))]
    (.or (.shiftLeft l4 (* 3 Integer/SIZE))
         (.or (.shiftLeft l3 (* 2 Integer/SIZE))
              (.or (.shiftLeft l2 Integer/SIZE)
                   l1)))))

;; BIGMIN/LITMAX based on
;; https://github.com/locationtech/geotrellis/tree/master/spark/src/main/scala/geotrellis/spark/io/index/zcurve
;; Apache License

;; Ultimately based on this paper and the decision tables on page 76:
;; https://www.vision-tools.com/h-tropf/multidimensionalrangequery.pdf

(def ^BigInteger z-max-mask (biginteger 0xffffffffffffffffffffffffffffffff))
(def ^:const z-max-bits (* 2 Long/SIZE))

(defn- bits-over ^BigInteger [^long x]
  (.shiftLeft BigInteger/ONE (dec x)))

(defn- bits-under ^BigInteger [^long x]
  (.subtract (.shiftLeft BigInteger/ONE (dec x)) BigInteger/ONE))

(defn- zload ^BigInteger [^BigInteger z ^BigInteger load ^long bits ^long dim]
  (let [mask (.not (.shiftLeft (bit-spread (.shiftRight z-max-mask (- z-max-bits bits))) dim))]
    (.or (.and z mask)
         (.shiftLeft (bit-spread load) dim))))

(defn zdiv [^Number start ^Number end ^Number z]
  (let [start (biginteger start)
        end (biginteger end)
        z (biginteger z)]
    (loop [n z-max-bits
           litmax BigInteger/ZERO
           bigmin BigInteger/ZERO
           start start
           end end]
      (cond
        (pos? (compare start end)) ;; TODO: This cannot be right, but kind of works.
        nil

        (zero? n)
        [litmax bigmin]

        :else
        (let [n (dec n)
              bits (inc (unsigned-bit-shift-right n 1))
              dim (bit-and n 1)
              zb (if (.testBit z n) 1 0)
              sb (if (.testBit start n) 1 0)
              eb (if (.testBit end n) 1 0)]
          (cond
            (and (= 0 zb) (= 0 sb) (= 0 eb))
            (recur n litmax bigmin start end)

            (and (= 0 zb) (= 0 sb) (= 1 eb))
            (recur n litmax (zload start (bits-over bits) bits dim) start (zload end (bits-under bits) bits dim))

            (and (= 0 zb) (= 1 sb) (= 0 eb))
            (throw (IllegalStateException. "This case is not possible because MIN <= MAX. (0 1 0)"))

            (and (= 0 zb) (= 1 sb) (= 1 eb))
            [litmax start]

            (and (= 1 zb) (= 0 sb) (= 0 eb))
            [end bigmin]

            (and (= 1 zb) (= 0 sb) (= 1 eb))
            (recur n (zload end (bits-under bits) bits dim) bigmin (zload start (bits-over bits) bits dim) end)

            (and (= 1 zb) (= 1 sb) (= 0 eb))
            (throw (IllegalStateException. "This case is not possible because MIN <= MAX. (1 1 0)"))

            (and (= 1 zb) (= 1 sb) (= 1 eb))
            (recur n litmax bigmin start end)))))))
