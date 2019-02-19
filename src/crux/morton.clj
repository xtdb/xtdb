(ns crux.morton
  (:import crux.morton.UInt128))

;; NOTE: Many papers use y/x order for coordinates, so some tests are
;; a bit confusing. We, and the original paper use x/y:
;; https://www.vision-tools.com/h-tropf/multidimensionalrangequery.pdf
;; Internally its first and second dimension, x and y are just names
;; for these which may change depending on context.

(def ^:const use-space-filling-curve-index? (Boolean/parseBoolean (System/getenv "CRUX_SPACE_FILLING_CURVE_INDEX")))

(set! *unchecked-math* :warn-on-boxed)

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

(defn- bit-interleave-ints ^long [^long d1 ^long d2]
  (bit-or (bit-shift-left (bit-spread-int (Integer/toUnsignedLong (unchecked-int d1))) 1)
          (bit-spread-int (Integer/toUnsignedLong (unchecked-int d2)))))

(defn- bit-uninterleave-ints [^long x]
  (int-array [(bit-unspread-int (unsigned-bit-shift-right x 1))
              (bit-unspread-int x)]))

(defn interleaved-longs->morton-number ^crux.morton.UInt128 [^long upper ^long lower]
  (UInt128. upper lower))

(defn longs->morton-number-parts [^long d1 ^long d2]
  (let [lower (bit-interleave-ints d1 d2)
        upper (bit-interleave-ints (unsigned-bit-shift-right d1 Integer/SIZE)
                                   (unsigned-bit-shift-right d2 Integer/SIZE))]
    [upper lower]))

(defn longs->morton-number ^crux.morton.UInt128 [^long d1 ^long d2]
  (let [[upper lower] (longs->morton-number-parts d1 d2)]
    (interleaved-longs->morton-number upper lower)))

(defn morton-number->interleaved-longs [^UInt128 z]
  [(.upper z) (.lower z)])

(defn morton-number->longs [^Number z]
  (let [z (UInt128/fromNumber z)
        lower ^ints (bit-uninterleave-ints (.lower z))
        upper ^ints (bit-uninterleave-ints (.upper z))]
    [(bit-or (bit-shift-left (Integer/toUnsignedLong (aget upper 0)) Integer/SIZE)
             (Integer/toUnsignedLong (aget lower 0)))
     (bit-or (bit-shift-left (Integer/toUnsignedLong (aget upper 1)) Integer/SIZE)
             (Integer/toUnsignedLong (aget lower 1)))]))

(def ^:private ^UInt128 morton-d1-mask (UInt128/fromBigInteger (biginteger 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa)))
(def ^:private ^UInt128 morton-d2-mask (UInt128/fromBigInteger (biginteger 0x55555555555555555555555555555555)))

(defn morton-number-within-range? [^Number min ^Number max ^Number z]
  (let [min (UInt128/fromNumber min)
        max (UInt128/fromNumber max)
        z (UInt128/fromNumber z)
        z-d1 (.and z morton-d1-mask)
        z-d2 (.and z morton-d2-mask)]
    (and (not (pos? (.compareTo (.and min morton-d1-mask) z-d1)))
         (not (pos? (.compareTo (.and min morton-d2-mask) z-d2)))
         (not (pos? (.compareTo z-d1 (.and max morton-d1-mask))))
         (not (pos? (.compareTo z-d2 (.and max morton-d2-mask)))))))

;; BIGMIN/LITMAX based on
;; https://github.com/locationtech/geotrellis/tree/master/spark/src/main/scala/geotrellis/spark/io/index/zcurve
;; Apache License

;; Ultimately based on this paper and the decision tables on page 76:
;; https://www.vision-tools.com/h-tropf/multidimensionalrangequery.pdf

(def ^UInt128 z-max-mask UInt128/MAX)
(def ^UInt128 z-max-mask-spread (UInt128/fromBigInteger (biginteger 0x55555555555555555555555555555555)))
(def ^:const z-max-bits UInt128/SIZE)

(defn- bits-over-spread ^UInt128 [^long x]
  (.shiftLeft UInt128/ONE (bit-shift-left (dec x) 1)))

(defn- bits-under-spread ^UInt128 [^long x]
  (.and z-max-mask-spread (.dec (.shiftLeft UInt128/ONE (bit-shift-left (dec x) 1)))))

(defn- zload ^UInt128 [^UInt128 z ^UInt128 load ^long bits ^long dim]
  (let [mask (.not (.shiftLeft (.shiftRight z-max-mask-spread (- z-max-bits (bit-shift-left bits 1))) dim))]
    (.or (.and z mask)
         (.shiftLeft load dim))))

(defn zdiv [^Number start ^Number end ^Number z]
  (let [start (UInt128/fromNumber start)
        end (UInt128/fromNumber end)
        z (UInt128/fromNumber z)]
    (loop [n z-max-bits
           litmax UInt128/ZERO
           bigmin UInt128/ZERO
           start start
           end end]
      (if (zero? n)
        [litmax bigmin]
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
            (recur n litmax (zload start (bits-over-spread bits) bits dim) start (zload end (bits-under-spread bits) bits dim))

            (and (= 0 zb) (= 1 sb) (= 0 eb))
            (throw (IllegalStateException. "This case is not possible because MIN <= MAX. (0 1 0)"))

            (and (= 0 zb) (= 1 sb) (= 1 eb))
            [litmax start]

            (and (= 1 zb) (= 0 sb) (= 0 eb))
            [end bigmin]

            (and (= 1 zb) (= 0 sb) (= 1 eb))
            (recur n (zload end (bits-under-spread bits) bits dim) bigmin (zload start (bits-over-spread bits) bits dim) end)

            (and (= 1 zb) (= 1 sb) (= 0 eb))
            (throw (IllegalStateException. "This case is not possible because MIN <= MAX. (1 1 0)"))

            (and (= 1 zb) (= 1 sb) (= 1 eb))
            (recur n litmax bigmin start end)))))))
