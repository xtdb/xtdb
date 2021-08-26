(ns ^:no-doc crux.morton
  (:import crux.morton.UInt128))

;; Start here:
;; https://en.wikipedia.org/wiki/Z-order_curve
;; https://en.wikipedia.org/wiki/Z-order_curve#Use_with_one-dimensional_data_structures_for_range_searching

;; Based on this paper and the decision tables on page 76:
;; https://www.vision-tools.com/h-tropf/multidimensionalrangequery.pdf

;; Further resources:
;; https://github.com/locationtech/geotrellis/tree/master/spark/src/main/scala/geotrellis/spark/io/index/zcurve
;; https://raima.com/wp-content/uploads/COTS_embedded_database_solving_dynamic_pois_2012.pdf
;; https://github.com/hadeaninc/libzinc/blob/master/libzinc/AABB.hh
;; http://cppedinburgh.uk/slides/201603-zcurves.pdf

;; https://aws.amazon.com/blogs/database/z-order-indexing-for-multifaceted-queries-in-amazon-dynamodb-part-1/
;; https://redis.io/topics/indexes#multi-dimensional-indexes
;; http://www.dcs.bbk.ac.uk/~jkl/thesis.pdf

;; NOTE: Many papers use y/x order for coordinates, so some tests are
;; a bit confusing. We, and the original paper use x/y:
;; https://www.vision-tools.com/h-tropf/multidimensionalrangequery.pdf
;; Internally its first and second dimension, x and y are just names
;; for these which may change depending on context.

(def ^:dynamic *use-space-filling-curve-index?* (not (Boolean/parseBoolean (System/getenv "XTDB_DISABLE_SPACE_FILLING_CURVE_INDEX"))))

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

(defn longs->morton-number ^crux.morton.UInt128 [^long d1 ^long d2]
  (let [lower (bit-interleave-ints d1 d2)
        upper (bit-interleave-ints (unsigned-bit-shift-right d1 Integer/SIZE)
                                   (unsigned-bit-shift-right d2 Integer/SIZE))]
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

(def ^UInt128 z-max-mask UInt128/MAX)
(def ^UInt128 z-max-mask-spread (UInt128/fromBigInteger (biginteger 0x55555555555555555555555555555555)))
(def ^:const z-max-bits UInt128/SIZE)

(defrecord MortonRange [litmax bigmin])

(defn- morton-get-next-address-internal ^crux.morton.MortonRange [^UInt128 start ^UInt128 end]
  (let [first-differing-bit (.numberOfLeadingZeros (.xor start end))
        split-dimension (bit-and 1 first-differing-bit)
        dimension-inherit-mask (if (zero? split-dimension)
                                 morton-d2-mask
                                 morton-d1-mask)
        common-most-significant-bits-mask (.shiftLeft UInt128/MAX (- UInt128/SIZE first-differing-bit))
        all-common-bits-mask (.or dimension-inherit-mask common-most-significant-bits-mask)

        ;; 1000 -> 1000000
        other-dimension-above (.shiftLeft UInt128/ONE (dec (- UInt128/SIZE first-differing-bit)))
        bigmin (.or (.and all-common-bits-mask start) other-dimension-above)

        ;; 0111 -> 0010101
        other-dimension-below (.and (.dec other-dimension-above)
                                    (if (zero? split-dimension)
                                      morton-d1-mask
                                      morton-d2-mask))
        litmax (.or (.and all-common-bits-mask end) other-dimension-below)]
    (MortonRange. litmax bigmin)))

(defn morton-get-next-address [^Number start ^Number end]
  (let [start (UInt128/fromNumber start)
        end (UInt128/fromNumber end)
        range (morton-get-next-address-internal start end)]
    [(.litmax range) (.bigmin range)]))

(defn morton-range-search [^Number start ^Number end ^Number z]
  (let [z (UInt128/fromNumber z)]
    (loop [start (UInt128/fromNumber start)
           end (UInt128/fromNumber end)]
      (cond
        (neg? (.compareTo ^UInt128 end z))
        [end 0]

        (neg? (.compareTo ^UInt128 z start))
        [0 start]

        :else
        (let [range (morton-get-next-address-internal start end)
              litmax (.litmax range)
              bigmin (.bigmin range)]
          (cond
            (neg? (.compareTo ^UInt128 bigmin z))
            (recur bigmin end)

            (neg? (.compareTo ^UInt128 z litmax))
            (recur start litmax)

            :else
            [litmax bigmin]))))))
