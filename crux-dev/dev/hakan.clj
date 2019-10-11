(ns hakan
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.rdf :as rdf])
  (:import java.util.Arrays
           [org.ejml.data DMatrix DMatrixRMaj
            DMatrixSparse DMatrixSparseCSC]
           org.ejml.dense.row.CommonOps_DDRM
           [org.ejml.sparse.csc CommonOps_DSCC MatrixFeatures_DSCC]
           org.ejml.generic.GenericMatrixOps_F64
           [org.roaringbitmap FastRankRoaringBitmap RoaringBitmap]))

;;; Experiment implementing a parser for a subset of Prolog using spec.

;; See Racket for the Datalog syntax in EBNF.
;; https://docs.racket-lang.org/datalog/datalog.html

(defn- prolog-var? [s]
  (and (symbol? s)
       (Character/isUpperCase (char (first (name s))))))

(s/def ::program (s/* ::statement))
(s/def ::statement (s/alt :assertion ::assertion
                          :retraction ::retraction
                          :query ::query))
(s/def ::assertion (s/cat :clause ::clause
                          :dot #{'.}))
(s/def ::retraction (s/cat :clause ::clause
                           :tilde #{'-}))
(s/def ::query (s/cat :literal ::literal
                      :question-mark #{'?}))
(s/def ::clause (s/alt :rule (s/cat :literal ::literal
                                    :comma-hypen #{:-}
                                    :body ::body)
                       :fact ::literal))
(s/def ::body (s/+ ::literal))
(s/def ::literal (s/alt :predicate (s/cat :symbol ::identifier
                                          :terms (s/? (s/coll-of ::term :kind list?)))
                        :equality-predicate ::equality-predicate))
(s/def ::equality-predicate (s/and list? (s/cat :op '#{= !=}
                                                :terms (s/+ ::term))))
(s/def ::term (s/or :variable ::variable
                    :constant ::constant))

(s/def ::constant (complement (some-fn list? prolog-var?)))
(s/def ::identifier (s/and symbol? (complement (some-fn prolog-var? '#{. - ? = %}))))
(s/def ::variable prolog-var?)

(comment
  (s/conform
   ::program
   '[mother_child(trude, sally).

     father_child(tom, sally).
     father_child(tom, erica).
     father_child(mike, tom).

     sibling(X, Y)      :- parent_child(Z, X), parent_child(Z, Y).

     parent_child(X, Y) :- father_child(X, Y).
     parent_child(X, Y) :- mother_child(X, Y).])

  ;; http://discovery.ucl.ac.uk/1474713/1/main.pdf
  (s/conform
   ::program
   '[edge (1, 2).
     edge (2, 3).
     path (X, Y) :- edge(X, Y).
     path (X, Z) :- path(X, Y), edge (Y, Z).])

  ;; https://github.com/racket/datalog/tree/master/tests/examples
  (s/conform
   ::program
   '[parent(john,douglas).
     parent(john,douglas)?
     ;; % parent(john, douglas).

     parent(john,ebbon)?

     parent(bob,john).
     parent(ebbon,bob).
     parent(A,B)?
     ;; % parent(john, douglas).
     ;; % parent(bob, john).
     ;; % parent(ebbon, bob).

     parent(john,B)?
     ;; % parent(john, douglas).

     parent(A,A)?

     ancestor(A,B) :- parent(A,B).
     ancestor(A,B) :- parent(A,C), ancestor(C, B).
     ancestor(A, B)?
     ;; % ancestor(ebbon, bob).
     ;; % ancestor(bob, john).
     ;; % ancestor(john, douglas).
     ;; % ancestor(bob, douglas).
     ;; % ancestor(ebbon, john).
     ;; % ancestor(ebbon, douglas).

     ancestor(X,john)?
     ;; % ancestor(bob, john).
     ;; % ancestor(ebbon, john).

     parent(bob, john)-
     parent(A,B)?
     ;; % parent(john, douglas).
     ;; % parent(ebbon, bob).

     ancestor(A,B)?
     ;; % ancestor(john, douglas).
     ;; % ancestor(ebbon, bob).
     ]))

;;; https://docs.racket-lang.org/datalog/Parenthetical_Datalog_Module_Language.html
;; (! (parent john douglas))
;; (? (parent john douglas))

;; (? (parent john ebbon))

;; (! (parent bob john))
;; (! (parent ebbon bob))
;; (? (parent A B))

;; (? (parent john B))

;; (? (parent A A))

;; (! (:- (ancestor A B)
;;        (parent A B)))
;; (! (:- (ancestor A B)
;;        (parent A C)
;;        (ancestor C B)))
;; (? (ancestor A B))

;; (? (ancestor X john))

;; (~ (parent bob john))

;; (? (parent A B))

;; (? (ancestor A B))


;;; Id compression spike

(set! *unchecked-math* :warn-on-boxed)

(def five-bit-page (zipmap (sort (str (apply str (map char (range (int \a) (inc (int \z)))))
                                      "-_:/#@"))
                           (range)))
(def five-bit-reverse-page (zipmap (vals five-bit-page)
                                   (keys five-bit-page)))

(defn compress-str
  ([s]
   (compress-str s (java.nio.ByteBuffer/allocate (count s))))
  ([s ^java.nio.ByteBuffer acc]
   (if (empty? s)
     (doto (byte-array (.remaining (.flip acc)))
       (->> (.get acc)))
     (let [[head tail] (split-at 3 s)
           three-five-bit-chars (map five-bit-page head)]
       (if (= 3 (count (filter int? three-five-bit-chars)))
         (let [[^long a ^long b ^long c] three-five-bit-chars]
           (.put acc (unchecked-byte (bit-and 0xFF (bit-or 0x80
                                                           (bit-shift-left a 2)
                                                           (bit-shift-right b 3)))))
           (.put acc (unchecked-byte (bit-and 0xFF (bit-or (bit-shift-left b 5) c))))
           (recur tail acc))
         (let [[a & tail] s]
           (recur tail (.put acc (unchecked-byte (bit-and 0x7F (int a)))))))))))

(defn decompress-to-str
  ([bs]
   (decompress-to-str bs ""))
  ([bs acc]
   (if (empty? bs)
     acc
     (if (= 0x80 (bit-and 0x80 ^long (first bs)))
       (let [[^long x ^long y & bs] bs
             a (bit-and 0x1F (bit-shift-right x 2))
             b (bit-and 0x1F (bit-or (bit-shift-left x 3)
                                     (bit-and 0x7 (bit-shift-right y 5))))
             c (bit-and 0x1F y)
             three-five-bit-chars (map five-bit-reverse-page [a b c])]
         (recur bs (apply str acc three-five-bit-chars)))
       (let [[x & bs] bs]
         (recur bs (str acc (char x))))))))

(def ^:const max-run-length 16)

(defn compress-run-lengths [s]
  (->> (for [rl (partition-by identity s)
             rl (partition-all max-run-length rl)]
         (if (> (count rl) 2)
           [(char (count rl)) (first rl)]
           rl))
       (reduce into [])
       (apply str)))

(defn decompress-run-lengths [s]
  (loop [[c & rst] s
         acc nil]
    (if-not c
      acc
      (if (<= (int c) max-run-length)
        (recur (rest rst)
               (apply str acc (repeat (int c) (first rst))))
        (recur rst
               (str acc c))))))

(defn compress-lzss [s]
  (loop [idx 0
         acc ""]
    (if (= idx (count s))
      acc
      (let [prefix-s (subs s 0 idx)
            [^long n ^long sub-s-idx] (loop [n 0x1F]
                                        (when (> n 2)
                                          (let [sub-s (subs s idx (min (+ n idx) (count s)))]
                                            (if-let [idx (clojure.string/index-of prefix-s sub-s)]
                                              [(count sub-s) idx]
                                              (recur (dec n))))))]
        (if (and sub-s-idx (pos? sub-s-idx))
          (recur (+ idx n)
                 (str acc (char (bit-or 0x80 n)) (char sub-s-idx)))
          (recur (inc idx) (str acc (get s idx))))))))

(defn decompress-lzss [s]
  (loop [s s
         acc ""]
    (if-not (seq s)
      acc
      (let [[n idx & rst] s]
        (if (= 0x80 (bit-and 0x80 (int n)))
          (recur rst (str acc (subs acc (int idx) (+ (int idx) (bit-xor 0x80 (int n))))))
          (recur (next s) (str acc n)))))))

(defn build-huffman-tree [s weight]
  (loop [pq (->> (for [[i v] (map-indexed vector (reverse s))]
                   [(Math/pow i weight) v])
                 (sort-by first))]
    (let [[[^long ia :as an] [^long ib :as bn] & pq] pq]
      (if bn
        (recur (conj (vec (sort-by first pq))
                     [(+ ia ib) an bn]))
        an))))

(defn generate-huffman-codes
  ([node]
   (generate-huffman-codes "" node))
  ([prefix [_ ln rn]]
   (->> (for [[n bit] [[ln 0] [rn 1]]]
          (cond
            (vector? n)
            (generate-huffman-codes (str prefix bit) n)

            n
            [[n prefix]]))
        (reduce into []))))

(def huffman-reverse-words (zipmap
                            (map (comp str char) (range 1 32))
                            ["w3.org/"
                             "1999/02/22-rdf-syntax-ns#"
                             "2000/01/rdf-schema#"
                             "2001/XMLSchema#"
                             "2002/07/owl#"
                             "www." "http://" "https://"  ".com/" ".org/" ".net/"]))

(def huffman-words (zipmap (vals huffman-reverse-words)
                           (keys huffman-reverse-words)))

(def huffman-alphabet (str "\u0000"
                           "-._"
                           ":/"
                           "etaoinshrdlcumwfgypbvkjxqz"
                           "0123456789"
                           "ETAOINSHRDLCUMWFGYPBVKJXQZ"
                           "~"
                           "?#[]@"
                           "%"
                           "!$&'()*+,;="
                           (apply str (vals huffman-words))))

(def huffman-tree (build-huffman-tree huffman-alphabet Math/E))

(def huffman-codes
  (->> (generate-huffman-codes huffman-tree)
       (reduce
        (fn [^"[Ljava.lang.Object;" a [c ^String bits]]
          (doto a
            (aset (int c) (boolean-array (map (comp boolean #{\1}) bits)))))
        (object-array Byte/MAX_VALUE))))

(defn- ^String replace-all [s m]
  (reduce-kv (fn [s k v]
               (clojure.string/replace s k v))
             s m))

(defn compress-huffman
  (^bytes [s]
   (compress-huffman huffman-codes s))
  (^bytes [^"[Ljava.lang.Object;" huffman-codes ^String s]
   (let [acc (java.util.BitSet.)
         s (replace-all s huffman-words)
         s-len (count s)]
     (loop [s-idx 0
            bit-idx 0]
       (let [at-end? (= s-idx s-len)
             bits ^booleans (aget huffman-codes (if at-end?
                                                  0
                                                  (.charAt s s-idx)))]
         (dotimes [idx (alength bits)]
           (.set acc (unchecked-add-int bit-idx idx) (aget bits idx)))
         (if at-end?
           (.toByteArray acc)
           (recur (unchecked-inc-int s-idx)
                  (unchecked-add-int bit-idx (alength bits)))))))))

(defn decompress-huffman
  (^String [^bytes bs]
   (decompress-huffman huffman-tree bs))
  (^String [huffman-tree ^bytes bs]
   (let [bs (java.util.BitSet/valueOf bs)]
     (loop [acc (StringBuilder.)
            bit-idx 0
            [_ ln rn] huffman-tree]
       (let [[_ c :as node] (if (.get bs bit-idx)
                              rn
                              ln)]
         (if (char? c)
           (if (= (char 0) ^char c)
             (replace-all (str acc) huffman-reverse-words)
             (recur (.append acc ^char c)
                    (unchecked-inc-int bit-idx)
                    huffman-tree))
           (recur acc
                  (unchecked-inc-int bit-idx)
                  node)))))))

(defn encode-elias-omega-code [^long n]
  (assert (pos? n))
  (loop [acc "0"
         n n]
    (if (= 1 n)
      acc
      (let [bits (Integer/toBinaryString n)]
        (recur (str bits acc) (dec (count bits)))))))

(defn decode-elias-omega-code [c]
  (loop [[c :as cs] c
         n 1]
    (if (or (= \0 c)
            (>= (inc n) (count cs)))
      n
      (let [bits (subs (str cs) 0 (inc n))]
        (recur (subs (str cs) (inc n))
               (Integer/parseInt bits 2))))))

(defn compress-rle-elias-omega [^bytes bs]
  (let [bits (.toString (BigInteger. bs) 2)
        last-bit (if (get bits 0)
                   \1
                   \0)]
    (loop [idx 0
           acc (str last-bit)
           last-bit last-bit
           rle 0]
      (if (= idx (count bits))
        (.toByteArray (BigInteger. (str acc (encode-elias-omega-code rle)) 2))
        (if (= last-bit (get bits idx))
          (recur (inc idx)
                 acc
                 last-bit
                 (inc rle))
          (recur (inc idx)
                 (str acc (encode-elias-omega-code rle))
                 (get bits idx)
                 1))))))

(defn decompress-rle-elias-omega ^bytes [^bytes bs]
  (let [bits (.toString (BigInteger. bs) 2)
        acc ""]
    (loop [idx 1
           acc acc
           last-bit (get bits 0)]
      (if (= idx (count bits))
        (.toByteArray (BigInteger. acc 2))
        (if-let [[oc new-idx] (loop [idx idx
                                     n 1]
                                (if (= \0 (get bits idx))
                                  [n (inc idx)]
                                  (let [new-idx (+ idx (inc n))]
                                    (recur new-idx (Integer/parseInt (subs bits idx new-idx) 2)))))]
          (recur (int new-idx)
                 (apply str acc (repeat oc last-bit))
                 (if (= \1 last-bit)
                   \0
                   \1))
          (.toByteArray (BigInteger. acc 2)))))))

;; "^BANANA|" "BNN^AA|A"
(defn bwt [s]
  (let [result (->> (for [i (range (count s))]
                      (str (subs s i) (subs s 0 i)))
                    (sort))]

    [(->> result
          (map last)
          (apply str))
     (count (first (partition-by #{s} result)))]))

(defn inverse-bwt [^String s eof-char-or-index]
  (let [acc (object-array (repeat (count s) ""))]
    (dotimes [_ (count s)]
      (dotimes [i (count s)]
        (aset acc i (str (.charAt s i) (aget acc i))))
      (java.util.Arrays/sort acc))
    (if (integer? eof-char-or-index)
      (aget acc eof-char-or-index)
      (->> acc
           (filter #(clojure.string/ends-with? % (str eof-char-or-index)))
           (first)))))

(defn encode-mtf [alphabet s]
  (loop [alphabet alphabet
         [c & s] s
         acc []]
    (if-not c
      (byte-array acc)
      (let [idx (.indexOf (str alphabet) (int c))]
        (recur (str (get alphabet idx)
                    (subs alphabet 0 idx)
                    (subs alphabet (inc idx)))
               s (conj acc idx))))))

(defn decode-mtf [alphabet bs]
  (loop [alphabet alphabet
         [x & xs] bs
         acc ""]
    (if-not x
      acc
      (let [idx (long x)]
        (recur (str (get alphabet idx)
                    (subs alphabet 0 idx)
                    (subs alphabet (inc idx)))
               xs
               (str acc (get alphabet idx)))))))

(def arithmetic-alphabet (str "\u0000"
                              "-._"
                              ":/"
                              "etaoinshrdlcumwfgypbvkjxqz"
                              "0123456789"
                              "ETAOINSHRDLCUMWFGYPBVKJXQZ"
                              "~ "
                              "?#[]@"
                              "%"
                              "!$&'()*+,;="))

(defn build-arithmetic-lookup [alphabet]
  (let [weights (for [[i x] (map-indexed vector (reverse alphabet))]
                  [(bigdec (Math/pow i Math/E)) x])
        total-weights (bigdec (reduce + (map first weights)))
        lookup (for [[w x] weights]
                 [x (with-precision 5
                      (.divide (bigdec w) total-weights *math-context*))])
        [_ lookup] (reduce
                    (fn [[idx acc] [x w]]
                      (let [end-idx (.min (.add ^BigDecimal idx w) 1M)]
                        [end-idx (assoc acc x [idx end-idx])]))
                    [0M {}] lookup)]
    lookup))

(defn- build-arithmetic-reverse-lookup [arithmetic-lookup]
  (->> (zipmap (map first (vals arithmetic-lookup))
               arithmetic-lookup)
       (into (sorted-map))))

(def arithmetic-lookup (build-arithmetic-lookup arithmetic-alphabet))
(def arithmetic-reverse-lookup (build-arithmetic-reverse-lookup arithmetic-lookup))

;; "bill gates"
;; {\space [0.00M 0.10M]
;;  \a [0.10M 0.20M]
;;  \b [0.20M 0.30M]
;;  \e [0.30M 0.40M]
;;  \g [0.40M 0.50M]
;;  \i [0.50M 0.60M]
;;  \l [0.60M 0.80M]
;;  \s [0.80M 0.90M]
;;  \t [0.90M 1.00M]}

;; Set low to 0.0
;; Set high to 1.0
;; While there are still input symbols do
;; get an input symbol
;; code range = high - low.
;; high = low + range·high range(symbol)
;; low = low + range·low range(symbol)
;; End of While
;; output low
(defn decompress-arithmetic
  ([n]
   (decompress-arithmetic arithmetic-reverse-lookup n))
  ([arithmetic-reverse-lookup n]
   (let [n (if (bytes? n)
             (let [bd (bigdec (biginteger ^butes n))]
               (.movePointLeft bd (.precision bd)))
             n)]
     (with-precision (.scale (bigdec n))
       (loop [acc ""
              low 0M
              high 1M]
         (let [range (.subtract high low)
               seek (.divide (.subtract ^BigDecimal n low) range *math-context*)
               [_ [c [l-low l-high]]] (first (rsubseq arithmetic-reverse-lookup <= seek))]
           (if (= c \u0000)
             acc
             (recur (str acc c)
                    (.add low (.multiply range l-low))
                    (.add low (.multiply range l-high))))))))))

;; get encoded number
;; Do
;; find symbol whose range straddles
;; the encoded number
;; output the symbol
;; range = symbol high value - symbol
;; low value
;; subtract symbol low value from encoded
;; number
;; divide encoded number by range
;; until no more symbols
(defn compress-arithmetic
  (^bytes [s]
   (compress-arithmetic s arithmetic-lookup))
  (^bytes [s arithmetic-lookup]
   (let [[low high] (reduce
                     (fn [[^BigDecimal low ^BigDecimal high] c]
                       (let [range (.subtract ^BigDecimal high low)
                             [c-low c-high] (get arithmetic-lookup c)]
                         [(.add low (.multiply range c-low))
                          (.add low (.multiply range c-high))]))
                     [0.0M 1.0M] (str s \u0000))]
     (loop [p (long (* 1.5 (count s)))]
       (let [candidate (with-precision p
                         (.setScale (.add ^BigDecimal low (.divide (.subtract ^BigDecimal high low) 2M *math-context*)) p java.math.RoundingMode/HALF_UP))]
         (if (= s (try
                    (decompress-arithmetic arithmetic-reverse-lookup candidate)
                    (catch ArithmeticException ignore)))
           (.toByteArray (.unscaledValue (bigdec candidate)))
           (recur (inc p))))))))

;; See https://marknelson.us/posts/2014/10/19/data-compression-with-arithmetic-coding.html
;; https://web.stanford.edu/class/ee398a/handouts/papers/WittenACM87ArithmCoding.pdf

(def ^:const bac-eof 256)

(def ^:const bac-code-bits 40)
(def ^:const bac-top-value (dec (bit-shift-left 1 bac-code-bits)))
(def ^:const bac-first-quarter (inc (quot bac-top-value 4)))
(def ^:const bac-half (* 2 bac-first-quarter))
(def ^:const bac-third-quarter (* 3 bac-first-quarter))

(def ^{:tag 'ints} brown-letter-frequencies
  (int-array (inc bac-eof)
             (concat [1, 1, 1. 1. 1, 1, 1, 1, 1, 1, 124, 1, 1, 1, 1, 1
                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                      1236, 1, 21, 9, 3, 1, 2, 15, 2, 2, 2, 1, 79, 19, 60, 1,
                      15, 15, 8, 5, 4, 7, 5, 4, 4, 6, 3, 2, 1, 1, 1, 1,
                      1, 24, 15, 22, 12, 15, 10, 9, 16, 16, 8, 6, 12, 23, 13, 11,
                      14, 1, 14, 26, 29, 6, 3, 11, 1, 3, 1, 1, 1, 1, 1, 5,
                      1, 491, 85, 173, 232, 744, 127, 110, 293, 418, 6, 39, 250, 139, 42, 446,
                      111, 5, 388, 375, 531, 159, 57, 97, 12, 101, 5, 2, 1, 2, 3]
                     (repeat 1))))

(defn brown-char->weight ^long [^long c]
  (aget brown-letter-frequencies c))

(defn default-char->weight ^long [c]
  (cond
    (= bac-eof c)
    250

    (pos? (.indexOf "-._:/" (int c)))
    250

    (Character/isDigit (char c))
    150

    (pos? (.indexOf "~?#[]@%!$&'()*+,;=" (int c)))
    50

    :else
    (aget brown-letter-frequencies (int c))))

(defn- frequency-table-total-weight ^long [^ints frequency-table]
  (loop [i (int 0)
         acc 0]
    (if (= (alength frequency-table) i)
      acc
      (let [p (aget frequency-table i)]
        (recur (unchecked-inc-int i) (+ p acc))))))

(defn- update-frequency-weight ^long [^ints frequency-table char->weight ^long total-weights c]
  (let [max-weight Short/MAX_VALUE
        weight (long (* 0.5 (long (char->weight (int c)))))]
    (if (> (+ weight total-weights) max-weight)
      (do (loop [i 0]
            (when (< i (alength frequency-table))
              (aset frequency-table i (int (max 1 (quot (aget frequency-table i) 2))))
              (recur (inc i))))
          (recur frequency-table char->weight (frequency-table-total-weight frequency-table) c))
      (do (aset frequency-table (int c) (int (+ weight (int (aget frequency-table (int c))))))
          (+ weight total-weights)))))

(defn- build-frequency-table ^ints [char->weight]
  (int-array (map char->weight (range (inc bac-eof)))))

(defn- output-pending-bits ^long [^java.util.BitSet acc ^long idx ^Boolean bit ^long pending-bits]
  (.set acc idx bit)
  (loop [idx (inc idx)
         pending-bits pending-bits]
    (if (zero? pending-bits)
      idx
      (do (.set acc idx (not bit))
          (recur (inc idx) (dec pending-bits))))))

(defn compress-binary-arithmetic
  ([s]
   (compress-binary-arithmetic s default-char->weight))
  ([s char->weight]
   (let [frequency-table (build-frequency-table char->weight)
         acc (java.util.BitSet.)]
     (loop [[c & s] (conj (vec s) bac-eof)
            low 0
            high bac-top-value
            idx 0
            pending-bits 0
            total-weights (frequency-table-total-weight frequency-table)]
       (if-not c
         (do (if (< low bac-first-quarter)
               (output-pending-bits acc idx false (inc pending-bits))
               (output-pending-bits acc idx true (inc pending-bits)))
             (.toByteArray acc))
         (let [range (inc (- high low))
               [^long p-low
                ^long p-high] (loop [i (int 0)
                                     p-low 0]
                                (let [p-high (+ p-low (aget frequency-table i))]
                                  (if (= i (int c))
                                    [p-low p-high]
                                    (recur (unchecked-inc-int i) p-high))))
               high (dec (+ low (quot (* range p-high) total-weights)))
               low (+ low (quot (* range p-low) total-weights))
               total-weights (update-frequency-weight frequency-table char->weight total-weights c)]
           (let [[low high idx pending-bits]
                 (loop [low low
                        high high
                        pending-bits pending-bits
                        idx idx]
                   (cond
                     (< high bac-half)
                     (let [idx (long (output-pending-bits acc idx false pending-bits))]
                       (recur (bit-shift-left low 1)
                              (bit-or (bit-shift-left high 1) 1)
                              0
                              idx))

                     (>= low bac-half)
                     (let [idx (long (output-pending-bits acc idx true pending-bits))]
                       (recur (bit-shift-left (- low bac-half) 1)
                              (bit-or (bit-shift-left (- high bac-half) 1) 1)
                              0
                              idx))

                     (and (>= low bac-first-quarter)
                          (< high bac-third-quarter))
                     (recur (bit-shift-left (- low bac-first-quarter ) 1)
                            (bit-or (bit-shift-left (- high bac-first-quarter) 1) 1)
                            (inc pending-bits)
                            idx)
                     :else
                     [low high idx pending-bits]))]
             (recur s (long low) (long high) (long idx) (long pending-bits) total-weights))))))))

(defn decompress-binary-arithmetic
  ([bytes]
   (decompress-binary-arithmetic bytes default-char->weight))
  ([^bytes bytes char->weight]
   (let [frequency-table (build-frequency-table char->weight)
         bs (java.util.BitSet/valueOf bytes)]
     (loop [acc ""
            low 0
            high bac-top-value
            value (long (loop [idx 0
                               value 0]
                          (if (= bac-code-bits idx)
                            value
                            (recur (inc idx)
                                   (bit-or value (bit-shift-left (if (.get bs idx)
                                                                   1
                                                                   0)
                                                                 (- (dec bac-code-bits) idx)))))))
            idx bac-code-bits
            total-weights (frequency-table-total-weight frequency-table)]
       (let [range (inc (- high low))
             cumulative (quot (dec (* (inc (- value low)) total-weights)) range)
             [c
              ^long p-low
              ^long p-high] (loop [i (int 0)
                                   p-low 0]
                              (let [p-high (+ p-low (aget frequency-table i))]
                                (if (> p-high cumulative)
                                  [i p-low p-high]
                                  (recur (unchecked-inc-int i) p-high))))
             high (dec (+ low (quot (* range p-high) total-weights)))
             low (+ low (quot (* range p-low) total-weights))
             total-weights (update-frequency-weight frequency-table char->weight total-weights c)]
         (if (= bac-eof c)
           (str acc)
           (let [[low high value idx]
                 (loop [low low
                        high high
                        value value
                        idx idx]
                   (cond
                     (< high bac-half)
                     (recur (bit-shift-left low 1)
                            (bit-or (bit-shift-left high 1) 1)
                            (bit-or (bit-shift-left value 1)
                                    (if (.get bs idx)
                                      1
                                      0))
                            (inc idx))

                     (>= low bac-half)
                     (recur (bit-shift-left (- low bac-half) 1)
                            (bit-or (bit-shift-left (- high bac-half) 1) 1)
                            (bit-or (bit-shift-left (- value bac-half) 1)
                                    (if (.get bs idx)
                                      1
                                      0))
                            (inc idx))

                     (and (>= low bac-first-quarter)
                          (< high bac-third-quarter))
                     (recur (bit-shift-left (- low bac-first-quarter) 1)
                            (bit-or (bit-shift-left (- high bac-first-quarter) 1) 1)
                            (bit-or (bit-shift-left (- value bac-first-quarter) 1)
                                    (if (.get bs idx)
                                      1
                                      0))
                            (inc idx))

                     :else
                     [low high value idx]))]
             (recur (str acc (char c)) (long low) (long high) (long value) (long idx) total-weights))))))))

;; Bloom Filter

(def ^:const ^:private bloom-filter-hashes 2)

(defn bloom-filter-probe [size x]
  (let [h (hash x)]
    (loop [n 0
           p (java.util.BitSet.)]
      (if (= bloom-filter-hashes n)
        p
        (recur (inc n)
               (doto p
                 (.set (long (mod (mix-collection-hash h n) size)))))))))

(defn add-to-bloom-filter ^java.util.BitSet [^java.util.BitSet bs x]
  (doto bs
    (.or (bloom-filter-probe (.size bs) x))))

(defn bloom-filter-might-contain? [^java.util.BitSet bs x]
  (.intersects bs (bloom-filter-probe (.size bs) x)))


;; k2-tree

(defn bit-str->bitset
  (^org.roaringbitmap.RoaringBitmap [s]
   (bit-str->bitset (RoaringBitmap.) s))
  (^org.roaringbitmap.RoaringBitmap [^RoaringBitmap bs s]
   (let [bits (->> s
                   (remove #{\space})
                   (vec))]
     (dotimes [n (count bits)]
       (when (= \1 (get bits n))
         (.add bs n)))
     bs)))

(defn bitset-rank ^long [^RoaringBitmap bs ^long n]
  (if (= -1 n)
    0
    (.rank bs n)))

(defn power-of? [^long x ^long y]
  (if (zero? (rem x y))
    (recur (quot x y) y)
    (= 1 x)))

(defn next-power-of ^long [^long x ^long y]
  (long (Math/pow 2 (long (Math/pow y (Math/log x))))))

(defrecord K2Tree [^long n ^long k ^long k2 ^RoaringBitmap t ^long t-size ^RoaringBitmap l])

(defn new-static-k2-tree [^long n ^long k tree-bit-str leaf-bit-str]
  (map->K2Tree
   {:n (if (power-of? n k)
         n
         (next-power-of n k))
    :k k
    :k2 (long (Math/pow k 2))
    :t (bit-str->bitset (FastRankRoaringBitmap.) tree-bit-str)
    :t-size (->> tree-bit-str
                 (remove #{\space})
                 (count)
                 (long))
    :l (bit-str->bitset leaf-bit-str)}))

;; http://repositorio.uchile.cl/bitstream/handle/2250/126520/Compact%20representation%20of%20Webgraphs%20with%20extended%20functionality.pdf?sequence=1
;; NOTE: Redefined in terms of k2-tree-range below.
(defn k2-tree-check-link? [^K2Tree k2-tree ^long row ^long col]
  (let [n (.n k2-tree)
        k (.k k2-tree)
        k2 (.k2 k2-tree)
        t ^RoaringBitmap (.t k2-tree)
        t-size (.t-size k2-tree)
        l ^RoaringBitmap (.l k2-tree)]
    (loop [n n
           p row
           q col
           z -1]
      (if (>= z t-size)
        (.contains l (- z t-size))
        (if (or (= -1 z) (.contains t z))
          (let [n (quot n k)
                y (* (bitset-rank t z) k2)
                y (+ y
                     (* (Math/abs (quot p n)) k)
                     (Math/abs (quot q n)))]
            (recur n
                   (rem p n)
                   (rem q n)
                   y))
          false)))))

;; NOTE: All elements in a row.
(defn k2-tree-succsessors [{:keys [^long n
                                   ^long k
                                   ^long k2
                                   ^long t-size
                                   ^RoaringBitmap t
                                   ^RoaringBitmap l] :as k2-tree} ^long row]
  ((fn step [^long n ^long p ^long q ^long z]
     (if (>= z t-size)
       (when (.contains l (- z t-size))
         [q])
       (when (or (= -1 z) (.contains t z))
         (let [n (quot n k)
               y (+ (* (bitset-rank t z) k2)
                    (* (Math/abs (quot p n)) k))]
           (->> (range k)
                (mapcat (fn [^long j]
                          (step n (mod p n) (+ q (* n j)) (+ y j)))))))))
   n row 0 -1))

;; NOTE: All elements in a column.
(defn k2-tree-predecessors [{:keys [^long n
                                    ^long k
                                    ^long k2
                                    ^long t-size
                                    ^RoaringBitmap t
                                    ^RoaringBitmap l] :as k2-tree} ^long col]
  ((fn step [^long n ^long q ^long p ^long z]
     (if (>= z t-size)
       (when (.contains l (- z t-size))
         [p])
       (when (or (= -1 z) (.contains t z))
         (let [n (quot n k)
               y (+ (* (bitset-rank t z) k2)
                    (Math/abs (quot q n)))]
           (->> (range k)
                (mapcat (fn [^long j]
                          (step n (mod q n) (+ p (* n j)) (+ y (* j k))))))))))
   n col 0 -1))

;; TOO: How to make iterative?
(defn k2-tree-range [{:keys [^long n
                             ^long k
                             ^long k2
                             ^long t-size
                             ^RoaringBitmap t
                             ^RoaringBitmap l] :as k2-tree} row1 row2 col1 col2]
  ((fn step [n p1 p2 q1 q2 dp dq z]
     (let [n (long n)
           z (long z)
           p1 (long p1)
           p2 (long p2)
           q1 (long q1)
           q2 (long q2)
           dp (long dp)
           dq (long dq)]
       (if (>= z t-size)
         (when (.contains l (- z t-size))
           [[dp dq]])
         (when (or (= -1 z) (.contains t z))
           (let [n (quot n k)
                 y (* (bitset-rank t z) k2)]
             (->> (for [^long i (range (quot p1 n) (inc (quot p2 n)))
                        :let [p1 (if (= i (quot p1 n))
                                   (mod p1 n)
                                   0)
                              p2 (if (= i (quot p2 n))
                                   (mod p2 n)
                                   (dec n))]
                        ^long j (range (quot q1 n) (inc (quot q2 n)))
                        :let [q1 (if (= j (quot q1 n))
                                   (mod q1 n)
                                   0)
                              q2 (if (= j (quot q2 n))
                                   (mod q2 n)
                                   (dec n))]]
                    (step n p1 p2 q1 q2 (+ dp (* n i)) (+ dq (* n j)) (+ y (* k i) j)))
                  (apply concat)))))))
   n row1 row2 col1 col2 0 0 -1))

;; NOTE: The above re-implemented in terms of k2-tree-range.
;;       Original iterative version is order of magnitude faster.
(defn k2-tree-check-link? [k2 row col]
  (->> (k2-tree-range k2 row row col col)
       (seq)
       (boolean)))

(defn k2-tree-succsessors [{:keys [^long n] :as k2} row]
  (->> (k2-tree-range k2 row row 0 n)
       (map second)))

(defn k2-tree-predecessors [{:keys [^long n] :as k2} col]
  (->> (k2-tree-range k2 0 n col col)
       (map first)))


;; Matrix data form https://arxiv.org/pdf/1707.02769.pdf page 8.
(comment
  (let [k2 (new-static-k2-tree
            10
            2
            "1110 1101 1010 0100 0110 1001 0101 0010 1010 1100"
            "0011 0011 0010 0010 0001 0010 0100 0010 1000 0010 1010")
        expected '[true
                   true
                   true
                   true
                   true
                   true
                   false
                   ([5 8])
                   ([1 2] [1 3] [1 4])
                   ([3 6] [7 6] [8 6] [9 6])
                   (2 3 4)
                   (3 7 8 9)]]
    (= expected
       [;; 3rd q
        (k2-tree-check-link? k2 9 6)
        (k2-tree-check-link? k2 8 6)

        ;; 1st q
        (k2-tree-check-link? k2 1 2)
        (k2-tree-check-link? k2 3 0)

        ;; 2nd q
        (k2-tree-check-link? k2 2 9)
        (k2-tree-check-link? k2 5 8)

        ;; Should be false
        (k2-tree-check-link? k2 0 0)

        (k2-tree-range k2 5 5 8 8)

        (k2-tree-range k2 1 1 0 16)
        (k2-tree-range k2 0 16 6 6)

        ;; TODO: Does not work yet:
        ;; Should return 2 3 4
        (k2-tree-succsessors k2 1)
        ;; Should return 3 7 8 9
        (k2-tree-predecessors k2 6)])))

;; Wavelet Matrix
;; https://users.dcc.uchile.cl/~gnavarro/ps/is14.pdf
;; https://diegocaro.cl/thesis/thesis.pdf

(defn- log2-binary ^long [^long x]
  (- Long/SIZE (Long/numberOfLeadingZeros x)))

(defn- rank-zero ^long [^RoaringBitmap b ^long n]
  (- (inc n) (.rankLong b n)))

;; TODO: How to express properly and efficiently?
(defn- select-zero ^long [^RoaringBitmap b ^long n]
  (.select (RoaringBitmap/flip b 0 (long (.last b))) n))

(defn build-wavelet-matrix [s]
  (let [c (count s)
        h (log2-binary (reduce max s))]
    (last
     (reduce
      (fn [[s bs] ^long l]
        (let [mask (bit-shift-left 1 (- h l))
              one? (fn [^long x]
                     (pos? (bit-and x mask)))
              b (FastRankRoaringBitmap.)]
          (dotimes [n c]
            (when (one? (nth s n))
              (.add b n)))
          (let [z (- c (.getLongCardinality ^RoaringBitmap b))]
            [(sort-by one? s)
             (conj bs [b z c])])))
      [s []]
      (range 1 (inc h))))))

(defn wavelet-matrix-access ^long [[[_ _ ^long c] :as wm] ^long i]
  (if (or (neg? i) (>= i c))
    -1
    (loop [l 0
           i i
           acc 0]
      (if-let [[^RoaringBitmap b ^long z] (nth wm l nil)]
        (let [one? (.contains b i)]
          (recur
           (inc l)
           (if one?
             (+ z (dec (.rankLong b i)))
             (dec (rank-zero b i)))
           (cond-> (bit-shift-left acc 1)
             one? (bit-or 1))))
        acc))))

(defn wavelet-matrix-rank ^long [[[_ _ ^long c] :as wm] ^long i ^long a]
  (if (or (neg? i) (>= i c))
    0
    (loop [l 0
           i (inc i)
           p 0]
      (if-let [[^RoaringBitmap b ^long z] (nth wm l nil)]
        (if (pos? (bit-and a (bit-shift-left 1 (dec (- (count wm) l)))))
          (let [rank-i (.rankLong b (dec i))
                rank-p (if (zero? p)
                         0
                         (.rankLong b (dec p)))]
            (if (or (zero? rank-i)
                    (and (zero? rank-p)
                         (not (zero? l))))
              0
              (recur (inc l) (+ z rank-i) (+ z rank-p))))
          (let [rank-i (rank-zero b (dec i))
                rank-p (if (zero? p)
                         0
                         (rank-zero b (dec p)))]
            (if (or (zero? rank-i)
                    (and (zero? rank-p)
                         (not (zero? l))))
              0
              (recur (inc l) rank-i rank-p))))
        (- i p)))))

(defn wavelet-matrix-select ^long [[[_ _ ^long c] :as wm] ^long j ^long a]
  (if (or (neg? j) (>= j c))
    -1
    (loop [l 0
           p 0]
      (if-let [[^RoaringBitmap b ^long z] (nth wm l nil)]
        (if (pos? (bit-and a (bit-shift-left 1 (dec (- (count wm) l)))))
          (let [rank-p (if (zero? p)
                         0
                         (.rankLong b (dec p)))]
            (recur (inc l) (+ z rank-p)))
          (let [rank-p (if (zero? p)
                         0
                         (rank-zero b (dec p)))]
            (recur (inc l) rank-p)))
        (try
          (let [j (+ p j)]
            (if (>= j c)
              -1
              (loop [l 0
                     j j]
                (if-let [[^RoaringBitmap b ^long z] (nth wm (dec (- (count wm) l)) nil)]
                  (recur
                   (inc l)
                   (if (pos? (bit-and a (bit-shift-left 1 l)))
                     (long (.select b (- j z)))
                     (select-zero b j)))
                  j))))
          (catch IllegalArgumentException _
            -1))))))

(comment
  (let [wm (build-wavelet-matrix [4 7 6 5 3 2 1 0 2 1 4 1 7])]
    (= [4 -1 2 0 2 1 0 10 -1]
       [(wavelet-matrix-access wm 0)
        (wavelet-matrix-access wm 13)
        (wavelet-matrix-rank wm 10 4)
        (wavelet-matrix-rank wm 14 4)
        (wavelet-matrix-rank wm 12 7)
        (wavelet-matrix-rank wm 5 5)
        (wavelet-matrix-select wm 0 4)
        (wavelet-matrix-select wm 1 4)
        (wavelet-matrix-select wm 2 4)])))

;; WatDiv analysis helpers:

;; 230 WatDiv queries:

;; Concurrency, 1 vs 2 query threads:
;; RocksJNR: (/ 85185.683737  60181.143235) 1.4154879611435816
;; RocksJava: (/ 96791.482514 69994.982891) 1.3828345763685514
;; LMDB: (/ 29754.773409 21082.969962) 1.4113179244968845

;; Using chunked Buffers+OpenSSL+value objects (9e58f2d716b829e1333c09dd7658a513816b6eb5)
;; ("master" below was before this commit.)

;; RocksJNR:
;; run 1: 79986 347.76521739130436
;; run 2: 68590 298.2173913043478
;; run 3: 68195 296.5
;; empty caches: 81919 356.1695652173913

;; Using chunked Buffers+OpenSSL (ce63626eebb151946b166d0cc409416bb637935d)
;; ("master" below was before this commit.)

;; RocksJNR:
;; run 1: 86984 378.1913043478261
;; run 2: 71378 310.3391304347826
;; run 3: 75396 327.8086956521739
;; empty caches: 93228 405.3391304347826

;; RocksJava:
;; run 1: 134982 586.8782608695652
;; run 2: 98710 429.17391304347825
;; run 3: 94586 411.24347826086955
;; empty caches: 104238 453.2086956521739

;; LMDB:
;; run 1: 110801 481.74347826086955
;; run 2: 38391 166.91739130434783
;; run 3: 30168 131.16521739130434
;; empty caches: 99904 434.3652173913043

;; Using Buffers+libgcrypt (master):

;; RocksJNR:
;; run 1: 129179 561.6478260869566
;; run 2: 99675 433.3695652173913
;; run 3: 91778 399.03478260869565
;; empty caches: 104029 452.3

;; RocksJava:
;; run 1: 138160 600.695652173913
;; run 2: 129845 564.5434782608696
;; run 3: 112573 489.4478260869565
;; empty caches: 124112 539.6173913043478

;; LMDB:
;; run 1: 133482 580.3565217391305
;; run 2: 68293 296.9260869565217
;; run 3: 44112 191.7913043478261
;; empty caches: 108552 471.96521739130435

;; Using Buffers+MessageDigest (master):

;; RocksJNR:
;; run 1: 115483 502.1
;; run 2: 91973 399.88260869565215
;; run 3: 92495 402.1521739130435
;; empty caches: 103185 448.6304347826087

;; Using Buffers+MessageDigest+agrona.disable.bounds.checks (master):

;; RocksJNR:
;; run 1: 116484 506.45217391304345
;; run 2: 92879 403.82173913043476
;; run 3: 92497 402.1608695652174
;; empty caches: 103704 450.88695652173914

;; Using Buffers+libgcrypt+agrona.disable.bounds.checks (master):

;; RocksJNR:
;; run 1: 109899 477.82173913043476
;; run 2: 98163 426.795652173913
;; run 3: 89005 386.9782608695652
;; empty caches: 94521 410.96086956521737

;; Using Buffers+OpenSSL (master):

;; RocksJNR:
;; run 1: 110868 482.03478260869565
;; run 2: 88095 383.0217391304348
;; run 3: 91061 395.9173913043478
;; empty caches: 103080 448.17391304347825

;; LMDB:
;; run 1: 83796 364.3304347826087
;; run 2: 39404 171.3217391304348
;; run 3: 42925 186.6304347826087
;; empty caches: 71580 311.2173913043478

;; Using Buffers+ByteUtils/sha1 ThreadLocals (master):

;; RocksJNR:
;; run 1: 99720 433.5652173913044
;; run 2: 86895 377.80434782608694
;; run 3: 86584 376.45217391304345
;; empty caches: 97467 423.7695652173913

;; Using Buffers+ByteUtils/sha1 locals (master+local changes):

;; RocksJNR:
;; run 1: 102052 443.704347826087
;; run 2: 87579 380.7782608695652
;; run 3: 88220 383.5652173913044
;; empty caches: 98702 429.1391304347826

;; Using Bytes (d34e06af2cd474fcc9c912bbb8cc823e723ddfac):

;; RocksJNR:
;; run 1: 96162 418.09565217391304
;; run 2: 74150 322.39130434782606
;; run 3: 72645 315.8478260869565
;; empty caches: 90454 393.2782608695652

;; RocksJava:
;; run 1: 114670 498.5652173913044
;; run 2: 94821 412.26521739130436
;; run 3: 94199 409.5608695652174
;; empty caches: 97881 425.5695652173913

;; LMDB:
;; run 1: 104996 456.504347826087
;; run 2: 32548 141.51304347826087
;; run 3: 30490 132.56521739130434
;; empty caches: 50331 218.8304347826087

;; dev/matrix.clj:
;; ingest: 1m10s, 112.5M (RoaringBitmap estimated in-memory size.)
;; 228 queries work, 2 does not finish.
;;
;; Average query time:
;; run 1: 21898 95.20869565217392
;; run 2: 19780 86.0
;; run 3: 19445 84.54347826086956

;; dev/matrix.clj roaring-*, one Roaring64NavigableMap:
;; ingest: 1m10s, 112.5M (RoaringBitmap estimated in-memory size.)
;; 228 queries work, 2 does not finish.
;;
;; Average query time:
;; run 1: 21898 95.20869565217392
;; run 2: 19780 86.0
;; run 3: 19445 84.54347826086956

;; dev/matrix.clj r-* Int2ObjectHashMap, one RoaringBitmap per row:
;; ingest: 1m10s, 105M (RoaringBitmap estimated in-memory size.)
;; All queries work, 2 are slow (same as above), numbers exclude these.
;;
;; Average query time:
;; run 1: 8015 34.84782608695652
;; run 2: 7878 34.25217391304348
;; run 3: 8376 36.417391304347824

;; dev/matrix.clj r-* Int2ObjectHashMap, one off-heap
;; ImmutableRoaringBitmap per row:
;; run 1: 11038 47.99130434782609
;; run 2: 8920 38.78260869565217
;; run 3: 8690 37.78260869565217

(comment
  (swap! (:cache-state (:kv-store node)) empty)
  (crux.query/query-plan-for (crux.sparql/sparql->datalog
                              "")
                             (crux.index/read-meta (:kv-store node) :crux.kv/stats))
  (defn run-watdiv-reference
    ([]
     (run-watdiv-reference (io/resource "watdiv/watdiv_crux.edn") #{35 67}))
    ([resource skip?]
     (let [times (mapv
                  (fn [{:keys [idx query crux-results]}]
                    (let [start (System/currentTimeMillis)]
                      (try
                        (let [result (count (.q (.db node)
                                                (crux.sparql/sparql->datalog query)))]
                          (assert (= crux-results result)
                                  (pr-str [idx crux-results result])))
                        (catch Throwable t
                          (prn idx t)))
                      (- (System/currentTimeMillis) start)))
                  (->> (read-string (slurp resource))
                       (remove :crux-error)
                       (remove (comp skip? :idx))))
           total (reduce + times)]
       {:total total :average (/ total (double (count times)))}))))

;; Destructure costs:

;; Records
;; Record field access
;; "Elapsed time: 264.034433 msecs"
;; Record destructure
;; "Elapsed time: 4180.275593 msecs"
;; Record keyword lookup
;; "Elapsed time: 425.580412 msecs"
;; Record keyword lookup with default
;; "Elapsed time: 1252.865026 msecs"
;; Record get lookup
;; "Elapsed time: 1344.899763 msecs"
;; Record get lookup with default
;; "Elapsed time: 1346.48226 msecs"
;; Record .valAt Java interop
;; "Elapsed time: 446.372848 msecs"

;; Maps
;; Map destructure
;; "Elapsed time: 4201.326436 msecs"
;; Map keyword lookup
;; "Elapsed time: 813.649374 msecs"
;; Map keyword lookup with default
;; "Elapsed time: 916.385803 msecs"
;; Map get lookup
;; "Elapsed time: 957.835825 msecs"
;; Map get lookup with default
;; "Elapsed time: 935.201014 msecs"
;; Map .valAt Java interop
;; "Elapsed time: 439.555881 msecs"

;; Vectors
;; Vector destructure
;; "Elapsed time: 398.926347 msecs"
;; Vector first
;; "Elapsed time: 5938.484479 msecs"
;; Vector get
;; "Elapsed time: 974.473855 msecs"
;; Vector nth
;; "Elapsed time: 619.242223 msecs"
;; Vector nth with default
;; "Elapsed time: 399.153515 msecs"
;; Vector .nth Java interop
;; "Elapsed time: 330.657743 msecs"

(defrecord Foo [bar])
(defn destructure-test []
  (println "Records")
  (println "Record field access")
  (let [x (Foo. 1)
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (.bar x))))
    (assert (= 1 (aget a 0))))

  (println "Record destructure")
  (let [x (Foo. 1)
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (let [{:keys [bar]} x]
         (aset a 0 bar))))
    (assert (= 1 (aget a 0))))

  (println "Record keyword lookup")
  (let [x (Foo. 1)
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (:bar x))))
    (assert (= 1 (aget a 0))))

  (println "Record keyword lookup with default")
  (let [x (Foo. 1)
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (:bar x nil))))
    (assert (= 1 (aget a 0))))

  (println "Record get lookup")
  (let [x (Foo. 1)
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (get x :bar))))
    (assert (= 1 (aget a 0))))

  (println "Record get lookup with default")
  (let [x (Foo. 1)
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (get x :bar nil))))
    (assert (= 1 (aget a 0))))

  (println "Record .valAt Java interop")
  (let [x (Foo. 1)
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (.valAt x :bar))))
    (assert (= 1 (aget a 0))))

  (println)
  (println "Maps")
  (println "Map destructure")
  (let [x {:bar 1}
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (let [{:keys [bar]} x]
         (aset a 0 bar))))
    (assert (= 1 (aget a 0))))

  (println "Map keyword lookup")
  (let [x {:bar 1}
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (:bar x))))
    (assert (= 1 (aget a 0))))

  (println "Map keyword lookup with default")
  (let [x {:bar 1}
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (:bar x nil))))
    (assert (= 1 (aget a 0))))

  (println "Map get lookup")
  (let [x {:bar 1}
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (get x :bar))))
    (assert (= 1 (aget a 0))))

  (println "Map get lookup with default")
  (let [x {:bar 1}
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (get x :bar nil))))
    (assert (= 1 (aget a 0))))

  (println "Map .valAt Java interop")
  (let [x {:bar 1}
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (.valAt x :bar))))
    (assert (= 1 (aget a 0))))

  (println)
  (println "Vectors")
  (println "Vector destructure")
  (let [x [1]
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (let [[y] x]
         (aset a 0 y))))
    (assert (= 1 (aget a 0))))

  (println "Vector first")
  (let [x [1]
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (first x))))
    (assert (= 1 (aget a 0))))

  (println "Vector get")
  (let [x [1]
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (get x 0))))
    (assert (= 1 (aget a 0))))

  (println "Vector nth")
  (let [x [1]
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (nth x 0))))
    (assert (= 1 (aget a 0))))

  (println "Vector nth with default")
  (let [x [1]
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (nth x 0 nil))))
    (assert (= 1 (aget a 0))))

  (println "Vector .nth Java interop")
  (let [x [1]
        a (object-array 1)]
    (time
     (dotimes [_ 100000000]
       (aset a 0 (.nth x 0))))
    (assert (= 1 (aget a 0)))))


;; Matrix / GraphBLAS style breath first search
;; https://redislabs.com/redis-enterprise/technology/redisgraph/
;; MAGiQ http://www.vldb.org/pvldb/vol11/p1978-jamour.pdf
;; gSMat https://arxiv.org/pdf/1807.07691.pdf

(defn square-matrix ^DMatrixSparseCSC [size]
  (DMatrixSparseCSC. size size))

(defn row-vector ^DMatrixSparseCSC [size]
  (DMatrixSparseCSC. 1 size))

(defn col-vector ^DMatrixSparseCSC [size]
  (DMatrixSparseCSC. size 1))

(defn equals-matrix [^DMatrix a ^DMatrix b]
  (GenericMatrixOps_F64/isEquivalent a b 0.0))

(defn resize-matrix [^DMatrixSparse m new-size]
  (let [grown (square-matrix new-size)]
    (CommonOps_DSCC/extract m
                            0
                            (.getNumCols m)
                            0
                            (.getNumRows m)
                            grown
                            0
                            0)
    grown))

(defn ensure-matrix-capacity ^DMatrixSparseCSC [^DMatrixSparseCSC m size factor]
  (if (> (long size) (.getNumRows m))
    (resize-matrix m (* (long factor) (long size)))
    m))

(defn round-to-power-of-two [^long x ^long stride]
  (bit-and (bit-not (dec stride))
           (dec (+ stride x))))

(defn load-rdf-into-matrix-graph [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (let [{:keys [value->id
                  eid->matrix]}
          (->> (rdf/ntriples-seq in)
               (map rdf/rdf->clj)
               (reduce (fn [{:keys [value->id eid->matrix]} [s p o]]
                         (let [value->id (reduce (fn [value->id v]
                                                   (update value->id v (fn [x]
                                                                         (or x (count value->id)))))
                                                 value->id
                                                 [s p o])]
                           {:eid->matrix (update eid->matrix
                                                 p
                                                 (fn [x]
                                                   (let [s-id (long (get value->id s))
                                                         o-id (long (get value->id o))
                                                         size (count value->id)
                                                         m (if x
                                                             (ensure-matrix-capacity x size 2)
                                                             (square-matrix size))]
                                                     (doto m
                                                       (.unsafe_set s-id o-id 1.0)))))
                            :value->id value->id}))))
          max-size (round-to-power-of-two (count value->id) 64)]
      {:eid->matrix (->> (for [[k ^DMatrixSparseCSC v] eid->matrix]
                           [k (ensure-matrix-capacity v max-size 1)])
                         (into {}))
       :value->id value->id
       :id->value (->> (set/map-invert value->id)
                       (into (sorted-map)))
       :max-size max-size})))

(defn print-assigned-values [{:keys [id->value] :as graph} ^DMatrix m]
  (if (instance? DMatrixSparse m)
    (.printNonZero ^DMatrixSparse m)
    (.print m))
  (doseq [r (range (.getNumRows m))
          c (range (.getNumCols m))
          :when (= 1.0 (.unsafe_get m r c))]
    (if (= 1 (.getNumRows m))
      (prn (get id->value c))
      (prn (get id->value r) (get id->value c))))
  (prn))

;; TODO: Couldn't this be a vector in most cases?
(defn new-constant-matix ^DMatrixSparseCSC [{:keys [value->id max-size] :as graph} & vs]
  (let [m (square-matrix max-size)]
    (doseq [v vs
            :let [id (get value->id v)]
            :when id]
      (.unsafe_set m id id 1.0))
    m))

(defn transpose-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC m]
  (CommonOps_DSCC/transpose m nil nil))

(defn boolean-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC m]
  (dotimes [n (alength (.nz_values m))]
    (when (pos? (aget (.nz_values m) n))
      (aset (.nz_values m) n 1.0)))
  m)

(defn ^DMatrixSparseCSC assign-mask
  ([^DMatrixSparseCSC mask ^DMatrixSparseCSC w u]
   (assign-mask mask w u pos?))
  ([^DMatrixSparseCSC mask ^DMatrixSparseCSC w u pred]
   (assert (= 1 (.getNumCols mask) (.getNumCols w)))
   (dotimes [n (min (.getNumRows mask) (.getNumRows w))]
     (when (pred (.get mask n 0))
       (.set w n 0 (double u))))
   w))

(defn mask ^DMatrixSparseCSC [^DMatrixSparseCSC mask ^DMatrixSparseCSC w]
  (assign-mask mask w 0.0 zero?))

(defn inverse-mask ^DMatrixSparseCSC [^DMatrixSparseCSC mask ^DMatrixSparseCSC w]
  (assign-mask mask w 0.0 pos?))

(defn multiply-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC a ^DMatrixSparseCSC b]
  (doto (DMatrixSparseCSC. (.getNumRows a) (.getNumCols b))
    (->> (CommonOps_DSCC/mult a b))))

(defn or-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC a ^DMatrixSparseCSC b]
  (->> (multiply-matrix a b)
       (boolean-matrix)))

(defn multiply-elements-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC a ^DMatrixSparseCSC b]
  (let [c (DMatrixSparseCSC. (max (.getNumRows a) (.getNumRows b))
                             (max (.getNumCols a) (.getNumCols b)))]
    (CommonOps_DSCC/elementMult a b c nil nil)
    c))

(defn add-elements-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC a ^DMatrixSparseCSC b]
  (let [c (DMatrixSparseCSC. (max (.getNumRows a) (.getNumRows b))
                             (max (.getNumCols a) (.getNumCols b)))]
    (CommonOps_DSCC/add 1.0 a 1.0 b c nil nil)
    c))

(defn or-elements-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC a ^DMatrixSparseCSC b]
  (->> (add-elements-matrix a b)
       (boolean-matrix)))

;; NOTE: these return row vectors, which they potentially shouldn't?
;; Though the result of this is always fed straight into a diagonal.
(defn matlab-any-matrix ^DMatrixRMaj [^DMatrixSparseCSC m]
  (CommonOps_DDRM/transpose (CommonOps_DSCC/maxCols m nil) nil))

(defn matlab-any-matrix-sparse ^DMatrixSparseCSC [^DMatrixSparseCSC m]
  (let [v (col-vector (.getNumRows m))]
    (doseq [^long x (->> (.col_idx m)
                         (map-indexed vector)
                         (remove (comp zero? second))
                         (partition-by second)
                         (map ffirst))]
      (.unsafe_set v (dec x) 0 1.0))
    v))

(defn diagonal-matrix ^DMatrixSparseCSC [^DMatrix v]
  (let [target-size (.getNumRows v)
        m (square-matrix target-size)]
    (doseq [i (range target-size)
            :let [x (.unsafe_get v i 0)]
            :when (not (zero? x))]
      (.unsafe_set m i i x))
    m))

;; GraphBLAS Tutorial https://github.com/tgmattso/GraphBLAS

(defn graph-blas-tutorial []
  (let [num-nodes 7
        graph (doto (square-matrix num-nodes)
                (.set 0 1 1.0)
                (.set 0 3 1.0)
                (.set 1 4 1.0)
                (.set 1 6 1.0)
                (.set 2 5 1.0)
                (.set 3 0 1.0)
                (.set 3 2 1.0)
                (.set 4 5 1.0)
                (.set 5 2 1.0)
                (.set 6 2 1.0)
                (.set 6 3 1.0)
                (.set 6 4 1.0))
        vec (doto (col-vector num-nodes)
              (.set 2 0 1.0))]
    (println "Exercise 3: Adjacency matrix")
    (println "Matrix: Graoh =")
    (.print graph)

    (println "Exercise 4: Matrix Vector Multiplication")
    (println "Vector: Target node =")
    (.print vec)
    (println "Vector: sources =")
    (.print (multiply-matrix graph vec))

    (println "Exercise 5: Matrix Vector Multiplication")
    (let [vec (doto (col-vector num-nodes)
                (.set 6 0 1.0))]
      (println "Vector: source node =")
      (.print vec)
      (println "Vector: neighbours =")
      (.print (multiply-matrix (transpose-matrix graph) vec)))

    (println "Exercise 7: Traverse the graph")
    (let [w (doto (col-vector num-nodes)
              (.set 0 0 1.0))]
      (println "Vector: wavefront(src) =")
      (.print w)
      (loop [w w
             n 0]
        (when (< n num-nodes)
          ;; TODO: This should really use or and not accumulate.
          (let [w (or-matrix (transpose-matrix graph) w)]
            (println "Vector: wavefront =")
            (.print w)
            (recur w (inc n))))))

    (println "Exercise 9: Avoid revisiting")
    (let [w (doto (col-vector num-nodes)
              (.set 0 0 1.0))
          v (col-vector num-nodes)]
      (println "Vector: wavefront(src) =")
      (.print w)
      (loop [v v
             w w
             n 0]
        (when (< n num-nodes)
          (let [v (or-elements-matrix v w)]
            (println "Vector: visited =")
            (.print v)
            (let [w (inverse-mask v (or-matrix (transpose-matrix graph) w))]
              (println "Vector: wavefront =")
              (.print w)
              (when-not (MatrixFeatures_DSCC/isZeros w 0)
                (recur v w (inc n))))))))

    (println "Exercise 10: level BFS")
    (let [w (doto (col-vector num-nodes)
              (.set 0 0 1.0))
          levels (col-vector num-nodes)]
      (println "Vector: wavefront(src) =")
      (.print w)
      (loop [levels levels
             w w
             lvl 1]
        (let [levels (assign-mask w levels lvl)]
          (println "Vector: levels =")
          (.print levels)
          (let [w (inverse-mask levels (or-matrix (transpose-matrix graph) w))]
            (println "Vector: wavefront =")
            (.print w)
            (when-not (MatrixFeatures_DSCC/isZeros w 0)
              (recur levels w (inc lvl)))))))))

(def ^:const example-data-artists-resource "crux/example-data-artists.nt")

(defn example-data-artists-with-matrix [{:keys [eid->matrix id->value value->id max-size] :as graph}]
  (println "== Data")
  (doseq [[k ^DMatrixSparseCSC v] eid->matrix]
    (prn k)
    (print-assigned-values graph v))

  (println "== Query")
  (let [ ;; ?x :rdf/label "The Potato Eaters" -- backwards, so
        ;; transposing adjacency matrix.
        potato-eaters-label (multiply-matrix
                             (new-constant-matix graph "The Potato Eaters" "Guernica")
                             (transpose-matrix (:http://www.w3.org/2000/01/rdf-schema#label eid->matrix)))
        ;; Create mask for subjects. A mask is a diagonal, like the
        ;; constant matrix. Done to "lift" the new left hand side into
        ;; "focus".
        potato-eaters-mask (->> potato-eaters-label ;; TODO: Why transpose here in MAGiQ paper?
                                (matlab-any-matrix)
                                (diagonal-matrix))
        ;; ?y :example/creatorOf ?x -- backwards, so transposing adjacency
        ;; matrix.
        creator-of (multiply-matrix
                    potato-eaters-mask
                    (transpose-matrix (:http://example.org/creatorOf eid->matrix)))
        ;; ?y :foaf/firstName ?z -- forwards, so no transpose of adjacency matrix.
        creator-of-fname (multiply-matrix
                          (->> creator-of
                               (matlab-any-matrix)
                               (diagonal-matrix))
                          (:http://xmlns.com/foaf/0.1/firstName eid->matrix))]
    (print-assigned-values graph potato-eaters-label)
    (print-assigned-values graph potato-eaters-mask)
    (print-assigned-values graph creator-of)
    (print-assigned-values graph creator-of-fname)))


;; Other Matrix-format related spikes:

(defn mat-mul [^doubles a ^doubles b]
  (assert (= (alength a) (alength b)))
  (let [size (alength a)
        n (bit-shift-right size 1)
        c (double-array size)]
    (dotimes [i n]
      (dotimes [j n]
        (let [row-idx (* n i)
              c-idx (+ row-idx j)]
          (dotimes [k n]
            (aset c
                  c-idx
                  (+ (aget c c-idx)
                     (* (aget a (+ row-idx k))
                        (aget b (+ (* n k) j)))))))))
    c))

(defn vec-mul [^doubles a ^doubles x]
  (let [size (alength a)
        n (bit-shift-right size 1)
        y (double-array n)]
    (assert (= n (alength x)))
    (dotimes [i n]
      (dotimes [j n]
        (aset y
              i
              (+ (aget y i)
                 (* (aget a (+ (* n i) j))
                    (aget x j))))))
    y))

;; CSR
;; https://people.eecs.berkeley.edu/~aydin/GALLA-sparse.pdf

{:nrows 6
 :row-ptr (int-array [0 2 5 6 9 12 16])
 :col-ind (int-array [0 1
                      1 3 5
                      2
                      2 4 5
                      0 3 4
                      0 2 3 5])
 :values (double-array [5.4 1.1
                        6.3 7.7 8.8
                        1.1
                        2.9 3.7 2.9
                        9.0 1.1 4.5
                        1.1 2.9 3.7 1.1])}

;; DCSC Example
{:nrows 8
 :jc (int-array [1 7 8])
 :col-ptr (int-array [1 3 4 5])
 :row-ind (int-array [6 8 4 5])
 :values (double-array [0.1 0.2 0.3 0.4])}

(defn vec-csr-mul [{:keys [^long nrows ^ints row-ptr ^ints col-ind ^doubles values] :as a} ^doubles x]
  (let [n nrows
        y (double-array n)]
    (assert (= n (alength x)))
    (dotimes [i n]
      (loop [j (aget row-ptr i)
             yr 0.0]
        (if (< j (aget row-ptr (inc i)))
          (recur (inc i)
                 (+ yr
                    (* (aget values j)
                       (aget x (aget col-ind j)))))
          (aset y i yr))))
    y))

;; Z-curve spike, not fully tested.

(defn long->binary-str [^long x]
  (.replace (format "%64s" (Long/toBinaryString x)) \space \0))

;; http://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
(defn bit-spread-int ^long [^long x]
  (let [x (bit-and (bit-or x (bit-shift-left x 16)) 0x0000ffff0000ffff)
        x (bit-and (bit-or x (bit-shift-left x 8)) 0x00ff00ff00ff00ff)
        x (bit-and (bit-or x (bit-shift-left x 4)) 0x0f0f0f0f0f0f0f0f)
        x (bit-and (bit-or x (bit-shift-left x 2)) 0x3333333333333333)
        x (bit-and (bit-or x (bit-shift-left x 1)) 0x5555555555555555)]
    x))

(defn bit-unspread-int ^long [^long x]
  (let [x (bit-and x 0x5555555555555555)
        x (bit-and (bit-or x (bit-shift-right x 1)) 0x3333333333333333)
        x (bit-and (bit-or x (bit-shift-right x 2)) 0x0f0f0f0f0f0f0f0f)
        x (bit-and (bit-or x (bit-shift-right x 4)) 0x00ff00ff00ff00ff)
        x (bit-and (bit-or x (bit-shift-right x 8)) 0x0000ffff0000ffff)
        x (bit-and (bit-or x (bit-shift-right x 16)) 0xffffffff)]
    x))

;; NOTE: we're putting x before y dimension here.
(defn bit-interleave-ints ^long [^long x ^long y]
  (bit-or (bit-shift-left (bit-spread-int (Integer/toUnsignedLong (unchecked-int x))) 1)
          (bit-spread-int (Integer/toUnsignedLong (unchecked-int y)))))

(defn bit-uninterleave-ints ^ints [^long x]
  (int-array [(bit-unspread-int (bit-shift-right x 1))
              (bit-unspread-int x)]))

(defn bit-interleave-longs ^longs [^long x ^long y]
  (long-array [(bit-interleave-ints (unsigned-bit-shift-right x Integer/SIZE)
                                    (unsigned-bit-shift-right y Integer/SIZE))
               (bit-interleave-ints x y)]))

(defn bit-uninterleave-longs ^longs [^longs z]
  (let [z1s ^ints (bit-uninterleave-ints (aget z 0))
        z2s ^ints (bit-uninterleave-ints (aget z 1))]
    (long-array [(bit-or (bit-shift-left (Integer/toUnsignedLong (aget z1s 0)) Integer/SIZE)
                         (Integer/toUnsignedLong (aget z2s 0)))
                 (bit-or (bit-shift-left (Integer/toUnsignedLong (aget z1s 1)) Integer/SIZE)
                         (Integer/toUnsignedLong (aget z2s 1)))])))

;; range 27-102 (3,5)-(5,10), given 58 (7,4) should return litmax 55 (5,7) and bigmin 74 (3,8).
;; range 12-45 (2,2)-(6,3), given 19 (1,5) should return litmax 15 and bigmin 36.
;; We might only need one of them, as we're following a line, not
;; doing a range search.

;; From https://github.com/locationtech/geotrellis/tree/master/spark/src/main/scala/geotrellis/spark/io/index/zcurve
;; Apache License

;; NOTE: Only handles two ints stored in one long for now.  While
;; based on the code above, its all ultimately based on this paper and
;; the decision tables on page 76:
;; https://www.vision-tools.com/h-tropf/multidimensionalrangequery.pdf

(defn bit-at ^long [^long x ^long n]
  (bit-and (unsigned-bit-shift-right x n) 1))

(defn bits-over ^long [^long x]
  (bit-shift-left 1 (dec x)))

(defn bits-under ^long [^long x]
  (dec (bit-shift-left 1 (dec x))))

(def ^:const max-mask 0x7fffffff)
(def ^:const max-bits (dec Integer/SIZE))

(defn zload ^long [^long z ^long load ^long bits ^long dim]
  (let [mask (bit-not (bit-shift-left (bit-spread-int (unsigned-bit-shift-right max-mask (- max-bits bits))) dim))]
    (bit-or (bit-and z mask)
            (bit-shift-left (bit-spread-int load) dim))))

(defn zdiv ^longs [^long start ^long end ^long z]
  (loop [n Long/SIZE
         litmax 0
         bigmin 0
         start start
         end end]
    (if (zero? n)
      (long-array [litmax bigmin])
      (let [n (dec n)
            bits (inc (unsigned-bit-shift-right n 1))
            dim (bit-and n 1)
            zb (bit-at z n)
            sb (bit-at start n)
            eb (bit-at end n)]
        (cond
  ;;       case (0, 0, 0) =>
  ;;         // continue
          (and (= 0 zb) (= 0 sb) (= 0 eb))
          (recur n litmax bigmin start end)

  ;;       case (0, 0, 1) =>
  ;;         zmax   = load(zmax, under(bits), bits, dim)
  ;;         bigmin = load(zmin, over(bits), bits, dim)
          (and (= 0 zb) (= 0 sb) (= 1 eb))
          (recur n litmax (zload start (bits-over bits) bits dim) start (zload end (bits-under bits) bits dim))

  ;;       case (0, 1, 0) =>
  ;;         // sys.error(s"Not possible, MIN <= MAX, (0, 1, 0)  at index $i")
          (and (= 0 zb) (= 1 sb) (= 0 eb))
          (throw (IllegalStateException. "This case is not possible because MIN <= MAX. (0 1 0)"))

  ;;       case (0, 1, 1) =>
  ;;         bigmin = zmin
  ;;         return (litmax, bigmin)
          (and (= 0 zb) (= 1 sb) (= 1 eb))
          (long-array [litmax start])

  ;;       case (1, 0, 0) =>
  ;;         litmax = zmax
  ;;         return (litmax, bigmin)
          (and (= 1 zb) (= 0 sb) (= 0 eb))
          (long-array [end bigmin])

  ;;       case (1, 0, 1) =>
  ;;         litmax = load(zmax, under(bits), bits, dim)
  ;;         zmin = load(zmin, over(bits), bits, dim)
          (and (= 1 zb) (= 0 sb) (= 1 eb))
          (recur n (zload end (bits-under bits) bits dim) bigmin (zload start (bits-over bits) bits dim) end)

  ;;       case (1, 1, 0) =>
  ;;         // sys.error(s"Not possible, MIN <= MAX, (1, 1, 0) at index $i")
          (and (= 1 zb) (= 1 sb) (= 0 eb))
          (throw (IllegalStateException. "This case is not possible because MIN <= MAX. (1 1 0)"))

  ;;       case (1, 1, 1) =>
  ;;         // continue
          (and (= 1 zb) (= 1 sb) (= 1 eb))
          (recur n litmax bigmin start end))))))

(comment
  (assert (Arrays/equals (long-array [15 36])
                         ^longs (hakan/zdiv 12 45 19)))
  (assert (Arrays/equals (long-array [55 74])
                         ^longs (hakan/zdiv 27 102 58))))

(def ^:private ^:const max-unsigned-long (biginteger 18446744073709551615))

(def ^BigInteger biginteger-max-mask (biginteger 0xffffffffffffffffffffffffffffffff))
(def ^:const biginteger-bits Long/SIZE)

(defn biginteger-bits-over ^BigInteger [^long x]
  (.shiftLeft BigInteger/ONE (dec x)))

(defn biginteger-bits-under ^BigInteger [^long x]
  (.subtract (.shiftLeft BigInteger/ONE (dec x)) BigInteger/ONE))

(defn biginteger-bit-spread ^BigInteger [^BigInteger x]
  (let [l1 (biginteger (bit-spread-int (.intValue x)))
        l2 (biginteger (bit-spread-int (.intValue (.shiftRight x Integer/SIZE))))
        l3 (biginteger (bit-spread-int (.intValue (.shiftRight x (* 2 Integer/SIZE)))))
        l4 (biginteger (bit-spread-int (.intValue (.shiftRight x (* 3 Integer/SIZE)))))]
    (.or (.shiftLeft l4 (* 3 Long/SIZE))
         (.or (.shiftLeft l3 (* 2 Long/SIZE))
              (.or (.shiftLeft l2 Long/SIZE)
                   l1)))))

(defn biginteger-bit-unspread ^BigInteger [^BigInteger x]
  (let [l1 (biginteger (bit-unspread-int (.longValue x)))
        l2 (biginteger (bit-unspread-int (.longValue (.shiftRight x Long/SIZE))))
        l3 (biginteger (bit-unspread-int (.longValue (.shiftRight x (* 2 Long/SIZE)))))
        l4 (biginteger (bit-unspread-int (.longValue (.shiftRight x (* 3 Long/SIZE)))))]
    (.or (.shiftLeft l4 (* 3 Integer/SIZE))
         (.or (.shiftLeft l3 (* 2 Integer/SIZE))
              (.or (.shiftLeft l2 Integer/SIZE)
                   l1)))))

;; TODO: About 3-4 times slower than primitive version.

(defn biginteger-zload ^BigInteger [^BigInteger z ^BigInteger load ^long bits ^long dim]
  (let [mask (.not (.shiftLeft (biginteger-bit-spread (.shiftRight biginteger-max-mask (- biginteger-bits bits))) dim))]
    (.or (.and z mask)
         (.shiftLeft (biginteger-bit-spread load) dim))))

(defn biginteger-zdiv [^Number start ^Number end ^Number z]
  (let [start (biginteger start)
        end (biginteger end)
        z (biginteger z)]
    (loop [n (bit-shift-left Long/SIZE 1)
           litmax BigInteger/ZERO
           bigmin BigInteger/ZERO
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
            (recur n litmax (biginteger-zload start (biginteger-bits-over bits) bits dim) start (biginteger-zload end (biginteger-bits-under bits) bits dim))

            (and (= 0 zb) (= 1 sb) (= 0 eb))
            (throw (IllegalStateException. "This case is not possible because MIN <= MAX. (0 1 0)"))

            (and (= 0 zb) (= 1 sb) (= 1 eb))
            [litmax start]

            (and (= 1 zb) (= 0 sb) (= 0 eb))
            [end bigmin]

            (and (= 1 zb) (= 0 sb) (= 1 eb))
            (recur n (biginteger-zload end (biginteger-bits-under bits) bits dim) bigmin (biginteger-zload start (biginteger-bits-over bits) bits dim) end)

            (and (= 1 zb) (= 1 sb) (= 0 eb))
            (throw (IllegalStateException. "This case is not possible because MIN <= MAX. (1 1 0)"))

            (and (= 1 zb) (= 1 sb) (= 1 eb))
            (recur n litmax bigmin start end)))))))

(comment
  (assert (= [15 36] (hakan/biginteger-zdiv 12 45 19)))
  (assert (= [55 74] (hakan/biginteger-zdiv 27 102 58))))

;; From http://cppedinburgh.uk/slides/201603-zcurves.pdf
;; Compares real x,y coordinates in Z order without interleaving them
;; first.

(defn zless [^longs a ^longs b]
  (let [x-diff (bit-xor (aget a 0) (aget b 0))
        y-diff (bit-xor (aget a 1) (aget b 1))]
    (if (and (<= y-diff x-diff) (< y-diff (bit-xor x-diff y-diff)))
      (< (aget a 0) (aget b 0))
      (< (aget a 1) (aget b 1)))))

;; NOTE: Based on
;; https://github.com/hadeaninc/libzinc/blob/master/libzinc/AABB.hh
;; This file and project lacks license.

(def ^BigInteger morton-2-x-mask (biginteger 0x55555555555555555555555555555555))

;; NOTE: 51,193 should return 107,145. So this is like calling zdiv
;; with the mid point of min and max (122 in this case). That is, this
;; algorithm divides space in two parts. One can keep walking parts of
;; the curve by updating max to litmax or min to bigmin.

;; This is 3-4x faster (and a bit smaller - but have no explanation of
;; it, but might be possible to figure out by some tracing) than
;; biginteger-zdiv above, but might not be enough to be usable, as the
;; pivot point is assumed to be the middle? Note that we might not
;; need both litmax and bigmin. This is true for all implementations.

;; I think this might implement the building block to execute the
;; range search algorithm on left hand side of page 75 in
;; https://www.vision-tools.com/h-tropf/multidimensionalrangequery.pdf

(defn morton-get-next-address [^Number min ^Number max]
  (let [min (biginteger min)
        max (biginteger max)
        _ (log/debug :min min (.toString min 2))
        _ (log/debug :max max (.toString max 2))
        ;; Calculates the last common bit as seen from MSB, with
        ;; 1-based indexing from LSB. 6, 7 returns 2. 12, 13 return 2.
        ;; Returns 1 when there are no different bits. 2, 2 returns 1.
        ;  Returns the zero of the highest bit of the longest if there
        ;  is no common prefix. 2, 7 returns 4.
        index (- (inc (* 2 Long/SIZE)) (- (* 2 Long/SIZE) (.bitLength (.xor min max))))
        _ (log/debug :index index)

        ;; This creates a mask for un-spread value (half size) to
        ;; filter out the uncommon bits.
        mask (.not (.subtract (.shiftLeft BigInteger/ONE (quot index 2)) BigInteger/ONE))
        _ (log/debug :mask mask (.toString mask 2))
        ;; This is to increase the bit that differs (I think) in
        ;; un-spread space.
        inc (.shiftLeft BigInteger/ONE (dec (quot index 2)))
        _ (log/debug :inc inc (.toString inc 2))
        ;; Index is now dimension.
        index (bit-and 1 index)
        _ (log/debug :dimension index)
        ;; The shift right and un spread gives the value for the
        ;; dimension. This is masked and then increased.
        _ (log/debug :unspread-min (.toString (biginteger-bit-unspread (.shiftRight min index)) 2))
        _ (log/debug :unspread-min-masked (.toString (.and ^BigInteger (biginteger-bit-unspread (.shiftRight min index)) mask) 2))
        part (.add (.and ^BigInteger (biginteger-bit-unspread (.shiftRight min index)) mask) inc)
        _ (log/debug :part part (.toString part 2))
        ;; 0x5555 etc is every other bit set, with LSB set. 5 is 0b101
        ;; 0x55 is 0b1010101. This is shifted either 0 or 1 step to
        ;; the left and inverted to create a mask. Selects one of the
        ;; dimensions. Likely the other compared to what's in part
        ;; above. The inversion after shift left means that two zeros
        ;; can become two ones, so the mask always includes the LSB?
        _ (log/debug :inverted-morton-mask (.toString (.not (.shiftLeft morton-2-x-mask index)) 2))
        bigmin (.and min (.not (.shiftLeft morton-2-x-mask index)))
        _ (log/debug :min-masked bigmin (.toString bigmin 2))
        ;; Takes part, which is min for one dimension with the bit
        ;; that differs increased (and the below zeroed) out and ors
        ;; this with the original min value.
        _ (log/debug :spread-part (.toString (biginteger-bit-spread part) 2))
        _ (log/debug :spread-part-shifted (.toString (.shiftLeft (biginteger-bit-spread part) index) 2))
        bigmin (.or bigmin (.shiftLeft (biginteger-bit-spread part) index))
        _ (log/debug :bigmin bigmin (.toString bigmin 2))

        ;; Subtract one and do the similar steps for max.
        part (.subtract part BigInteger/ONE)
        _ (log/debug :part-dec part (.toString part 2))
        litmax (.and max (.not (.shiftLeft morton-2-x-mask index)))
        _ (log/debug :max-masked litmax (.toString litmax 2))
        _ (log/debug :spread-part (.toString (biginteger-bit-spread part) 2))
        _ (log/debug :spread-part-shifted (.toString (.shiftLeft (biginteger-bit-spread part) index) 2))
        litmax (.or litmax (.shiftLeft (biginteger-bit-spread part) index))
        _ (log/debug :litmax litmax (.toString litmax 2))]
    [litmax bigmin]))

(comment
  (assert (= [107 145] (morton-get-next-address 51 193)))
  (assert (= [63 98] (morton-get-next-address 51 107)))
  (assert (= [99 104] (morton-get-next-address 98 107)))
  (assert (= [149 192] (morton-get-next-address 145 193))))

(defn interleaved-longs->morton-number ^java.math.BigInteger [^longs z]
  (.or (.shiftLeft (biginteger (aget z 0)) Long/SIZE)
        (biginteger (aget z 1))))

(defn morton-number->interleaved-longs ^longs [^BigInteger z]
  (long-array [(.longValue (.shiftRight z Long/SIZE))
               (.longValue z)]))

(defn morton-within-range? [^Number min ^Number max ^Number z]
  (let [min (biginteger min)
        max (biginteger max)
        z (biginteger z)
        zx (.and z morton-2-x-mask)
        zy (.and z (.shiftLeft morton-2-x-mask 1))]
    (and (not (pos? (.compareTo (.and min morton-2-x-mask) zx)))
         (not (pos? (.compareTo (.and min (.shiftLeft morton-2-x-mask 1)) zy)))
         (not (pos? (.compareTo zx (.and max morton-2-x-mask))))
         (not (pos? (.compareTo zy (.and max (.shiftLeft morton-2-x-mask 1))))))))

;; Example of how this would actually be used in Crux. Coordinates
;; here would really be the reversed dates. Max (63 in this example)
;; would be (.longValue 0xffffffffffffffff). 0 represents the future,
;; and new values in times shrinks towards it.

;; See https://en.wikipedia.org/wiki/Z-order_curve#Use_with_one-dimensional_data_structures_for_range_searching

;; Say time we look for is 12, max is 63. We find 16 via a seek, above
;; 12, but outside the box:

;; (hakan/morton-within-range? 12 63 16)
;; false

;; We then divide the box based on 16:

;; (hakan/zdiv 12 63 16)
;; [15, 24]

;; This means the box continues below between 12 and 15 and above
;; between 24 and 63. As we did seek for 12 but found 16 we know the
;; lower box is empty, and can seek for 24 to continue. Say we find
;; 33:

;; (hakan/morton-within-range? 12 63 33)
;; false
;; (hakan/zdiv 12 63 33)
;; [31, 36]

;; Again we can skip the lower box 12 and 31 as this is below 33, but
;; continue back in range between 36 and 63. Say you now seek and find
;; 39, this is within the box:

;; (hakan/morton-within-range? 12 63 39)
;; true

;; 39 is our answer. Or do need to sanity check we haven't skipped
;; anything (in the lower boxes)?

;; http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.144.505&rep=rep1&type=pdf

;; Algorithm 3 Algorithm to calculate the Z-order next-match

;; 1: next-match <= page-key

;; 2: check each coordinate in the page-key-point to find the highest
;; bit in the next-match which must change:

;; (a) from 1 to 0 because a page-key-point coordinate > the
;; corresponding coordinate in the range upper bound or

;; (b) from 0 to 1 because a page-key-point coordinate < the
;; corresponding coordinate in the range lower bound

;; 3: where the highest bit to change arises from condition (a) in
;; some coordinate:

;; find the lowest but higher bit in the next-match determined by some
;; other coordinate which can be changed from 0 to 1, such that if all
;; lower bits are set to 0, the coordinate value remains leq the
;; corresponding coordinate in the range upper bound

;; 4: set to 1 the bit identified in the next-match as being the
;; highest zero-valued bit which must be incremented

;; 5: set all lower bits in the next-match to 0

;; 6: for each coordinate of the point corresponding to the
;; next-match, if any is < the corresponding coordinate of the range
;; lower bound, then increment the corresponding ts in the next-match
;; to the values in the range lower bound

;; TODO: Try
;; https://www.research-collection.ethz.ch/bitstream/handle/20.500.11850/123617/eth-50204-01.pdf

;; NOTE: these don't work, they should operate on the "h" path into
;; the tree somehow, which is z chopped up in k (2) bits. It's not
;; clear in the paper how one actually makes that step. If they did
;; work, this should be enough for the int case, but we need longs.

;; There are tests seemingly related to the paper here, but they don't
;; show the relation between z and h:
;; https://github.com/tzaeschke/phtree/blob/master/src/test/java/ch/ethz/globis/phtree/bits/TestIncSuccessor.java

;; ;; isInI
;; (defn within-range-ints? [^long start ^long end ^long z]
;;   (= (bit-and (bit-or z start) end) z))

;; ;; inc, z has to already be in range.
;; (defn next-in-range [^long start ^long end ^long z]
;;   (let [next-z (bit-or (bit-and (inc (bit-or z (bit-not end))) end) start)]
;;     (if (<= next-z z)
;;       -1
;;       next-z)))

;; ;; succ, z can be anywhere.
;; (defn next-within-range [^long start ^long end ^long z]
;;   (let [mask-start (dec (Long/highestOneBit (bit-or (bit-and (bit-not z) start) 1)))
;;         end-high-bit (Long/highestOneBit (bit-or (bit-and z (bit-not end)) 1))
;;         mask-end (dec end-high-bit)
;;         next-z (bit-or z (bit-not end))
;;         next-z (bit-and next-z (bit-not (bit-or mask-start mask-end)))
;;         next-z (+ next-z (bit-and end-high-bit (bit-not mask-start)))]
;;     (bit-or (bit-and next-z end) start)))

;; Graal:
;; https://www.innoq.com/en/blog/native-clojure-and-graalvm/
;; https://github.com/taylorwood/lein-native-image
;; https://www.graalvm.org/docs/getting-started/

;; export JAVA_HOME=~/opt/graalvm-ce-1.0.0-rc12/
;; export PATH=$JAVA_HOME/bin:$PATH
;; CRUX_DISABLE_LIBGCRYPT=true CRUX_DISABLE_LIBCRYPTO=true lein with-profile graal,uberjar uberjar
;; native-image --no-server -H:+ReportExceptionStackTraces -H:+ReportUnsupportedElementsAtRuntime -H:ReflectionConfigurationFiles=~/dev/crux/resources/graal_reflectconfig.json -H:EnableURLProtocols=http -H:IncludeResources='.*/.*properties$' -H:IncludeResources='.*/.*so$' -H:IncludeResources='.*/.*xml$' -Dclojure.compiler.direct-linking=true -jar ./crux-0.1.0-SNAPSHOT-standalone.jar

;; Priority Search Tree:

;; Based on
;; http://cs.brown.edu/courses/cs252/misc/resources/lectures/pdf/notes07.pdf
;; https://pdfs.semanticscholar.org/ce68/4914d2ee4c870db16a2edc2dbceca4c2ad2c.pdf
;; See also https://github.com/michalmarczyk/psq.clj
;; https://github.com/spratt/PrioritySearchTree

(def example-pst
  [[[1 8] :n]
   [[9 -3] :m]
   [[10 -2] :e]
   [[4 0] :k]
   [[6 4] :g]
   [[12 1] :c]
   [[2 3] :j]
   [[7 6] :h]
   [[16 2] :b]
   [[14 -1] :d]
   [[-1 9] :f]
   [[-2 5] :i]
   [[15 7] :a]])

(def expected-pst
  [[[-1 9] :f 8]
   [[[1 8] :n 3]
    [[[-2 5] :i -2] [[[2 3] :j 2]]]
    [[[7 6] :h 5] [[[4 0] :k 4]] [[[6 4] :g 6]]]]
   [[[15 7] :a 11]
    [[[10 -2] :e 10] [[[9 -3] :m 9]]]
    [[[16 2] :b 13] [[[12 1] :c 12]] [[[14 -1] :d 14]]]]])

(defn build-priority-search-tree [kvs]
  (let [kvs (sort-by (comp second first) kvs)
        [[x y] :as p] (last kvs)
        s (disj (set kvs) p)]
    (let [s (vec (sort-by ffirst s))
          [lower upper] (split-at (quot (count s) 2) s)
          x-of-p (if (<= (count s) 1)
                   x
                   (quot (+ (long (ffirst (last lower)))
                            (long (ffirst (first upper))))
                         2))]
      (cond-> [(conj p x-of-p)]
        (seq lower) (conj (build-priority-search-tree lower))
        (seq upper) (conj (build-priority-search-tree upper))))))

(def expected-pst-result
  [[[1 8] :n 3]
   [[7 6] :h 5]])

(defn query-priority-search-tree [[[[x y] k x-of-p :as node] l r :as tree] x-min y-min x-max]
  (when (and node (>= (double y) (double y-min)))
    (concat
     (when (<= x-min x x-max)
       [node])
     (when (< (double x-min) (double x-of-p))
       (query-priority-search-tree l x-min y-min x-max))
     (when (> (double x-max) (double x-of-p))
       (query-priority-search-tree r x-min y-min x-max)))))

(comment
  (= expected-pst-result
     (query-priority-search-tree
      (build-priority-search-tree example-pst)
      0 4.5 11)))
