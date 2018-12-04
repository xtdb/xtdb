(ns hakan
  (:require [clojure.spec.alpha :as s]
            [clojure.java.io :as io]
            [clojure.core.matrix :as m]
            [crux.rdf :as rdf])
  (:import [org.ejml.data DMatrix DMatrixSparseCSC DMatrixSparseTriplet]
           org.ejml.generic.GenericMatrixOps_F64
           org.ejml.sparse.csc.CommonOps_DSCC
           org.ejml.ops.ConvertDMatrixStruct
           org.ejml.equation.Equation))

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

(defn bit-str->bitset [s]
  (let [bs (java.util.BitSet.)
        bits (->> s
                  (remove #{\space})
                  (vec))]
    (dotimes [n (count bits)]
      (when (= \1 (get bits n))
        (.set bs n true)))
    bs))

;; TODO: Obviously slow, should use proper data structure.
(defn bitset-rank ^long [^java.util.BitSet bs ^long n]
  (loop [n (.previousSetBit bs n)
         rank 0]
    (if (= -1 n)
      rank
      (recur (.previousSetBit bs (dec n)) (inc rank)))))

(defn power-of? [^long x ^long y]
  (if (zero? (rem x y))
    (recur (quot x y) y)
    (= 1 x)))

(defn next-power-of ^long [^long x ^long y]
  (long (Math/pow 2 (long (Math/pow y (Math/log x))))))

(defn new-static-k2-tree [^long n ^long k tree-bit-str leaf-bit-str]
  {:n (if (power-of? n k)
        n
        (next-power-of n k))
   :k k
   :k2 (long (Math/pow k 2))
   :t (bit-str->bitset tree-bit-str)
   :t-size (->> tree-bit-str
                (remove #{\space})
                (count))
   :l (bit-str->bitset leaf-bit-str)})

;; http://repositorio.uchile.cl/bitstream/handle/2250/126520/Compact%20representation%20of%20Webgraphs%20with%20extended%20functionality.pdf?sequence=1
;; NOTE: Redefined in terms of k2-tree-range below.
(defn k2-tree-check-link? [{:keys [^long n
                                   ^long k
                                   ^long k2
                                   ^long t-size
                                   ^java.util.BitSet t
                                   ^java.util.BitSet l] :as k2-tree} ^long row ^long col]
  (loop [n n
         p row
         q col
         z -1]
    (if (>= z t-size)
      (.get l (- z t-size))
      (if (or (= -1 z) (.get t z))
        (let [n (quot n k)
              y (* (bitset-rank t z) k2)
              y (+ y
                   (* (Math/abs (quot p n)) k)
                   (Math/abs (quot q n)))]
          (recur n
                 (long (mod p n))
                 (long (mod q n))
                 y))
        false))))

;; NOTE: All elements in a row.
(defn k2-tree-succsessors [{:keys [^long n
                                   ^long k
                                   ^long k2
                                   ^long t-size
                                   ^java.util.BitSet t
                                   ^java.util.BitSet l] :as k2-tree} ^long row]
  ((fn step [^long n ^long p ^long q ^long z]
     (if (>= z t-size)
       (when (.get l (- z t-size))
         [q])
       (when (or (= -1 z) (.get t z))
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
                                    ^java.util.BitSet t
                                    ^java.util.BitSet l] :as k2-tree} ^long col]
  ((fn step [^long n ^long q ^long p ^long z]
     (if (>= z t-size)
       (when (.get l (- z t-size))
         [p])
       (when (or (= -1 z) (.get t z))
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
                             ^java.util.BitSet t
                             ^java.util.BitSet l] :as k2-tree} row1 row2 col1 col2]
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
         (when (.get l (- z t-size))
           [[dp dq]])
         (when (or (= -1 z) (.get t z))
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
            "0011 0011 0010 0010 0001 0010 0100 0010 1000 0010 1010")]
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
     (k2-tree-predecessors k2 6)]))

;; Matrix / GraphBLAS style breath first search
;; https://redislabs.com/redis-enterprise/technology/redisgraph/

;; Using core.matrix for simplicity for now to explore the algorithms.

(def adjacency-matrix
  (m/matrix [[0 0 0 1 0 0 0]
             [1 0 0 0 0 0 0]
             [0 0 0 1 0 1 1]
             [1 0 0 0 0 0 1]
             [0 1 0 0 0 0 1]
             [0 0 1 0 1 0 0]
             [0 1 0 0 0 0 0]]))

;; Breath first search, looking for neighbours of elements 1 and 3 (0
;; and 2 in zero-based indexing).
(def bfs-mask (m/matrix [1 0 1 0 0 0 0]))

;; One hop.
(assert (= (m/matrix [0.0 1.0 0.0 1.0 0.0 1.0 0.0])
           (m/mmul adjacency-matrix bfs-mask)))

;; Two hops, value is number of in-edges.
(assert (= (m/matrix [1.0 0.0 2.0 0.0 1.0 0.0 1.0])
           (m/mmul adjacency-matrix adjacency-matrix bfs-mask)))

;; Breath first search, looking for neighbours of elements 1, 3 and 4
;; with individual results.
(def multiple-source-bfs-mask
  (m/matrix [[0 1 0]
             [0 0 0]
             [0 0 1]
             [1 0 0]
             [0 0 0]
             [0 0 0]
             [0 0 0]]))

(assert (= (m/matrix [[1.0 0.0 0.0]
                      [0.0 1.0 0.0]
                      [1.0 0.0 0.0]
                      [0.0 1.0 0.0]
                      [0.0 0.0 0.0]
                      [0.0 0.0 1.0]
                      [0.0 0.0 0.0]])
           (m/mmul adjacency-matrix multiple-source-bfs-mask)))

;; MAGiQ http://www.vldb.org/pvldb/vol11/p1978-jamour.pdf

(def magiq (m/matrix [[0 2 1 0 1]
                      [0 0 0 2 2]
                      [0 0 0 3 0]
                      [0 0 0 0 0]
                      [0 0 5 5 0]]))
;; a b c e = 1 2 3 5

;; SELECT ?x ?y ?z ?w WHERE {
;; ?x <a> ?y .
;; ?y <c> ?z .
;; ?x <b> ?w .
;; }

;; Mxy = I ∗ a ⊗ A
;; Myz = diag(any(M'xy)) ∗ c ⊗ A
;; Mxy = Mxy × diag(any(Myz))
;; Mxw = diag(any(Mxy)) ∗ b ⊗ A

;; "The first line selects the valid bindings of variables x and y
;; using predicate a from the RDF matrix A, and stores the results in
;; matrix Mxy. The second line uses the bindings of y and predicate c
;; to select the bindings of z.  The third line updates the bindings
;; of x and y to eliminate bindings invalidated by predicate
;; c. Finally, the fourth line uses the bindings of x in Mxy with
;; predicate b to select the valid bindings of w."

;; "The undirected version of the query graph is traversed in a
;; depth-first fashion"

;; "Forward edges are translated to RDF selection operations that
;; produce the binding matrix for the variables of the query
;; edge. Backward edges are translated to selection operations that
;; filter out invalid variable bindings"

;; Determine if any array elements are nonzero.
(defn matlab-any [m]
  (->> (m/columns m)
       (mapv #(if (some pos? %) 1.0 0.0))
       (m/matrix)))

(assert (= (m/matrix [0.0 0.0 1.0])
           (matlab-any [[0 0 3]
                        [0 0 3]
                        [0 0 3]])))

;; Creates a sparse matrix with matlab syntax, cols, rows, vals, rows,
;; cols
(defn matlab-sparse [i j v m n]
  (-> (m/zero-matrix m n)
      (m/set-indices (map vector i j)
                     (map double v))
      (m/sparse)))

(assert (= (m/matrix [[0.0 0.0 0.0]
                      [0.0 0.0 0.0]
                      [0.0 1.0 0.0]])
           (matlab-sparse [2] [1] [1] 3 3)))

;; This example is LUBM query 2 written with clause order.

;; SELECT ?x, ?y, ?z
;; WHERE
;; { ?z ub:subOrganizationOf ?y .
;;   ?z rdf:type ub:Department .
;;   ?x ub:memberOf ?z .
;;   ?x rdf:type ub:GraduateStudent .
;;   ?x ub:undergraduateDegreeFrom ?y
;;   ?y rdf:type ub:University .
;; }

;; This is a GUESS based on the Matlab translation in the
;; paper. Should be possible to play with in the REPL if one loads
;; some real LUBM data into matrix form. This is taken from the
;; screenshot on page 4. This also includes the graph form of the
;; query above and the order its walked.

;; G is a list of matrixes, one per predicate. N is dimension (I think).
;; [G, N] = load_rdf_graph('data/lubm2560.nt');

;; This creates a matrix with a single cell set, GradStud, and selects
;; the entries in G{type} with this value into M_01 (GradStud- > ?x).
;; GradStud and type are ids of predicates I think.
;; M_01 = sparse([GradStud], [GradStud], [1], N, N) * G{type};

;; (def m_01 (m/mmul (matlab-sparse [GradStud] [GradStud] [1.0] n n)
;;                   (get g type)))

;; Transposes M_01 (') any returns one for every row with a column set
;; and creates a diagonal matrix to select elements from G{uGradFrom}
;; based on ?x into M_12 (?x -> ?y).
;; M_12 = diag(any(M_01')) * G{uGradFrom};

;; (def m_12 (m/mmul (m/diagonal-matrix (matlab-any (m/transpose m_01)))
;;                   (get g uGradFrom)))

;; Transposes M_12 and selects like above, navigates from ?y to its types.
;; M_24 = diag(any(M_12')) * G{type};

;; (def m_24 (m/mmul (m/diagonal-matrix (matlab-any (m/transpose m_12)))
;;                   (get g type)))

;; Select the types with Univ. M_24 (?y -> Univ)
;; M_24 = M_24 * sparse([Univ], [Univ], [1], N, N);

;; (def m_24 (m/mmul m24 (matlab-sparse [Univ] [Univ] [1.0] n n)))

;; Navigates from ?y to subOrgOf to M_25 (?z -> ?y) as the navigation is
;; backwards there's no transpose (I think).
;; M_25 = diag(any(M_24)) * G{subOrgOf};

;; (def m_25 (m/mmul (m/diagonal-matrix (matlab-any m_24))
;;                   (get g subOrgOf)))

;; Updates ?x -> ?y, I think to ensure it contains the same links to
;; ?y as M_25 (?z -> ?y).
;; M_12 = M_12 * diag(any(M_25));

;; (def m_12 (m/mmul m_12 (m/diagonal-matrix (matlab-any m_25))))

;; Navigate to M_13 (?x -> ?z).
;; M_13 = diag(any(M_12)) * G{memberOf};

;; (def m_13 (m/mmul (m/diagonal-matrix (matlab-any m_12))
;;                   (get g memberOf)))

;; Selects the ?z with type Dept into M_36 (?z -> Dept)
;; M_36 = diag(any(M_13')) * G{type};

;; (def m_36 (m/mmul (m/diagonal-matrix (matlab-any (m/transpose m_13)))
;;                   (get g type)))

;; M_36 = M_36 * sparse([Dept], [Dept], [1], N, N);

;; (def m_36 (m/mmul m36 (matlab-sparse [Dept] [Dept] [1.0] n n)))

;; Updates ?x -> ?z based on Dept to ensure they contain the same ?z.
;; M_13 = M_13 * diag(any(M_36));

;; (def m_13 (m/mmul m_13 (m/diagonal-matrix (matlab-any m_36))))

;; Updates GradStud -> ?x based on ?x -> ?z to ensure they contain the
;; same ?x.
;; M_01 = M_01 * diag(any(M_13));

;; (def m_01 (m/mmul m_01 (m/diagonal-matrix (matlab-any m_13))))

;; print_results({M_01, M_12, M_24, M_25, M_13, M_36});

;; (m/pm m_01)
;; (m/pm m_12)
;; (m/pm m_24)
;; (m/pm m_25)
;; (m/pm m_13)
;; (m/pm m_36)

;; Using EJML instead of core.matrix

(defn core-matrix->ejml-sparse-matrix ^DMatrixSparseCSC [vm]
  (let [[r c] (m/shape vm)
        m (DMatrixSparseCSC. r (or c 1))]
    (m/emap-indexed (fn [[r c] x]
                      (.set m r (or c 0) x))
                    vm)
    m))

(let [m (DMatrixSparseCSC. 0 0)]
  (CommonOps_DSCC/mult
   (core-matrix->ejml-sparse-matrix adjacency-matrix)
   (core-matrix->ejml-sparse-matrix bfs-mask)
   m)
  (assert
   (GenericMatrixOps_F64/isEquivalent
    (core-matrix->ejml-sparse-matrix [0.0 1.0 0.0 1.0 0.0 1.0 0.0])
    m
    0)))

(let [e (doto (Equation.)
          (.alias (core-matrix->ejml-sparse-matrix adjacency-matrix) "A")
          (.alias (core-matrix->ejml-sparse-matrix bfs-mask) "x"))
      s (.compile e "y = A * x")]
  (.perform s)
  (GenericMatrixOps_F64/isEquivalent
   (core-matrix->ejml-sparse-matrix [0.0 1.0 0.0 1.0 0.0 1.0 0.0])
   (.lookupDDRM e "y")
   0))

(def ^:const lubm-triples-resource "lubm/University0_0.ntriples")

(defn load-rdf-into-matrix [resource]
  (let [value->id (atom {})
        id->matrix (atom {})]
    (with-open [in (io/input-stream (io/resource resource))]
      (doseq [[s p o] (map rdf/statement->clj (rdf/ntriples-seq in))]
        (doseq [v [s p o]]
          (swap! value->id
                 update
                 v
                 (fn [x]
                   (or x (count @value->id)))))
        (swap! id->matrix
               update
               p
               (fn [x]
                 (let [^DMatrixSparseCSC m  (or x (DMatrixSparseCSC. 1000 1000))
                       size (long (max (.getNumRows m)
                                       (long (dec (count @value->id)))
                                       (inc (long (get @value->id s)))
                                       (inc (long (get @value->id o)))))
                       m (if (> size (.getNumRows m))
                           (let [m2 (DMatrixSparseCSC. (* 2 size) (* 2 size))]
                             (CommonOps_DSCC/extract m
                                                     0
                                                     (.getNumCols m)
                                                     0
                                                     (.getNumRows m)
                                                     m2
                                                     0
                                                     0)
                             m2)
                           m)]
                   (.set m
                         (int (get @value->id s))
                         (int (get @value->id o))
                         1.0)
                   m)))))
    (let [max-size (->> (for [[_ ^DMatrixSparseCSC v] @id->matrix]
                          (.getNumRows v))
                        (reduce max))
          stride 1024
          max-size (bit-and (bit-not (dec stride))
                            (dec (+ stride (long max-size))))]
      {:id->matrix (->> (for [[k ^DMatrixSparseCSC v] @id->matrix
                              :let [m2 (DMatrixSparseCSC. max-size max-size)]]
                          (do (CommonOps_DSCC/extract v
                                                      0
                                                      (.getNumCols v)
                                                      0
                                                      (.getNumRows v)
                                                      m2
                                                      0
                                                      0)
                              [k m2]))
                        (into {}))
       :value->id @value->id})))
