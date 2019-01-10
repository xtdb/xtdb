(ns hakan
  (:require [clojure.spec.alpha :as s]))

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

;; For reference, single run, same queries (if no errors):
;; Sail: 25872 113.47368421052632
;; Datomic: 129908 572.2819383259912
;; Neo4j: 7153 31.1
;; Neo4j, lein test: 18514 80.49565217391304

(comment
  (def c (read-string (slurp "test/watdiv/watdiv_crux.edn")))
  (swap! (:cache-state (:kv-store system)) empty)
  (doseq [{:keys [idx query crux-results crux-time]} (remove #(> 1000 (:crux-time %)) (remove :crux-error c))]
    (prn :idx idx)
    (prn query)
    (prn :previous-results crux-results)
    (prn :prevous-time crux-time)
    (dotimes [n 3]
      (let [start (System/currentTimeMillis)]
        (assert (= crux-results
                   (count (.q (.db system)
                              (crux.sparql/sparql->datalog query)))))
        (prn :run n (- (System/currentTimeMillis) start)))))

  (swap! (:cache-state (:kv-store system)) empty)

  (crux.query/query-plan-for (crux.sparql/sparql->datalog
                              "")
                             (crux.index/read-meta (:kv-store system) :crux.kv/stats))

  (let [total (atom 0)
        qs (remove :crux-error c)]
    (doseq [{:keys [idx query crux-results crux-time]} qs]
      (prn :idx idx)
      (prn query)
      (prn :previous-results crux-results)
      (prn :prevous-time crux-time)
      (dotimes [n 1]
        (let [start (System/currentTimeMillis)]
          (assert (= crux-results
                     (count (.q (.db system)
                                (crux.sparql/sparql->datalog query)))))
          (let [t (- (System/currentTimeMillis) start)]
            (swap! total + t)
            (prn :run n t)))))
    (prn @total (/ @total (double (count qs))))))

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
