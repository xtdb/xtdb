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
         (let [[a b c] three-five-bit-chars]
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
     (if (= 0x80 (bit-and 0x80 (first bs)))
       (let [[x y & bs] bs
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
            [n sub-s-idx] (loop [n 0x1F]
                            (when (> n 2)
                              (let [sub-s (subs s idx (min (+ n idx) (count s)))]
                                (if-let [idx (clojure.string/index-of prefix-s sub-s)]
                                  [(count sub-s) idx]
                                  (recur (dec n))))))]
        (if (and sub-s-idx (pos? sub-s-idx))
          (recur (+ idx (long n))
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
    (let [[[ia :as an] [ib :as bn] & pq] pq]
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
