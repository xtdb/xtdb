(ns core2.sql
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.instant :as i]
            [clojure.set :as set]
            [clojure.string :as s]
            [instaparse.core :as insta])
  (:import [java.util Date]
           [java.time Duration LocalTime Period ZoneOffset]
           [java.time.temporal TemporalAmount]))

(set! *unchecked-math* :warn-on-boxed)

(defn- parse-string [x]
  (s/replace (subs x 1 (dec (count x)))
             "''"
             "'"))

(defn- parse-date [x]
  (i/read-instant-date (s/replace (parse-string x) " " "T")))

(defn- parse-time [x]
  (LocalTime/parse (parse-string x)))

(defn- parse-number [x]
  (edn/read-string x))

(defn- parse-interval [x [field]]
  (let [x (parse-number (parse-string x))]
    (case field
      :year (Period/ofYears x)
      :month (Period/ofMonths x)
      :day (Period/ofDays x)
      :hour (Duration/ofHours x)
      :minute (Duration/ofMinutes x))))

(defn- parse-identifier [x]
  (symbol x))

(defn- parse-boolean [[x]]
  (case x
    :true true
    :false false
    :unknown nil))

(defn- parse-like-pattern [x & [escape]]
  (let [pattern (parse-string x)
        escape (or (some-> escape (parse-string)) "\\")
        regex (if (= "\\" escape)
                (-> pattern
                    (s/replace #"([^\\]|^)(_)" "$1.")
                    (s/replace #"([^\\]|^)(%)" "$1.*")
                    (s/replace "\\_" "_")
                    (s/replace "\\%" "%"))
                (-> pattern
                    (s/replace (re-pattern (str "([^"
                                                escape
                                                "]|^)(_)"))
                               "$1.")
                    (s/replace (re-pattern (str "([^"
                                                escape
                                                "]|^)(%)"))
                               "$1.*")
                    (s/replace (str escape "_") "_")
                    (s/replace (str escape "%") "%")))]
    (re-pattern (str "^" regex "$"))))

(def parse-sql
  (insta/parser (io/resource "core2/sql/sql.ebnf")
                :auto-whitespace (insta/parser "whitespace = #'\\s+' | #'\\s*--[^\r\n]*\\s*' | #'\\s*/[*].*([*]/\\s*|$)'")
                :string-ci true))

(def ^:private literal-transform
  {:boolean-literal parse-boolean
   :numeric-literal parse-number
   :unsigned-numeric-literal parse-number
   :date-literal parse-date
   :time-literal parse-time
   :timestamp-literal parse-date
   :interval-literal parse-interval
   :string-literal parse-string
   :like-pattern parse-like-pattern
   :identifier parse-identifier})

(set! *unchecked-math* false)

(def ^:private constant-folding-transform
  {:numeric-minus (fn
                    ([x]
                     (if (number? x)
                       (- x)
                       [:numeric-minus x]))
                    ([x y]
                     (cond
                       (and (instance? Date x)
                            (instance? TemporalAmount y))
                       (Date/from (.toInstant (.minus (.atOffset (.toInstant ^Date x) ZoneOffset/UTC) ^TemporalAmount y)))
                       (and (double? x) (double? y))
                       (double (- (bigdec x) (bigdec y)))
                       (and (number? x) (number? y))
                       (- x y)
                       :else
                       [:numeric-minus x y])))
   :numeric-plus (fn [x y]
                   (cond
                     (and (instance? Date x)
                          (instance? TemporalAmount y))
                     (Date/from (.toInstant (.plus (.atOffset (.toInstant ^Date x) ZoneOffset/UTC) ^TemporalAmount y)))
                     (and (double? x) (double? y))
                     (double (+ (bigdec x) (bigdec y)))
                     (and (number? x) (number? y))
                     (+ x y)
                     :else
                     [:numeric-plus x y]))
   :numeric-multiply (fn [x y]
                       (if (and (number? x) (number? y))
                         (* x y)
                         [:numeric-multiply x y]))
   :numeric-divide (fn [x y]
                     (if (and (number? x) (number? y))
                       (/ x y)
                       [:numeric-divide x y]))
   :numeric-modulo (fn [x y]
                     (if (and (number? x) (number? y))
                       (rem x y)
                       [:numeric-modulo x y]))
   :like-exp (fn
               ([x pattern]
                [:like-exp x pattern])
               ([x not pattern]
                [:boolean-not [:like-exp x pattern]]))
   :null-exp (fn
               ([x]
                [:null-exp x])
               ([x no]
                [:boolean-not [:null-exp x]]))
   :in-exp (fn
               ([x y]
                [:in-exp x y])
               ([x not y]
                [:boolean-not [:in-exp x y]]))
   :in-value-list hash-set
   :routine-invocation (fn [f & args]
                         (case f
                           'date (apply i/read-instant-date args)
                           `[:routine-invocation ~f ~@args]))
   :table-exp (fn [x]
                [:select-exp [:select :star] [:from x]])
   :between-exp (fn
                  ([v x y]
                   [:boolean-and
                    [:comp-ge v x]
                    [:comp-le v y]])
                  ([v not x y]
                   [:boolean-or
                    [:comp-lt v x]
                    [:comp-gt v y]]))})

(set! *unchecked-math* :warn-on-boxed)

(def ^:private nary-transform
  {:boolean-and (fn [x y]
                  (cond
                    (and (vector? x)
                         (= :boolean-and (first x))
                         (vector? y)
                         (= :boolean-and (first y)))
                    (apply conj x (rest y))
                    (and (vector? x)
                         (= :boolean-and (first x)))
                    (conj x y)
                    :else
                    [:boolean-and x y]))
   :boolean-or (fn [x y]
                 (cond
                   (and (vector? x)
                        (= :boolean-or (first x))
                        (vector? y)
                        (= :boolean-or (first y)))
                   (apply conj x (rest y))
                   (and (vector? x)
                        (= :boolean-or (first x)))
                   (conj x y)
                   :else
                   [:boolean-or x y]))})

(defn- symbol-suffix [x]
  (symbol (s/replace x #"^.+\." "")))

(defn- symbol-prefix [x]
  (symbol (s/replace x #"\..+$" "")))

(defn- symbol-with-prefix? [x]
  (boolean (re-find #"\." (str x))))

(defn- symbol-suffix-and-prefix->kw [x]
  (if (symbol-with-prefix? x)
    (keyword (name (symbol-prefix x)) (name (symbol-suffix x)))
    (keyword (name (symbol-suffix x)))))

(defn- normalize-where [where]
  (cond
    (set? where)
    where
    (= :and (first where))
    (set (rest where))
    (nil? where)
    #{}
    :else
    #{where}))

(def normalize-transform
  (merge
   {:name-intro (fn [x & [y]]
                  [x y])
    :table-spec (fn [x & [y]]
                  [x y])
    :select-item (fn [x & [y]]
                   [x (or y (if (symbol? x)
                              (symbol-suffix x)
                              (gensym "column_")))])
    :sort-spec (fn [x & [dir]]
                 [x (or dir :asc)])
    :set-function-spec (fn
                         ([type x]
                          [type :all x])
                         ([type quantifier x]
                          [type quantifier x]))
    :or (fn [& args]
          (let [common (apply set/intersection (map normalize-where args))]
            (if (empty? common)
              `[:or ~@args]
              `[:and
                ~@common
                [:or ~@(for [arg args]
                         (vec (cons :and (set/difference (normalize-where arg) common))))]])))}
   (let [constants [:count :sum :avg :min :max :star :all :distinct :asc :desc :year :month :day :hour :minute]]
     (zipmap constants (map constantly constants)))))

(def ^:private simplify-transform
  (->> (for [[x y] {:join-exp :join
                    :like-exp :like
                    :null-exp :null
                    :overlaps-exp :overlaps
                    :concatenation-exp :concatenation
                    :position-exp :position
                    :length-exp :length
                    :substring-exp :substring
                    :fold-exp :fold
                    :trim-exp :trim
                    :in-exp :in
                    :case-exp :case
                    :exists-exp :exists
                    :extract-exp :extract
                    :match-exp :match
                    :unique-exp :unique
                    :all-exp :all
                    :any-exp :any
                    :numeric-multiply :*
                    :numeric-divide :/
                    :numeric-plus :+
                    :numeric-minus :-
                    :numeric-modulo :%
                    :boolean-and :and
                    :boolean-or :or
                    :boolean-not :not
                    :comp-eq :=
                    :comp-ne :<>
                    :comp-lt :<
                    :comp-le :<=
                    :comp-gt :>
                    :comp-ge :>=}]
         [x (fn [& args]
              (vec (cons y args)))])
       (into {})))

(defn- query->map [[_ & args]]
  (let [select (zipmap (map first args)
                       (map (comp vec rest) args))]
    (reduce
     (fn [acc k]
       (cond-> acc
         (contains? acc k)
         (update k first)))
     select [:where :having :offset :limit])))

(defn- map->query [m]
  (vec (cons :select-exp
             (mapv vec
                   (for [[k v] m]
                     (if (contains? #{:where :having :offset :limit} k)
                       [k v]
                       (vec (cons k v))))))))

(defn parse-and-transform [sql]
  (reduce
   (fn [acc transform-map]
     (insta/transform transform-map acc))
   (parse-sql sql)
   [literal-transform
    constant-folding-transform
    nary-transform
    simplify-transform
    normalize-transform]))

;; SQL:2011 official grammar:

;; https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html

;; See also Date, SQL and Relational Theory, p. 455-458, A Simplified
;; BNF Grammar

;; High level SQL grammar, from
;; https://calcite.apache.org/docs/reference.html

;; SQLite grammar:
;; https://github.com/bkiers/sqlite-parser/blob/master/src/main/antlr4/nl/bigo/sqliteparser/SQLite.g4
;; https://www.sqlite.org/lang_select.html

;; SQL BNF from the spec:
;; https://ronsavage.github.io/SQL/

;; https://github.com/epfldata/dblab/blob/develop/components/src/main/scala/ch/epfl/data/dblab/frontend/parser/SQLAST.scala
;; https://github.com/epfldata/dblab/blob/develop/components/src/main/scala/ch/epfl/data/dblab/frontend/parser/SQLParser.scala

;; RelaX - relational algebra calculator
;; https://dbis-uibk.github.io/relax/

;; https://cs.ulb.ac.be/public/_media/teaching/infoh417/sql2alg_eng.pdf
;; https://www.cs.purdue.edu/homes/rompf/papers/rompf-icfp15.pdf

;; https://github.com/epfldata/dblab/blob/develop/components/src/main/scala/ch/epfl/data/dblab/queryengine/push/Operators.scala
;; https://github.com/epfldata/dblab/blob/develop/components/src/main/scala/ch/epfl/data/dblab/queryengine/volcano/Operators.scala
