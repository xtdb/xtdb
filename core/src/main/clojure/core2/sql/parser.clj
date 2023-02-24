(ns core2.sql.parser
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [core2.error :as err]
            [core2.util :as util])
  (:import [core2.sql.parser Parser Parser$AParser Parser$AltParser Parser$CatParser Parser$EpsilonParser Parser$HideParser Parser$MemoTable Parser$MemoizeLeftRecParser Parser$MemoizeParser Parser$NegParser Parser$NonTerminalParser Parser$OptParser Parser$OrdParser Parser$ParseErrors Parser$RegexpParser Parser$RepeatParser Parser$RuleParser Parser$StringParser]
           [java.util ArrayDeque HashSet Map Set]
           java.util.function.Function
           [java.util.regex Matcher Pattern]))

;; https://arxiv.org/pdf/1509.02439v1.pdf
;; https://medium.com/@gvanrossum_83706/left-recursive-peg-grammars-65dab3c580e1
;; https://bford.info/pub/lang/thesis.pdf
;; https://arxiv.org/pdf/1806.11150.pdf

(set! *unchecked-math* :warn-on-boxed)

(defrecord ParseFailure [^String in ^Set errs ^int idx])

(defn- left-recursive? [grammar rule-name]
  (let [visited (HashSet.)
        stack (ArrayDeque.)]
    (.push stack (get grammar rule-name))
    (loop []
      (if-let [node (.poll stack)]
        (cond
          (= (:keyword node) rule-name)
          true

          (.contains visited node)
          (recur)

          :else
          (do (.add visited node)
              (let [node (if (= :nt (:tag node))
                           (get grammar (:keyword node))
                           node)]
                (case (:tag node)
                  :cat (let [[opts mandatory] (split-with (comp #{:opt :star} :tag) (:parsers node))]
                         (doseq [opt opts]
                           (.push stack opt))
                         (when-not (empty? mandatory)
                           (.push stack (first mandatory))))
                  :ord (do (.push stack (:parser1 node))
                           (.push stack (:parser2 node)))
                  :alt (doseq [alt (:parsers node)]
                         (.push stack alt))
                  (:star :plus :opt) (.push stack (:parser node))
                  :nt (.push stack node)
                  nil)
                (recur))))
        false))))

(defn- rule-kw->name [kw]
  (str "<" (str/replace (name kw) "_" " ") ">"))

(defn index->line-column [^String in ^long idx]
  (loop [line 1
         col 1
         n 0]
    (cond
      (= idx n) {:line line :column col}
      (= \newline (.charAt in n)) (recur (inc line) 1 (inc n))
      :else (recur line (inc col) (inc n)))))

(defn failure? [x]
  (instance? ParseFailure x))

(defn failure->str ^String [^ParseFailure failure]
  (let [{:keys [^long line ^long column]} (index->line-column (.in failure) (.idx failure))]
    (with-out-str
      (println (str "Parse error at line " line ", column " column ":"))
      (println (get (str/split-lines (.in failure)) (dec line)))
      (dotimes [_ (dec column)]
        (print " "))
      (println "^")
      (doseq [[category errs] (group-by first (.errs failure))]
        (println (str (str/capitalize (name category))
                      (if (= 1 (count errs))
                        ":"
                        " one of:")))
        (doseq [[_ msg] errs]
          (println msg))))))

(defn- surround-parser-with-ws ^core2.sql.parser.Parser$AParser [parser ^Pattern ws-pattern]
  (Parser$CatParser. [(Parser$HideParser. (Parser$StringParser. "" ws-pattern))
                      parser
                      (Parser$EpsilonParser.)]))

(defn- lookahead [grammar seen {:keys [tag] :as parser}]
  (when-not (contains? seen parser)
    (let [seen (conj seen parser)]
      (case tag
        :nt (recur grammar seen (get grammar (:keyword parser)))
        (:star :opt :neg :epsilon) #{::undecided-lookahead}
        :plus (recur grammar seen (:parser parser))
        :cat ((fn step [[parser & parsers]]
                (case (:tag parser)
                  (:star :opt) (set/union (lookahead grammar seen (:parser parser))
                                          (step parsers))
                  :neg (step parsers)
                  (when parser
                    (lookahead grammar seen parser))))
              (:parsers parser))
        :alt (reduce set/union (map (partial lookahead grammar seen) (:parsers parser)))
        :ord (let [parsers ((fn step [{:keys [parser1 parser2]}]
                              (cons parser1 (if (= :ord (:tag parser2))
                                              (step parser2)
                                              [parser2])))
                            parser)]
               (reduce set/union (map (partial lookahead grammar seen) parsers)))
        :regexp (if-let [lookahead-set (not-empty (set (for [n (range 128)
                                                             :let [m (re-matcher (:regexp parser) (str (char n)))]
                                                             :when (or (.lookingAt m)
                                                                       (.hitEnd m))]
                                                         (char n))))]
                  lookahead-set
                  #{::undecided-lookahead})
        :string (hash-set (Character/toLowerCase (char (first (:string parser))))
                          (Character/toUpperCase (char (first (:string parser)))))))))

(def ^:private undecided-lookahead-table (boolean-array 128 true))

(defn- lookahead-alt-table [grammar parsers]
  (vec (for [parser parsers
             :let [lookahead-set (lookahead grammar #{} parser)]]
         (if (contains? lookahead-set ::undecided-lookahead)
           undecided-lookahead-table
           (reduce
            (fn [^booleans acc ^long c]
              (if (< c 128)
                (doto acc
                  (aset c true))
                acc))
            (boolean-array 128)
            (map long lookahead-set))))))

(defn build-ebnf-parser
  ([grammar ws-pattern]
   (build-ebnf-parser grammar ws-pattern (fn [_rule-name]
                                           Parser/NEVER_RAW)))
  ([^Map grammar ws-pattern ->raw?]
   (let [^Map rule->id (zipmap (keys grammar) (range))
         ^"[Lcore2.sql.parser.Parser$AParser;" rules (make-array Parser$AParser (count rule->id))]
     (letfn [(build-parser [rule-name {:keys [tag hide] :as parser}]
               (let [parser (case tag
                              :nt (let [body (get grammar (:keyword parser))]
                                    (if (and (or hide (= {:reduction-type :raw} (:red body)))
                                             (contains? #{:string :regexp :nt} (:tag body)))
                                      (build-parser rule-name body)
                                      (Parser$NonTerminalParser. (get rule->id (:keyword parser)))))
                              :star (Parser$RepeatParser. (build-parser rule-name (:parser parser)) true)
                              :plus (Parser$RepeatParser. (build-parser rule-name (:parser parser)) false)
                              :opt (Parser$OptParser. (build-parser rule-name (:parser parser)))
                              :cat (Parser$CatParser. (mapv (partial build-parser rule-name) (:parsers parser)))
                              :alt (Parser$AltParser. (mapv (partial build-parser rule-name) (:parsers parser))
                                                      (lookahead-alt-table grammar (:parsers parser)))
                              :ord (let [parsers ((fn step [{:keys [parser1 parser2]}]
                                                    (cons parser1 (if (= :ord (:tag parser2))
                                                                    (step parser2)
                                                                    [parser2])))
                                                  parser)]
                                     (Parser$OrdParser. (mapv (partial build-parser rule-name) parsers)))
                              :neg (Parser$NegParser. (build-parser rule-name (:parser parser)))
                              :epsilon (Parser$EpsilonParser.)
                              :regexp (Parser$RegexpParser. (:regexp parser)
                                                            [:expected (rule-kw->name rule-name)]
                                                            (reify Function
                                                              (apply [_ m]
                                                                [(.group ^Matcher m)]))
                                                            ws-pattern)
                              :string (if (re-find #"^\w+$" (:string parser))
                                        (Parser$RegexpParser. (Pattern/compile (str (Pattern/quote (:string parser)) "\\b")
                                                                               Pattern/CASE_INSENSITIVE)
                                                              [:expected (:string parser)]
                                                              (let [ast [(:string parser)]]
                                                                (reify Function
                                                                  (apply [_ m]
                                                                    ast)))
                                                              ws-pattern)
                                        (Parser$StringParser. (:string parser) ws-pattern)))]
                 (if hide
                   (Parser$HideParser. parser)
                   parser)))]
       (doseq [[k v] grammar
               :let [rule-id (int (get rule->id k))
                     raw? (if (= {:reduction-type :raw} (:red v))
                            Parser/ALWAYS_RAW
                            (->raw? k))
                     parser (Parser$RuleParser. k raw? (build-parser k v))
                     parser (if (left-recursive? grammar k)
                              (Parser$MemoizeLeftRecParser. parser rule-id (count grammar))
                              (Parser$MemoizeParser. parser rule-id (count grammar)))]]
         (aset rules rule-id ^Parser$AParser parser))

       (doseq [^Parser$AParser rule-parser rules]
         (.init rule-parser rules))

       (fn [^String in start-rule]
         (let [memos (Parser$MemoTable. (.length in))
               parser (surround-parser-with-ws (aget rules (.get rule->id start-rule)) ws-pattern)]
           (if-let [state (.parse parser in 0 memos Parser/NULL_PARSE_ERRORS false)]
             (.nth (.ast state) 0 nil)
             (let [memos (Parser$MemoTable. (.length in))
                   errors (Parser$ParseErrors.)]
               (.parse parser in 0 memos errors true)
               (ParseFailure. in (.getErrors errors) (.getIndex errors))))))))))

(defn or-throw [{:keys [errs] :as res}]
  (if errs
    (let [failure-str (failure->str res)]
      (throw (err/illegal-arg :core2.sql/parse-error
                              {::err/message (str "Invalid SQL query: " failure-str)
                               :errs [failure-str]})))
    res))

(def sql-cfg
  (read-string (slurp (io/resource "core2/sql/parser/SQL2011.edn"))))

(def sql-regular-ws-pattern
  #"\s+")

(def sql-single-line-comment-pattern
  #"--.*")

(def sql-multi-line-comment-pattern
  #"(?s)/[*].*?(?:[*]/|$)(?-s)")

;; TODO: I think this should be plus and not star, as the idea is that
;; it should match mandatory whitespace, but detect boundaries and
;; punctuation as well. Most of that is really handled by the parsing
;; of the keywords themselves, enforcing them to be at word
;; boundaries. Identifiers may bleed over into keywords if there's no
;; boundary, like fooASbar is a single identifier.

;; NOTE, the following are currently not used.

(def sql-after-punctuation-pattern
  #"(?<=\p{Punct})")

(def sql-word-boundary-pattern
  #"\b")


(def sql-ws-pattern
  (Pattern/compile
   (str "(?:"
        (->> (for [^Pattern p [sql-regular-ws-pattern
                               sql-single-line-comment-pattern
                               sql-multi-line-comment-pattern]]
               (.pattern p))
             (str/join "|"))
        ")*")))

;; API

(def sql-parser
  (build-ebnf-parser sql-cfg
                     sql-ws-pattern
                     (fn [rule-name]
                       (if (and (not (contains? #{:table_primary :query_expression :table_expression} rule-name))
                                (re-find #"(^|_)(term|factor|primary|expression|query_expression_body|boolean_test)$" (name rule-name)))
                         Parser/SINGLE_CHILD
                         Parser/NEVER_RAW))))

(def parse
  (-> (fn self
        ([in] (self in :directly_executable_statement))
        ([in start-rule]
         (-> (sql-parser in start-rule)
             (vary-meta assoc :sql in))))
      (util/lru-memoize)))

(comment
  (sql-parser "SELECT foo.bar,
                        SUM(foo.bar) OVER (PARTITION BY foo.biff) AS baz
                        FROM foo" :directly_executable_statement)


  (sql-parser "SELECT * FROMfoo" :directly_executable_statement)

  (println (failure->str (sql-parser "* FROMfoo" :term)))

  (sql-parser "  (SELECT avg(c) FROM t1)  " :subquery)

  (sql-parser "a[0]" :value_expression_primary)

  (sql-parser "- - 2" :factor)

  (time (sql-parser "(((((((((1)))))))))" :value_expression_primary))

  (= (sql-parser "SELECT pk FROM tab0 WHERE ((col0 >= 6) OR (col1 = 2.11 AND col3 BETWEEN 3 AND 4) AND ((col1 > 9.58 AND col0 <= 4)) AND (col0 >= 6))" :directly_executable_statement)
     (sql-parser (core2.sql.logic-test.xtdb-engine/remove-unnecessary-parens "SELECT pk FROM tab0 WHERE ((col0 >= 6) OR (col1 = 2.11 AND col3 BETWEEN 3 AND 4) AND ((col1 > 9.58 AND col0 <= 4)) AND (col0 >= 6))") :directly_executable_statement))

  (sql-parser "IN ( col0 * col0 )" :in_predicate_part_2)

  (sql-parser "INTERVAL '3 4' DAY TO HOUR" :interval_literal)

  (doseq [sql ["SELECT u.a[0] AS first_el FROM uo"
               "SELECT u.b[u.a[0]] AS dyn_idx FROM u"
               "SELECT 1 YEAR + 3 MONTH + 4 DAY t FROM foo WHERE foo.a = 42"
               "SELECT 3 * 1 YEAR t FROM foo WHERE foo.a = 42"
               "SELECT foo.a YEAR + 1 MONTH + 2 DAY t FROM foo WHERE foo.a = 42"
               "SELECT foo.a YEAR + 1 MONTH - 2 DAY t FROM foo WHERE foo.a = 42"
               "SELECT foo.a || 'a' || 'b' t FROM foo WHERE foo.a = 42"
               "SELECT ALL 4 AS col1 FROM tab2 AS cor0 WHERE NULL IS NULL"
               "SELECT ALL 74 * - COALESCE ( + CASE - CASE WHEN NOT ( NOT - 79 >= NULL ) THEN 48 END WHEN + + COUNT ( * ) THEN 6 END, MIN ( ALL + - 30 ) * 45 * 77 ) * - 14 FROM (VALUES 0) AS no_from"
               "SELECT cor0.col2 AS col2 FROM tab2 AS cor0 GROUP BY col2 HAVING NOT NULL < NULL"
               "SELECT DISTINCT col2 FROM tab1 WHERE NULL BETWEEN NULL AND - col2"
               ;; spec doesn't seem to allow expressions inside IN.
               "SELECT * FROM tab1 WHERE NULL NOT IN ( col0 * col0 )"
               "SELECT INTERVAL '3 4' DAY TO HOUR t FROM foo WHERE foo.a = 42"
               "SELECT * FROMfoo"
               "SELECT + NULLIF ( + 67, + + NULLIF ( + + 46, 66 - CASE WHEN 51 IN ( + 91 ) THEN + SUM ( CAST ( NULL AS INTEGER ) ) + - 92 ELSE - ( - 47 ) END ) ) FROM (VALUES 0) AS no_from
"]
          :let [tree (sql-parser sql :directly_executable_statement)]
          :when (failure? tree)]
    (println (failure->str tree)))

  (let [in "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4,
       abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d,
       a+b*2,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1 WHERE e+d BETWEEN a+b-10 AND c+130
    OR coalesce(a,b,c,d,e)<>0"]

    (time
     (dotimes [_ 1000]
       (sql-parser in :directly_executable_statement)))))
