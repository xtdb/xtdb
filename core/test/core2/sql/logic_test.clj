(ns core2.sql.logic-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.zip :as zip]
            [core2.api :as c2]
            [core2.rewrite :as rew]
            [core2.snapshot :as snap]
            [core2.sql :as sql]
            [core2.operator :as op]
            [core2.test-util :as tu]
            [instaparse.core :as insta])
  (:import java.nio.charset.StandardCharsets
           java.security.MessageDigest
           java.util.UUID))

(t/use-fixtures :each tu/with-node)

(defn- md5 ^String [^String s]
  (->> (.getBytes s StandardCharsets/UTF_8)
       (.digest (MessageDigest/getInstance "MD5"))
       (BigInteger. 1)
       (format "%032x")))

(defn- parse-create-table [^String x]
  (when-let [[_ table-name columns] (re-find #"(?is)^\s*CREATE\s+TABLE\s+(\w+)\s*\((.+)\)\s*$" x)]
    {:type :create-table
     :table-name table-name
     :columns (vec (for [column (str/split columns #",")]
                     (->> (str/split column #"\s+")
                          (remove str/blank?)
                          (first))))}))

(defn- parse-create-view [^String x]
  (when-let [[_ view-name query] (re-find #"(?is)^\s*CREATE\s+VIEW\s+(\w+)\s+AS\s+(.+)\s*$" x)]
    {:type :create-view
     :view-name view-name
     :as query}))

(defn- skip-statement? [^String x]
  (boolean (re-find #"(?is)^\s*CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*\((.+)\)\s*$" x)))

(defmulti parse-record (fn [[x & xs]]
                         (keyword (first (str/split x #"\s+")))))

(defmethod parse-record :statement [[x & xs]]
  (let [[_ mode] (str/split x #"\s+")
        statement (str/join "\n" xs)
        mode (keyword mode)]
    (assert (contains? #{:ok :error} mode))
    {:type :statement
     :mode mode
     :statement statement}))

(defmethod parse-record :query [[x & xs]]
  (let [[_ type-string sort-mode label] (str/split x #"\s+")
        [query _ result] (partition-by #{"----"} xs)
        query (str/join "\n" query)
        sort-mode (keyword (or sort-mode :nosort))
        record {:type :query
                :query query
                :type-string type-string
                :sort-mode sort-mode
                :label label}]
    (assert (contains? #{:nosort :rowsort :valuesort} sort-mode))
    (assert (re-find #"^[TIR]+$" type-string))
    (if-let [[_ values hash] (and (= 1 (count result))
                                  (re-find #"^(\d+) values hashing to (\p{XDigit}{32})$" (first result)))]
      (assoc record :result-set-size (Long/parseLong values) :result-set-md5sum hash)
      (assoc record :result-set-size (count result) :result-set (vec result)))))

(defmethod parse-record :skipif [[x & xs]]
  (let [[_ database-name] (str/split x #"\s+")]
    (assoc (parse-record xs) :skipif database-name)))

(defmethod parse-record :onlyif [[x & xs]]
  (let [[_ database-name] (str/split x #"\s+")]
    (assoc (parse-record xs) :onlyif database-name)))

(defmethod parse-record :halt [xs]
  (assert (= 1 (count xs)))
  {:type :halt})

(defmethod parse-record :hash-threshold [[x :as xs]]
  (assert (= 1 (count xs)))
  (let [[_ max-result-set-size] (str/split x #"\s+")]
    {:type :hash-threshold
     :max-result-set-size (Long/parseLong max-result-set-size)}))

(defn parse-script [script]
  (->> (str/split-lines script)
       (map #(str/replace % #"\s*#.+$" ""))
       (partition-by str/blank?)
       (remove #(every? str/blank? %))
       (mapv parse-record)))

(defmulti execute-statement (fn [ctx [type]]
                              type))

;; TODO: this is a temporary hack.
(defn- normalize-literal [x]
  (if (vector? x)
    (case (first x)
      :boolean_literal (= "TRUE" (second x))
      ;; TODO: is parsing NULL as an identifier correct?
      :regular_identifier (if (= "NULL" (str/upper-case (second x)))
                            nil
                            x)
      :character_string_literal (let [s (second x)]
                                  (subs s 1 (dec (count s))))
      :exact_numeric_literal (let [s (str/join "." (remove keyword? (flatten x)))]
                               (if (= 1 (count (rest x)))
                                 (Long/parseLong s)
                                 (Double/parseDouble s)))
      ;; TODO: should this parse as signed_numeric_literal?
      :factor (if (and (= 2 (count (rest x)))
                       (= [:sign [:minus_sign "-"]] (second x)))
                (- (first (filter number? (flatten x))))
                x)
      x)
    x))

(defn- insert->doc [{:keys [tables] :as ctx} insert-statement]
  (let [[_ _ _ insertion-target insert-columns-and-source] insert-statement
        table (first (filter string? (flatten insertion-target)))
        from-subquery (second insert-columns-and-source)
        columns (if (= 1 (count (rest from-subquery)))
                  (get tables table)
                  (let [insert-column-list (second from-subquery)]
                    (->> (flatten insert-column-list)
                         (filter string?))))
        query-expression (last from-subquery)
        [_ values] (->> query-expression
                        (w/postwalk normalize-literal)
                        (flatten)
                        (filter (some-fn number? string? boolean? nil?))
                        (split-with #{"VALUES"}))]
    (merge {:_table table} (zipmap (map keyword columns) values))))

(defmethod execute-statement :insert_statement [{:keys [node tables] :as ctx} insert-statement]
  (let [doc (insert->doc ctx insert-statement)]
    (-> (c2/submit-tx node [[:put (merge {:_id (UUID/randomUUID)} doc)]])
        (tu/then-await-tx node))
    ctx))

(defmulti execute-record (fn [ctx {:keys [type] :as record}]
                           type))

(defmethod execute-record :create-table [ctx {:keys [table-name columns]}]
  (assert (nil? (get-in ctx [:tables table-name])))
  (assoc-in ctx [:tables table-name] columns))

(defmethod execute-record :create-view [ctx {:keys [view-name as]}]
  (assert (nil? (get-in ctx [:views view-name])))
  (assoc-in ctx [:views view-name] as))

(defmethod execute-record :halt [ctx _]
  (reduced ctx))

(defmethod execute-record :hash-threshold [ctx {:keys [max-result-set-size]}]
  (assoc ctx :max-result-set-size max-result-set-size))

(defmethod execute-record :statement [ctx {:keys [mode statement]}]
  (if (skip-statement? statement)
    ctx
    (let [tree (sql/parse statement :direct_sql_data_statement)]
      (if (insta/failure? tree)
        (if-let [record (or (parse-create-table statement)
                            (parse-create-view statement))]
          (case mode
            :ok (execute-record ctx record)
            :error (t/is (thrown? Exception (execute-record ctx record))))
          (throw (IllegalArgumentException. (prn-str (insta/get-failure tree)))))
        (let [direct-sql-data-statement-tree (second tree)]
          (case mode
            :ok (execute-statement ctx direct-sql-data-statement-tree)
            :error (t/is (thrown? Exception (execute-statement ctx direct-sql-data-statement-tree)))))))))

(defn- format-result-str [sort-mode projection result]
  (let [result-rows (for [vs (map #(map % projection) result)]
                      (for [v vs]
                        (cond
                          (nil? v) "NULL"
                          (= "" v) "(empty)"
                          (float? v) (format "%.3f" v)
                          :else (str v))))]
    (->> (case sort-mode
           :rowsort (flatten (sort-by (partial str/join " ") result-rows))
           :valuesort (sort (flatten result-rows))
           :nosort (flatten result-rows))
         (str/join "\n"))))

(defn- validate-type-string [type-string projection result]
  (doseq [row result
          [value type] (map vector (map row projection) type-string)
          :let [java-class (case (str type)
                             "I" Long
                             "R" Double
                             "T" String)]]
    (t/is (or (nil? value) (cast java-class value)))))

(def ^:private normalize-query-rules
  {:select_sublist
   (rew/->scoped {:derived_column (rew/->after
                                   (fn [loc _]
                                     (if (rew/single-child? loc)
                                       (let [select-sublist-loc (zip/up loc)
                                             column (str "col" (count (zip/lefts select-sublist-loc)))]
                                         (zip/append-child
                                          loc
                                          [:as_clause
                                           "AS"
                                           [:column_name
                                            [:identifier
                                             [:actual_identifier [:regular_identifier column]]]]]))
                                       loc)))})

   :identifier_chain
   (rew/->after (fn [loc {:keys [tables] :as old-ctx}]
                  (if (rew/single-child? loc)
                    ;; TODO: does not take renamed tables into account.
                    (let [column (first (rew/text-nodes loc))
                          table (first (for [[table columns] tables
                                             :when (contains? (set columns) column)]
                                         table))]
                      (zip/replace loc [:identifier_chain
                                        [:identifier
                                         [:actual_identifier
                                          [:regular_identifier table]]]
                                        [:identifier
                                         [:actual_identifier
                                          [:regular_identifier column]]]]))
                    loc)))

   :sort_specification
   (rew/->scoped {:unsigned_value_specification
                  (rew/->after
                   (fn [loc _]
                     (let [ordinal (first (rew/text-nodes loc))
                           column (str "col" ordinal)]
                       (zip/replace loc [:column_reference
                                         [:basic_identifier_chain
                                          [:identifier_chain
                                           [:identifier
                                            [:actual_identifier
                                             [:regular_identifier column]]]]]]))))})})

(defn normalize-query-tree [tables tree]
  (rew/rewrite-tree tree {:tables tables :rules normalize-query-rules}))

;; TODO: parse query and qualify known table columns if
;; needed. Generate logical plan and format and hash result according
;; to sort mode. Projection will usually be positional.
(defmethod execute-record :query [{:keys [node tables] :as ctx}
                                  {:keys [query type-string sort-mode label
                                          result-set-size result-set result-set-md5sum]}]
  (let [tree (sql/parse query :query_expression)
        snapshot-factory (tu/component node ::snap/snapshot-factory)
        db (snap/snapshot snapshot-factory)]
    (when (insta/failure? tree)
      (throw (IllegalArgumentException. (prn-str (insta/get-failure tree)))))
    (let [tree (normalize-query-tree tables tree)
          result (op/query-ra '[:scan [_id]] db)
          projection [:_id]
          result-str (format-result-str sort-mode projection result)]
      #_(validate-type-string type-string projection result)
      (when-let [row (first result)]
        (t/is (count type-string) (count row)))
      (t/is (= result-set-size (count result)))
      #_(when result-set
          (t/is (= (str/join "\n" result-set) result-str)))
      (when result-set-md5sum
        (t/is (= result-set-md5sum (md5 result-str)))))
    ctx))

(defn- skip-record? [{:keys [skipif onlyif]
                      :or {onlyif "xtdb"}}]
 (or (= "xtdb" skipif)
     (not= "xtdb" onlyif)) )

(defn- execute-records [node records]
  (->> (remove skip-record? records)
       (reduce execute-record {:node node :tables {}})))

(t/deftest test-sql-logic-test-parser
  (t/is (= 6 (count (parse-script
                     "# full line comment

statement ok
CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)

statement ok
INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104) # end of line comment

halt

hash-threshold 32

query I nosort
SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 ORDER BY 1
----
30 values hashing to 3c13dee48d9356ae19af2515e05e6b54

skipif postgresql
query III rowsort label-xyzzy
SELECT a x, b y, c z FROM t1
")))))

(t/deftest test-sql-logic-test-execution
  (let [records (parse-script
                 "
statement ok
CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)

statement ok
INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)

query I nosort
SELECT t1.a FROM t1
----
104
")]

    (t/is (= 3 (count records)))
    (t/is (= {"t1" ["a" "b" "c" "d" "e"]} (:tables (execute-records tu/*node* records))))))

(t/deftest test-parse-create-table
  (t/is (= {:type :create-table
            :table-name "t1"
            :columns ["a" "b"]}
           (parse-create-table "CREATE TABLE t1(a INTEGER, b INTEGER)")))

  (t/is (= {:type :create-table
            :table-name "t2"
            :columns ["a" "b" "c"]}

           (parse-create-table "
CREATE TABLE t2(
  a INTEGER,
  b VARCHAR(30),
  c INTEGER
)"))))

(t/deftest test-parse-create-view
  (t/is (= {:type :create-view
            :view-name "view_1"
            :as "SELECT pk, col0 FROM tab0 WHERE col0 = 49"}
           (parse-create-view "CREATE VIEW view_1 AS SELECT pk, col0 FROM tab0 WHERE col0 = 49"))))

(t/deftest test-skip-statement?
  (t/is (false? (skip-statement? "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)")))
  (t/is (true? (skip-statement? "CREATE INDEX t1i0 ON t1(a1,b1,c1,d1,e1,x1)")))
  (t/is (true? (skip-statement? "
CREATE UNIQUE INDEX t1i0 ON t1(
  a1,
  b1
)"))))

(t/deftest test-normalize-query
  (let [tables {"t1" ["a" "b" "c" "d" "e"]}
        ctx {:tables tables}
        query "SELECT a+b*2+c*3+d*4+e*5, (a+b+c+d+e)/5 FROM t1 ORDER BY 1,2"
        expected "SELECT t1.a+t1.b*2+t1.c*3+t1.d*4+t1.e*5 AS col1, (t1.a+t1.b+t1.c+t1.d+t1.e)/5 AS col2 FROM t1 ORDER BY col1,col2"]
    (t/is (= (sql/parse expected :query_expression)
             (normalize-query-tree tables (sql/parse query :query_expression))))))

(t/deftest test-insert->doc
  (let [ctx {:tables {"t1" ["a" "b" "c" "d" "e"]}}]
    (t/is (= {:e 103 :c 102 :b 100 :d 101 :a 104 :_table "t1"}
             (insert->doc ctx (sql/parse "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)" :insert_statement))))
    (t/is (= {:a nil :b -102 :c true :d "101" :e 104.5 :_table "t1"}
             (insert->doc ctx (sql/parse "INSERT INTO t1 VALUES(NULL,-102,TRUE,'101',104.5)" :insert_statement))))))

(t/deftest test-annotate-query-scopes
  (let [tree (sql/parse "WITH foo AS (SELECT 1 FROM bar)
SELECT t1.d-t1.e
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND t1.a>t1.b
 ORDER BY 1" :query_expression)
        scopes (->> (sql/annotate-tree tree)
                    (tree-seq vector? seq)
                    (keep (comp :core2.sql/scope meta)))]
    (t/is (= [{:tables #{{:table-or-query-name "t1"}}
               :columns #{["t1" "e"] ["t1" "a"] ["t1" "b"] ["t1" "d"]}
               :with #{"foo"}
               :correlated-columns #{}}
              {:tables #{{:table-or-query-name "bar"}},
               :columns #{},
               :with #{},
               :correlated-columns #{}}
              {:tables #{{:table-or-query-name "t1" :correlation-name "x"}}
               :columns #{["x" "b"] ["t1" "b"]}
               :with #{}
               :correlated-columns #{["t1" "b"]}}]
             scopes))))

(comment
  (dotimes [n 5]
    (time
     (let [f (format "core2/sql/logic_test/select%d.test" (inc n))
           records (parse-script (slurp (io/resource f)))]
       (println f (count records))
       (doseq [{:keys [type statement query] :as record} records
               :let [input (case type
                             :statement statement
                             :query query
                             nil)]
               :when input
               :let [tree (sql/parse-sql2011 input :start :direct_sql_data_statement)]]
         (if-let [failure (insta/get-failure tree)]
           (println (or (parse-create-table input) failure))
           (print ".")))
       (println)))))
