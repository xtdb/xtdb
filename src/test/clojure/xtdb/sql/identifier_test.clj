(ns xtdb.sql.identifier-test
  (:require [clojure.set :as set]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu])
  (:import (xtdb.antlr SqlLexer)))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

;; the words PostgreSQL 16 accepts in every identifier position and XTDB refused in all of them
(def ^:private semi-reserved
  '[at between by current date day first hour last minute month next of period
    position quarter rank row second text time timestamp uuid week year])

(defn- refused-by [f]
  (->> semi-reserved
       (remove (fn [word]
                 (= [{(keyword (str word)) 1}]
                    (try (xt/q tu/*node* (f word))
                         (catch Exception _ ::threw)))))
       vec))

(t/deftest semi-reserved-keywords-are-usable-as-an-as-alias-6014
  (t/is (= [] (refused-by #(str "SELECT 1 AS " %)))))

(t/deftest semi-reserved-keywords-are-usable-as-a-derived-table-column-alias-6014
  (t/is (= [] (refused-by #(str "SELECT t." % " FROM (VALUES (1)) t(" % ")")))))

(t/deftest identifier-tier-keywords-still-open-their-own-constructs
  (doseq [[sql expected]
          [["SELECT EXTRACT(YEAR FROM DATE '2020-03-04') AS r" 2020]
           ["SELECT EXTRACT(MONTH FROM DATE '2020-03-04') AS r" 3]
           ["SELECT EXTRACT(DAY FROM DATE '2020-03-04') AS r" 4]
           ["SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-03-04 05:06:07') AS r" 5]
           ["SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-03-04 05:06:07') AS r" 6]
           ["SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-03-04 05:06:07') AS r" 7]
           ["SELECT EXTRACT(WEEK FROM DATE '2020-03-04') AS r" 10]
           ["SELECT EXTRACT(QUARTER FROM DATE '2020-03-04') AS r" 1]
           ["SELECT DATE_TRUNC('WEEK', TIMESTAMP '2020-03-04 05:06:07') AS r" #xt/ldt "2020-03-02T00:00"]
           ["SELECT POSITION('b' IN 'abc') AS r" 2]
           ["SELECT 2 BETWEEN 1 AND 3 AS r" true]
           ["SELECT 2 NOT BETWEEN SYMMETRIC 3 AND 1 AS r" false]
           ["SELECT 1 AS r FETCH FIRST 1 ROWS ONLY" 1]
           ["SELECT 1 AS r FETCH NEXT 1 ROWS ONLY" 1]
           ["SELECT 1 AS r ORDER BY r NULLS FIRST" 1]
           ["SELECT 1 AS r ORDER BY r NULLS LAST" 1]
           ["SELECT 1 AS r FROM (VALUES (1)) t(x) GROUP BY x" 1]
           ["SELECT COUNT(*) AS r FROM (VALUES (1)) t(x)" 1]
           ["SELECT MAX(x) AS r FROM (VALUES (1)) t(x)" 1]]]
    (t/is (= [{:r expected}] (xt/q tu/*node* sql)) sql)))

(t/deftest object-name-tier-keywords-still-open-their-own-constructs
  (doseq [[sql expected]
          [["SELECT DATE '2020-01-01' AS r" #xt/date "2020-01-01"]
           ["SELECT TIMESTAMP '2020-01-01 12:00:00' AS r" #xt/date-time "2020-01-01T12:00:00"]
           ["SELECT TIME '12:00:00' AS r" #xt/time "12:00"]
           ["SELECT UUID '97a392d5-5e3f-406f-9651-a828ee79b156' AS r"
            #uuid "97a392d5-5e3f-406f-9651-a828ee79b156"]
           ["SELECT INTERVAL '3' DAY AS r" #xt/interval "P3D"]
           ["SELECT INTERVAL '1-2' YEAR TO MONTH AS r" #xt/interval "P14M"]
           ["SELECT INTERVAL '3.5' SECOND(3) AS r" #xt/interval "PT3.5S"]
           ["SELECT CAST('2020-01-01' AS DATE) AS r" #xt/date "2020-01-01"]
           ["SELECT CAST(1 AS TEXT) AS r" "1"]
           ["SELECT CAST('12:00:00' AS TIME(3)) AS r" #xt/time "12:00"]
           ["SELECT 1 AS r WHERE DATE '2020-01-01' < TIMESTAMP '2020-06-01 00:00:00'" 1]
           ["SELECT PERIOD(TIMESTAMP '2020-01-01Z', TIMESTAMP '2021-01-01Z') AS r"
            #xt/tstz-range [#xt/zdt "2020-01-01T00:00Z[UTC]" #xt/zdt "2021-01-01T00:00Z[UTC]"]]
           ["SELECT PERIOD(TIMESTAMP '2020-01-01Z', TIMESTAMP '2021-01-01Z') CONTAINS TIMESTAMP '2020-06-01Z' AS r"
            true]]]
    (t/is (= [{:r expected}] (xt/q tu/*node* sql)) sql))

  (t/is (= [{:a 1, :b 2}] (xt/q tu/*node* "SELECT * FROM (VALUES ROW(1, 2)) AS t(a, b)"))
        "ROW's row constructor has a function call's exact shape, so ANTLR can't discriminate it"))

(t/deftest keyword-column-names-round-trip-through-dml
  (xt/execute-tx tu/*node* ["INSERT INTO docs (_id, year, date, text, period, row, interval) VALUES (1, 2020, DATE '2020-01-01', 'a', 2, 3, 4)"])

  (t/is (= [{:xt/id 1, :year 2020, :date #xt/date "2020-01-01", :text "a"
             :period 2, :row 3, :interval 4}]
           (xt/q tu/*node* "SELECT _id, year, date, text, period, row, interval FROM docs")))

  (t/is (= [{:period 2}] (xt/q tu/*node* "SELECT d.period FROM docs d WHERE d.row = 3")))

  (xt/execute-tx tu/*node* ["UPDATE docs SET year = 2021, period = 5 WHERE _id = 1"])

  (t/is (= [{:year 2021, :period 5}] (xt/q tu/*node* "SELECT year, period FROM docs"))))

(defn- keyword-tokens []
  (let [vocab (SqlLexer/VOCABULARY)]
    (->> (range 1 (inc (.getMaxTokenType vocab)))
         (keep (fn [tt]
                 (when-let [lit (.getLiteralName vocab tt)]
                   (let [word (subs lit 1 (dec (count lit)))]
                     (when (re-matches #"[A-Za-z_]+" word)
                       word)))))
         (into (sorted-set)))))

(defn- accepted-by
  "asks the grammar, not the key-fn — an underscored word comes back kebab-cased, so compare values"
  [f]
  (into (sorted-set)
        (filter (fn [word]
                  (= [[1]] (try (mapv (comp vec vals) (xt/q tu/*node* (f word)))
                                (catch Exception _ ::threw)))))
        (keyword-tokens)))

(def ^:private identifier-keywords
  "Sql.g4's `unreservedKeyword` production, with its four sub-productions expanded."
  (into (sorted-set)
        (concat ["START" "END" "COMMITTED" "UNCOMMITTED" "TIMEZONE" "VERSION" "SYSTEM_TIME"
                 "VALID_TIME" "SELECT" "INSERT" "UPDATE" "DELETE" "ERASE" "SETTING" "ROLE"
                 "USER" "PASSWORD" "VARBINARY" "BYTEA" "URI" "OID" "COPY" "FORMAT" "ATTACH"
                 "DETACH" "DATABASE" "LEVEL" "FILTER" "TABLE" "METADATA" "SYNC"
                 "AT" "BETWEEN" "BY" "CURRENT" "FIRST" "LAST" "NEXT" "OF" "POSITION"]
                ;; setFunctionType
                ["AVG" "MAX" "MIN" "SUM" "COUNT" "EVERY" "BOOL_AND" "BOOL_OR"
                 "STDDEV_POP" "STDDEV_SAMP" "VAR_SAMP" "VAR_POP"]
                ;; primaryDatetimeField
                ["YEAR" "MONTH" "DAY" "HOUR" "MINUTE" "SECOND"]
                ;; pgExtractField
                ["DOW" "ISODOW" "DOY" "WEEK" "QUARTER" "EPOCH"]
                ;; rankFunctionType
                ["RANK" "DENSE_RANK" "PERCENT_RANK" "CUME_DIST"])))

(def ^:private object-name-keywords
  "`identifier`'s tier plus the type-name-shaped words `objectName` adds on top of it."
  (into identifier-keywords
        ["BIGINT" "BOOLEAN" "CHAR" "DATE" "DEC" "DECIMAL" "DOUBLE" "DURATION" "FLOAT" "INT"
         "INTEGER" "INTERVAL" "KEYWORD" "NUMERIC" "OBJECT" "PERIOD" "PRECISION" "REAL" "RECORD"
         "REGCLASS" "REGPROC" "ROW" "SMALLINT" "TEXT" "TIME" "TIMESTAMP" "TIMESTAMPTZ"
         "TSTZRANGE" "UUID" "VARCHAR"]))

(t/deftest the-identifier-tier-is-exactly-the-keywords-a-correlation-name-accepts
  (let [accepted (accepted-by #(str "SELECT 1 AS r FROM (VALUES (1)) AS " %))]
    (t/is (= [] (vec (set/difference identifier-keywords accepted)))
          "the tier names a word the grammar won't accept as an identifier")

    (t/is (= [] (vec (set/difference accepted identifier-keywords)))
          "a keyword is usable as an identifier without the tier naming it")))

(t/deftest the-object-name-tier-is-exactly-the-keywords-a-column-alias-accepts
  (let [accepted (accepted-by #(str "SELECT 1 AS " %))]
    (t/is (= [] (vec (set/difference object-name-keywords accepted)))
          "the tier names a word the grammar won't accept as a name")

    (t/is (= ["CURRENT_USER"] (vec (set/difference accepted object-name-keywords)))
          "a keyword is usable as a name without the tier naming it - CURRENT_USER is columnLabel's own alternative, not objectName's")))
