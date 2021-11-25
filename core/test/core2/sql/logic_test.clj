(ns core2.sql.logic-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [core2.api :as c2]
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

;; TODO: parse insert and create proper document, potentially with
;; help of the known table columns.
(defmethod execute-statement :insert_statement [{:keys [node tables] :as ctx} insert-statement]
  (-> (c2/submit-tx node [[:put {:_id (UUID/randomUUID)}]])
      (tu/then-await-tx node))
  ctx)

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
    (let [tree (sql/parse-sql2011 statement :start :direct_sql_data_statement)]
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

;; TODO: parse query and qualify known table columns if
;; needed. Generate logical plan and format and hash result according
;; to sort mode. Projection will usually be positional.
(defmethod execute-record :query [{:keys [node tables] :as ctx}
                                  {:keys [query type-string sort-mode label
                                          result-set-size result-set result-set-md5sum]}]
  (let [tree (sql/parse-sql2011 query :start :query_expression)
        snapshot-factory (tu/component node ::snap/snapshot-factory)
        db (snap/snapshot snapshot-factory)]
    (when (insta/failure? tree)
      (throw (IllegalArgumentException. (prn-str (insta/get-failure tree)))))
    (let [result (op/query-ra '[:scan [_id]] db)
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
