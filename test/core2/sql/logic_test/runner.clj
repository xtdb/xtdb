(ns core2.sql.logic-test.runner
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [clojure.string :as str]
            [core2.test-util :as tu])
  (:import java.nio.charset.StandardCharsets
           java.io.File
           java.security.MessageDigest
           [java.sql Connection DriverManager]))

(defprotocol DbEngine
  (get-engine-name [_])
  (execute-statement [_ statement])
  (execute-query [_ query]))

(extend-protocol DbEngine
  Connection
  (get-engine-name [this]
    (str/lower-case (.getDatabaseProductName (.getMetaData this))))

  (execute-statement [this statement]
    (with-open [stmt (.createStatement this)]
      (.execute stmt statement))
    this)

  (execute-query [this query]
    (with-open [stmt (.createStatement this)
                rs (.executeQuery stmt query)]
      (let [column-count (.getColumnCount (.getMetaData rs))]
        (loop [acc []]
          (if (.next rs)
            (recur (loop [n 0
                          row []]
                     (if (= n column-count)
                       (conj acc row)
                       (recur (inc n)
                              (conj row (.getObject rs (inc n)))))))
            acc))))))

;; Parser

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

(defn parse-script
  ([script] (parse-script "" script))
  ([file-name script]
   (vec (for [idx+lines (->> (for [[idx line] (map-indexed vector (str/split-lines script))]
                               [idx (str/replace line #"\s*#.+$" "")])
                             (partition-by (comp str/blank? second))
                             (remove #(every? (comp str/blank? second) %)))]
          (assoc (parse-record (map second idx+lines))
                 :line (inc (ffirst idx+lines)) :file file-name)))))

;; Runner

(defmulti execute-record (fn [_ {:keys [type] :as record}]
                           type))

(defmethod execute-record :halt [ctx _]
  (reduced ctx))

(defmethod execute-record :hash-threshold [ctx {:keys [max-result-set-size]}]
  (assoc ctx :max-result-set-size max-result-set-size))

(defmethod execute-record :statement [{:keys [db-engine] :as ctx} {:keys [mode statement]}]
  (case mode
    :ok (update ctx :db-engine execute-statement statement)
    :error (do (t/is (thrown? Exception (execute-statement db-engine statement)))
               ctx)))

(defn- format-result-str [sort-mode type-string result]
  (let [result-rows (for [vs result]
                      (for [[t v] (map vector type-string vs)]
                        (if (nil? v)
                          "NULL"
                          (case t
                            \R (format "%.3f" v)
                            \I (format "%d" (long v))
                            (if (= "" v)
                              "(empty)"
                              (str v))))))
        result-rows (case sort-mode
                      :rowsort (flatten (sort-by (partial str/join " ") result-rows))
                      :valuesort (sort (flatten result-rows))
                      :nosort (flatten result-rows))]
    (str (str/join "\n" result-rows) "\n")))

(defn- validate-type-string [type-string result]
  (doseq [row result
          [value type] (map vector row type-string)
          :let [pred (case (str type)
                       "I" integer?
                       "R" float?
                       "T" some?)]]
    (t/is (or (nil? value) (pred value)))))

(defn- validate-result-set-size [result-set-size result]
  (t/is (= result-set-size (* (count result) (count (first result))))))

(defn- md5 ^String [^String s]
  (->> (.getBytes s StandardCharsets/UTF_8)
       (.digest (MessageDigest/getInstance "MD5"))
       (BigInteger. 1)
       (format "%032x")))

(defmethod execute-record :query [{:keys [db-engine max-result-set-size] :as ctx}
                                  {:keys [query type-string sort-mode label
                                          result-set-size result-set result-set-md5sum]}]

  (let [result (execute-query db-engine query)
        result-str (format-result-str sort-mode type-string result)]
    (when result-set
      (t/is (= (str (str/join "\n" result-set) "\n") result-str)))
    (when result-set-md5sum
      (t/is (= result-set-md5sum (md5 result-str))))
    ctx))

(defn- skip-record? [db-engine-name {:keys [skipif onlyif]
                                     :or {onlyif db-engine-name}}]
  (or (= db-engine-name skipif)
      (not= db-engine-name onlyif)))

(def ^:dynamic *db-engine*)

(def ^:private ^:dynamic *current-record* nil)

(defn execute-records [db-engine records]
  (with-redefs [clojure.test/do-report
                (fn [m]
                  (t/report
                   (case (:type m)
                     (:fail :error) (merge (select-keys *current-record* [:file :line]) m)
                     m)))]
    (->> (remove (partial skip-record? (get-engine-name db-engine)) records)
         (reduce (fn [db-engine record]
                   (binding [*current-record* record]
                     (execute-record db-engine record)))
                 {:db-engine db-engine})
         :db-engine)))

(defn- ns-relative-path ^java.io.File [ns file]
  (str (str/replace (namespace-munge (ns-name ns)) "." "/") "/" file))

(defn with-xtdb [f]
  (require 'core2.sql.logic-test.xtdb-engine)
  (binding [*db-engine* tu/*node*]
    (f)))

(defn with-sqlite [f]
  (with-open [c (DriverManager/getConnection "jdbc:sqlite::memory:")]
    (binding [*db-engine* c]
      (f))))

;; NOTE: this is called deftest to make cider-test happy, but could be
;; configured via cider-test-defining-forms.
;; Relative tests use hyphen instead of slash, as that confuses cider.
(defmacro deftest [name]
  (let [test-symbol (vary-meta name assoc :slt true)
        test-path (ns-relative-path *ns* (str (str/replace name "-" "/") ".test"))]
    `(alter-meta!
      (t/deftest ~test-symbol
        (execute-records *db-engine* (parse-script ~test-path (slurp (io/resource ~test-path)))))
      assoc :file ~test-path)))
