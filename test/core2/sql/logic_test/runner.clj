(ns core2.sql.logic-test.runner
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [core2.sql.plan :as plan]
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
;; reporting

(def error-counts-by-message (atom {}))

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
      (assoc record :result-set-size (count result) :result-set (str (str/join "\n" result) "\n")))))

(defmethod parse-record :skipif [[x & xs]]
  (let [[_ database-name] (str/split x #"\s+")]
    (update (parse-record xs) :skipif (fnil conj []) database-name)))

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

;; Printer

(defn- print-skip-only [{:keys [skipif onlyif] :as record}]
  (doseq [skipif skipif]
    (println "skipif" skipif))
  (when onlyif
    (println "onlyif" onlyif)))

(defmulti print-record (fn [{:keys [type] :as record}]
                         type))

(defmethod print-record :halt [_]
  (println "halt")
  (println))

(defmethod print-record :hash-threshold [{:keys [max-result-set-size]}]
  (println "hash-threshold" max-result-set-size)
  (println))

(defmethod print-record :statement [{:keys [mode statement] :as record}]
  (print-skip-only record)
  (println "statement" (name mode))
  (println statement)
  (println))

(defmethod print-record :query [{:keys [query type-string sort-mode label
                                        result-set-size result-set result-set-md5sum]
                                 :as record}]
  (print-skip-only record)
  (print "query" type-string (name sort-mode))
  (if label
    (println label)
    (println))
  (println query)
  (println "----")
  (if result-set-md5sum
    (do (println result-set-size "values hashing to" result-set-md5sum)
        (println))
    (println result-set)))

;; Runner

(defn skip-record? [db-engine-name {:keys [skipif onlyif] :as record}]
  (let [onlyif (or onlyif db-engine-name)]
    (or (contains? (set skipif) db-engine-name)
        (not= db-engine-name onlyif))))

(defmulti execute-record (fn [_ {:keys [type] :as record}]
                           type))

(defmethod execute-record :halt [{:keys [script-mode] :as ctx} record]
  (when (= :completion script-mode)
    (print-record record))
  (reduced ctx))

(defmethod execute-record :hash-threshold [{:keys [script-mode] :as ctx} {:keys [max-result-set-size] :as record}]
  (when (= :completion script-mode)
    (print-record record))
  (assoc ctx :max-result-set-size max-result-set-size))

(defmethod execute-record :statement [{:keys [db-engine script-mode] :as ctx} {:keys [mode statement] :as record}]
  (if (skip-record? (get-engine-name db-engine) record)
    (do (when (= :completion script-mode)
          (print-record record))
        ctx)
    (if (= :completion script-mode)
      (try
        (let [ctx (update ctx :db-engine execute-statement statement)]
          (print-record (assoc record :mode :ok))
          ctx)
        (catch Exception e
          (print-record (assoc record :mode :error))
          ctx))
      (case mode
        :ok (update ctx :db-engine execute-statement statement)
        :error (do (t/is (thrown? Exception (execute-statement db-engine statement)))
                   ctx)))))

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

(defn- md5 ^String [^String s]
  (->> (.getBytes s StandardCharsets/UTF_8)
       (.digest (MessageDigest/getInstance "MD5"))
       (BigInteger. 1)
       (format "%032x")))

(defmethod execute-record :query [{:keys [db-engine max-result-set-size script-mode] :as ctx}
                                  {:keys [query type-string sort-mode label
                                          result-set-size result-set result-set-md5sum]
                                   :as record}]
  (if (skip-record? (get-engine-name db-engine) record)
    (do (when (= :completion script-mode)
          (print-record record))
        ctx)
    (let [result (execute-query db-engine query)
          result-str (format-result-str sort-mode type-string result)]
      (when (= :completion script-mode)
        (print-record (if (and max-result-set-size (> (count (flatten result)) max-result-set-size))
                        (-> (assoc record :result-set-md5sum (md5 result-str))
                            (dissoc :result-set))
                        (-> (assoc record :result-set result-str)
                            (dissoc :result-set-md5sum)))))
      (when result-set
        (t/is (= result-set result-str)))
      (when result-set-md5sum
        (t/is (= result-set-md5sum (md5 result-str))))
      ctx)))

(def ^:dynamic *db-engine*)

(def ^:dynamic *opts* {:script-mode :validation
                       :query-limit nil})

(def ^:private ^:dynamic *current-record* nil)

(defn execute-records [db-engine records]
  (with-redefs [clojure.test/do-report
                (fn [m]
                  (t/report
                   (case (:type m)
                     (:fail :error) (merge (select-keys *current-record* [:file :line]) m)
                     m)))]
    (let [ctx (merge {:db-engine db-engine :queries-run 0} *opts*)]
      (->> records
           (reduce
            (fn [{:keys [queries-run query-limit] :as ctx} record]
              (binding [*current-record* record]
                (if (= queries-run query-limit)
                  (reduced ctx)
                  (try
                    (-> (execute-record ctx record)
                        (update :queries-run + (if (= :query (:type record))
                                                 1
                                                 0)))
                    (catch Throwable t
                      (t/do-report
                        {:type :error, :message "Error Executing Record"
                         :expected record :actual t})
                      #_(swap!
                          error-counts-by-message
                          (fn [acc]
                            (-> acc
                                (update-in
                                  [(ex-message t) :count]
                                  (fnil inc 0))
                                (update-in
                                  [(ex-message t) :lines]
                                  #(conj % (:line record))))))
                      (update ctx :queries-run + (if (= :query (:type record))
                                                   1
                                                   0))
                      #_(throw t))))))
            ctx)
           :db-engine))))

(defn- ns-relative-path ^java.io.File [ns file]
  (str (str/replace (namespace-munge (ns-name ns)) "." "/") "/" file))

(defn with-opts
  ([opts] (partial with-opts opts))
  ([opts f]
   (binding [*opts* opts]
     (f))))

(defn with-xtdb [f]
  (require 'core2.sql.logic-test.xtdb-engine)
  (binding [*db-engine* tu/*node*]
    (f)))

(defn with-jdbc [url f]
  (with-open [c (DriverManager/getConnection url)]
    (binding [*db-engine* c]
      (f))))

(def with-sqlite (partial with-jdbc "jdbc:sqlite::memory:"))

;; NOTE: this is called deftest to make cider-test happy, but could be
;; configured via cider-test-defining-forms.
;; Relative tests use hyphen instead of slash, as that confuses cider.
(defmacro deftest [name]
  (let [test-symbol (vary-meta name assoc :slt true)
        test-path (ns-relative-path *ns* (str (str/replace name "-" "/") ".test"))]
    `(alter-meta!
      (t/deftest ~test-symbol
        (binding [plan/*include-table-column-in-scan?* true]
          (execute-records *db-engine* (parse-script ~test-path (slurp (io/resource ~test-path))))))
      assoc :file ~test-path)))

(def cli-options
  [[nil "--verify"]
   [nil "--limit LIMIT" :parse-fn #(Long/parseLong %)]
   [nil "--db DB" :default "xtdb" :validate [(fn [x]
                                               (or (contains? #{"xtdb" "sqlite"} x)
                                                   (str/starts-with? x "jdbc:"))) "Unknown db."]]])

(defn -main [& args]
  (let [{:keys [options arguments errors]} (cli/parse-opts args cli-options)
        {:keys [verify db]} options]
    (if (seq errors)
      (binding [*out* *err*]
        (doseq [error errors]
          (println error))
        (System/exit 1))
      (binding [*opts* {:script-mode (if verify
                                       :validation
                                       :completion)
                        :query-limit (:limit options)}
                plan/*include-table-column-in-scan?* true]
        (doseq [script-name arguments
                :let [f #(binding [t/*report-counters* (ref t/*initial-report-counters*)]
                           (execute-records *db-engine* (parse-script script-name (slurp script-name)))
                           (when (:verify options)
                             (println @t/*report-counters*)))]]
          (case db
            "xtdb" (tu/with-node
                     #(with-xtdb f))
            "sqlite" (with-sqlite f)
            (with-jdbc db f)))))))
(comment

  (->> (sort-by (comp :count val) @error-counts-by-message)
       (map #(vector (first %) ((comp (partial take 5) reverse :lines second) %) (:count (second %)))))

  (sort-by val (update-vals (group-by #(subs % 0 20) (map key @error-counts-by-message)) count))


  (time (-main  "--verify" "--db" "xtdb" "test/core2/sql/logic_test/sqlite_test/random/aggregates/slt_good_0.test"))

  (time (-main "--verify" "--db" "sqlite" "test/core2/sql/logic_test/sqlite_test/select4.test"))

  (time (-main "--verify" "--db" "xtdb" "test/core2/sql/logic_test/sqlite_test/select1.test"))

  (= (time
      (with-out-str
        (-main "--db" "xtdb" "--limit" "10" "test/core2/sql/logic_test/sqlite_test/select1.test")))
     (time
      (with-out-str
        (-main "--db" "sqlite" "--limit" "10" "test/core2/sql/logic_test/sqlite_test/select1.test")))))
