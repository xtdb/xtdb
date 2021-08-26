(ns xtdb.bench.watdiv-neo4j
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [xtdb.bench :as bench]
            [xtdb.bench.watdiv :as watdiv]
            [crux.io :as cio]
            [xtdb.rdf :as rdf]
            [xtdb.sparql :as sparql])
  (:import java.util.concurrent.TimeUnit
           org.neo4j.dbms.api.DatabaseManagementServiceBuilder
           [org.neo4j.graphdb GraphDatabaseService Label RelationshipType]))

(def ^:dynamic ^GraphDatabaseService *neo4j-db*)
(def neo4j-tx-size 100000)
(def neo4j-index-timeout-ms 120000)

(defn with-neo4j [f]
  (let [db-dir (cio/create-tmpdir "neo4j")
        data-dir (io/file db-dir "data")
        db-manager (-> (DatabaseManagementServiceBuilder. data-dir)
                       (.build))]
    (try
      (f (.database db-manager "neo4j"))
      (finally
        (.shutdown db-manager)
        (cio/delete-dir db-dir)))))

(defn sparql->cypher [^GraphDatabaseService graph-db q]
  (let [relationship? (with-open [tx (.beginTx graph-db)]
                        (set (for [r (iterator-seq (.iterator (.getAllRelationshipTypes tx)))]
                               (keyword (subs (.name ^RelationshipType r) 1)))))
        maybe-fix-variable (fn [x]
                             (if (symbol? x)
                               (subs (name x) 1)
                               x))
        {:keys [find where]} (sparql/sparql->datalog q)
        property-returns (atom {})
        where (if (and (= 1 (count where))
                       (keyword? (ffirst where)))
                (let [[e a v] (first where)
                      tmp (gensym "?tmp")]
                  [[tmp :xt/id e]
                   [tmp a v]])
                where)]
    (str "MATCH " (->> (for [[e a v] where]
                         (if (relationship? a)
                           (format "(%s)-[:`%s`]->(%s)"
                                   (if (symbol? e)
                                     (str (maybe-fix-variable e) ":Entity")
                                     (format ":Entity {`:xt/id`: '%s'}" e))
                                   a
                                   (if (symbol? v)
                                     (str (maybe-fix-variable v) ":Entity")
                                     (format ":Entity {`:xt/id`: '%s'}" v)))
                           (if (symbol? v)
                             (do
                               (swap! property-returns assoc v [e a])
                               nil)
                             (format "(%s:Entity {`%s`: %s})"
                                     (maybe-fix-variable e) a (if (keyword? v)
                                                                (str "'" v "'")
                                                                v)))))
                       (remove nil?)
                       (string/join ", "))
         (when (not-empty @property-returns)
           (str " WHERE " (string/join " AND " (for [[_ [e a]] @property-returns]
                                                 (format "%s.`%s` IS NOT NULL"
                                                         (maybe-fix-variable e)
                                                         a)))))
         " RETURN " (string/join ", " (for [v find]
                                        (if-let [[e a] (get @property-returns v)]
                                          (format "%s.`%s` AS %s"
                                                  (maybe-fix-variable e)
                                                  a
                                                  (maybe-fix-variable v))
                                          (maybe-fix-variable v)))))))

(defn execute-cypher [^GraphDatabaseService graph-db q]
  (with-open [tx (.beginTx graph-db watdiv/query-timeout-ms TimeUnit/MILLISECONDS)
              result (.execute tx q)]
    (vec (iterator-seq result))))

(def entity-label (Label/label "Entity"))

(defn upsert-node [tx id]
  (let [id-str (str id)]
    (or (.findNode tx entity-label (str :xt/id) id-str)
        (doto (.createNode tx (into-array [entity-label]))
          (.setProperty (str :xt/id) id-str)))))

(defn load-rdf-into-neo4j [^GraphDatabaseService graph-db]
  (bench/run-bench :ingest-neo4j
    (with-open [tx (.beginTx graph-db)]
      (-> (.schema tx)
          (.indexFor entity-label)
          (.on (str :xt/id))
          (.create))
      (.commit tx))

    (with-open [in (io/input-stream watdiv/watdiv-input-file)]
      (->> (rdf/ntriples-seq in)
           (map rdf/rdf->clj)
           (map #(rdf/use-default-language % rdf/*default-language*))
           (partition-all neo4j-tx-size)
           (reduce (fn [^long n statements]
                     (with-open [tx (.beginTx graph-db)]
                       (doseq [[s p o] statements]
                         (let [s-node (upsert-node tx s)]
                           (if (keyword? o)
                             (let [o-node (upsert-node tx o)
                                   p-rel (doto (.createRelationshipTo s-node o-node
                                                                      (RelationshipType/withName (str p)))
                                           (.setProperty (str :xt/id) (str p)))])
                             (.setProperty s-node (str p) o))))
                       (.commit tx)
                       (+ n (count statements))))
                   0)))

    (with-open [tx (.beginTx graph-db)]
      (-> (.schema tx)
          (.awaitIndexesOnline neo4j-index-timeout-ms TimeUnit/MILLISECONDS))
      (.commit tx))))

(defn run-watdiv-bench [{:keys [test-count] :as opts}]
  (with-neo4j
    (fn [conn]
      (bench/with-bench-ns :watdiv-neo4j
        (load-rdf-into-neo4j conn)
        (watdiv/with-watdiv-queries watdiv/watdiv-stress-100-1-sparql
          (fn [queries]
            (-> queries
                (cond->> test-count (take test-count))
                (->> (bench/with-thread-pool opts
                       (fn [{:keys [idx q]}]
                         (bench/with-dimensions {:query-idx idx}
                           (bench/run-bench (format "query-%d" idx)
                                            {:result-count (count (execute-cypher conn (sparql->cypher conn q)))}))))))))))))

(defn -main []
  (let [output-file (io/file "neo4j-results.edn")
        watdiv-results (run-watdiv-bench {:test-count 100})]
    (bench/save-to-file output-file (cons (first watdiv-results)
                                          (->> (rest watdiv-results)
                                               (filter :query-idx)
                                               (sort-by :query-idx))))
    (bench/save-to-s3 {:database "neo4j" :version "4.0.0"} output-file)))
