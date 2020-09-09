(ns crux.lucene-test
  (:require [clojure.test :as t]
            [crux.api :as c]
            [crux.codec :as cc]
            [crux.fixtures :as fix :refer [*api* submit+await-tx with-tmp-dir]]
            [crux.lucene :as l]
            [crux.memory :as mem]
            [crux.node :as n]
            [crux.db :as db])
  (:import org.apache.lucene.analysis.Analyzer
           [org.apache.lucene.document Document Field StoredField TextField]
           [org.apache.lucene.index IndexWriter IndexWriterConfig]
           org.apache.lucene.store.Directory))

(defn with-lucene-module [f]
  (with-tmp-dir "lucene" [db-dir]
    (fix/with-opts (-> fix/*opts*
                       (update ::n/topology conj crux.lucene/module)
                       (assoc :crux.lucene/db-dir db-dir))
      f)))

(t/use-fixtures :each fix/with-standalone-topology with-lucene-module fix/with-kv-dir fix/with-node)

(t/deftest test-can-search-string
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])

  (assert (not-empty (c/q (c/db *api*) '{:find [?id]
                                         :where [[?id :name "Ivan"]]})))

  (let [d ^Directory (:directory (:crux.lucene/node (:crux.node/topology (meta *api*))))
        analyzer ^Analyzer (:analyzer (:crux.lucene/node (:crux.node/topology (meta *api*))))
        iwc (IndexWriterConfig. analyzer)]

    ;; Index
    (with-open [iw (IndexWriter. d, iwc)]
      (let [doc (doto (Document.)
                  (.add (Field. "eid", (mem/->on-heap (cc/->value-buffer :ivan)) StoredField/TYPE))
                  (.add (Field. "name", "Ivan", TextField/TYPE_STORED)))]
        (.addDocument iw doc)))

    ;; Search Manually:
    (with-open [search-results ^crux.api.ICursor (l/search *api* "name" "Ivan")]
      (let [docs (iterator-seq search-results)]
        (t/is (= 1 (count docs)))
        (t/is (= "Ivan" (.get ^Document (first docs) "name")))))

    ;; option 1:
    ;; We store the eid in the lucene doc and AV
    ;; The pred-fn does a search, it needs to idenfity entities
    ;; for each result out of lucene, we want to do the bitemporal dance, is there anything else?

    ;; Step 1, without pruning out stale data from lucene, we need to re-check the current C of the E still has the AV
    ;; how do we do this? Can compare the v in the doc.

    ;; option 2 to write up:
    ;; JMS -> go to AVE
    ;; unindex-eids

    ;; Search Pred-fn:
    (with-open [db (c/open-db *api*)]
      (t/is (not-empty (c/q db {:find '[?e]
                                :where '[[(?full-text :name "Ivan")]
                                         [?e :crux.db/id]]
                                :args [{:?full-text (fn [attr v]
                                                      (with-open [search-results ^crux.api.ICursor (l/search *api* (name attr) v)]
                                                        (let [{:keys [entity-resolver-fn index-store]} db
                                                              eids (->> (iterator-seq search-results)
                                                                        (keep (fn [^Document doc]
                                                                                (let [eid (mem/->off-heap (.-bytes (.getBinaryValue doc "eid")))
                                                                                      v (db/encode-value index-store (.getBinaryValue ^Document doc (name attr)))]
                                                                                  (when (db/aev index-store attr eid v entity-resolver-fn)
                                                                                    eid)))))]

                                                          ;; I'll need to make value->id-buffer?
                                                          ;; Could store eids in lucene as id-buffers

                                                          ;; Need the v..
                                                          (boolean (seq eids)))))}]})))))

  #_{:find '[?id]
     :where '[[?e :gender :female]]
     :args [{:?e :?name} (full-text-search :name "Ivan")]}


  ;; Musings:

  ;; Why wouldn't you put the TT into docs.
  ;;  Presumably docs then can't be compacted?
  ;;  Diff to Crux architecture of splitting content vs temporal

  ;; Need to return an entity-id + val (destructuring), to match

  ;; Questions (jms/jdt)
  ;;  Score (weak result matches?)
  ;;  Don't want to eagerly pull back all results (give me best 10 results) - impact Crux Query ordering, witout a subsequent re-order
  ;;  Cardinality many? (make a test for this)

  ;; ---------------

  ;; References:

  ;; See https://github.com/juxt/crux-rnd/blob/master/src/crux/datalog/lucene.clj
  ;; https://www.toptal.com/database/full-text-search-of-dialogues-with-apache-lucene
  ;; https://lucene.apache.org/core/8_6_0/core/index.html
  )
