(ns crux.lucene.egeria
  (:require [clojure.spec.alpha :as s]
            [crux.lucene :as l]
            [crux.query :as q])
  (:import org.apache.lucene.analysis.Analyzer
           [org.apache.lucene.analysis.core KeywordTokenizerFactory LowerCaseFilterFactory]
           [org.apache.lucene.analysis.custom CustomAnalyzer]
           org.apache.lucene.queryparser.classic.QueryParser
           [org.apache.lucene.search Query BooleanClause$Occur BooleanQuery$Builder]))

;; This namespace facilitates the evolving requirements of https://github.com/odpi/egeria-connector-crux
;; It also serves to demonstrate an example of custom `crux-lucene` usage

(defn ^Query build-query-egeria
  "Wildcard query builder with leading wildcard position enabled"
  [^Analyzer analyzer, [v]]
  (when-not (string? v)
    (throw (IllegalArgumentException. "Lucene text search values must be String")))
  (let [qp (doto (QueryParser. l/field-crux-val analyzer)
             (.setAllowLeadingWildcard true))
        b (doto (BooleanQuery$Builder.)
            (.add (.parse qp v) BooleanClause$Occur/MUST))]
    (.build b)))

(defmethod q/pred-args-spec 'egeria-text-search [_]
  (s/cat :pred-fn #{'egeria-text-search} :args (s/spec (s/cat :v string?)) :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'egeria-text-search [_ pred-ctx]
  (l/pred-constraint #'build-query-egeria l/resolve-search-results-a-v-wildcard pred-ctx))

(defn ->analyzer
  [_]
  (.build (doto (CustomAnalyzer/builder)
            (.withTokenizer ^String (cast String KeywordTokenizerFactory/NAME) ^"[Ljava.lang.String;" (into-array String []))
            (.addTokenFilter ^String (cast String LowerCaseFilterFactory/NAME) ^"[Ljava.lang.String;" (into-array String [])))))
