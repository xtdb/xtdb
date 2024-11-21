(ns xtdb.antlr
  (:require [xtdb.error :as err])
  (:import (com.github.benmanes.caffeine.cache Cache Caffeine)
           (org.antlr.v4.runtime BaseErrorListener CharStreams CommonTokenStream Recognizer)
           (xtdb.antlr Sql SqlLexer)))

(def ^:private ^com.github.benmanes.caffeine.cache.Cache parser-cache
  (-> (Caffeine/newBuilder)
      (.maximumSize 4096)
      (.build)))

(defn add-throwing-error-listener [^Recognizer x]
  (doto x
    (.removeErrorListeners)
    (.addErrorListener
     (proxy
      [BaseErrorListener] []
       (syntaxError [_ _ line char-position-in-line msg _]
         (throw
          (err/illegal-arg :xtdb/sql-error
                           {::err/message (str "Errors parsing SQL statement:\n  - "
                                               (format "line %s:%s %s" line char-position-in-line msg))})))))))

(defn ->parser ^xtdb.antlr.Sql [sql]
  (-> (SqlLexer. (CharStreams/fromString sql))
      (add-throwing-error-listener)
      (CommonTokenStream.)
      (Sql.)
      (add-throwing-error-listener)))

(defn parse-statement ^xtdb.antlr.Sql$DirectlyExecutableStatementContext [sql]
  (.get parser-cache [sql :single]
        (fn [[sql _]]
          (let [parser (->parser sql)]
            (-> (.directSqlStatement parser)
                #_(doto (-> (.toStringTree parser) read-string (clojure.pprint/pprint))) ; <<no-commit>>
                (.directlyExecutableStatement))))))

(defn parse-multi-statement [sql]
  (.get parser-cache [sql :multi]
        (fn [[sql _]]
          (let [parser (->parser sql)]
            (-> (.multiSqlStatement parser)
                #_(doto (-> (.toStringTree parser) read-string (clojure.pprint/pprint))) ; <<no-commit>>
                (.directlyExecutableStatement))))))
