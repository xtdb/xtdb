(ns ^{:clojure.tools.namespace.repl/load false, :clojure.tools.namespace.repl/unload false} xtdb.api
  (:import java.io.Writer
           java.time.Instant))

(defrecord TransactionInstant [^long tx-id, ^Instant system-time]
  Comparable
  (compareTo [_ tx-key]
    (Long/compare tx-id (.tx-id ^TransactionInstant tx-key))))

(defmethod print-dup TransactionInstant [tx-key ^Writer w]
  (.write w "#xt/tx-key ")
  (print-method (into {} tx-key) w))

(defmethod print-method TransactionInstant [tx-key w]
  (print-dup tx-key w))

(defrecord ClojureForm [form])

(defmethod print-dup ClojureForm [{:keys [form]} ^Writer w]
  (.write w "#xt/clj-form ")
  (print-method form w))

(defmethod print-method ClojureForm [clj-form w]
  (print-dup clj-form w))
