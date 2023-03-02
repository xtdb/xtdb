(ns ^{:clojure.tools.namespace.repl/load false, :clojure.tools.namespace.repl/unload false} core2.api
  (:import java.io.Writer
           java.time.Instant))

(defrecord TransactionInstant [^long tx-id, ^Instant sys-time]
  Comparable
  (compareTo [_ tx-key]
    (Long/compare tx-id (.tx-id ^TransactionInstant tx-key))))

(defmethod print-dup TransactionInstant [tx-key ^Writer w]
  (.write w "#c2/tx-key ")
  (print-method (into {} tx-key) w))

(defmethod print-method TransactionInstant [tx-key w]
  (print-dup tx-key w))

(defrecord ClojureForm [form])

(defmethod print-dup ClojureForm [{:keys [form]} ^Writer w]
  (.write w "#c2/clj-form ")
  (print-method form w))

(defmethod print-method ClojureForm [clj-form w]
  (print-dup clj-form w))
