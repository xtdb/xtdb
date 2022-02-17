(ns ^:no-doc xtdb.fixtures
  (:require [clojure.test :as t]
            [clojure.test.check.clojure-test :as tcct]
            [xtdb.api :as xt]
            [xtdb.db :as db]
            [xtdb.io :as xio])
  (:import (java.lang AutoCloseable)
           (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (java.util ArrayList Date List UUID)
           (xtdb.api IXtdb)))

(defn with-silent-test-check [f]
  (binding [tcct/*report-completion* false]
    (f)))

(defn with-tmp-dir* [prefix f]
  (let [dir (.toFile (Files/createTempDirectory prefix (make-array FileAttribute 0)))]
    (try
      (f dir)
      (finally
        (xio/delete-dir dir)))))

(defmacro ^:deprecated with-tmp-dir [prefix [dir-binding] & body]
  `(with-tmp-dir* ~prefix
     (fn [~(-> dir-binding (vary-meta assoc :tag 'java.io.File))]
       ~@body)))

(defmacro with-tmp-dirs
  "Usage:
    (with-tmp-dirs #{node-dir lucene-dir}
      ...)"
  [[dir-binding & more-bindings] & body]
  (if dir-binding
    `(with-tmp-dir* ~(name dir-binding)
       (fn [~(vary-meta dir-binding assoc :tag 'java.io.File)]
         (with-tmp-dirs #{~@more-bindings}
           ~@body)))
    `(do ~@body)))

(def ^:dynamic ^IXtdb *api*)
(def ^:dynamic *opts* [])

(defn with-opts
  ([opts] (fn [f] (with-opts opts f)))
  ([opts f]
   (binding [*opts* (conj *opts* opts)]
     (f))))

(defn with-node [f]
  (with-open [node (xt/start-node *opts*)]
    (binding [*api* node]
      (f))))

(defn submit+await-tx
  ([tx-ops] (submit+await-tx *api* tx-ops))
  ([api tx-ops]
   (let [tx (xt/submit-tx api tx-ops)]
     (xt/await-tx api tx)
     tx)))

(defn maps->tx-ops
  ([maps]
   (vec (for [m maps]
          [::xt/put m])))
  ([maps ts]
   (vec (for [m maps]
          [::xt/put m ts]))))

(defn transact!
  "Helper fn for transacting entities "
  ([api entities]
   (transact! api entities (Date.)))
  ([^IXtdb api entities ts]
   (let [tx (xt/submit-tx api (maps->tx-ops entities ts))]
     (xt/await-tx api tx))
   entities))

(defn random-person []
  {:xt/id (UUID/randomUUID)
   :name (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
   :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
   :sex (rand-nth [:male :female])
   :age (rand-int 100)
   :salary (rand-int 100000)})

(defn people [people-mixins]
  (->> people-mixins (map merge (repeatedly random-person))))

;; Literal vectors aren't type hinted as List in Clojure, and cannot
;; be type hinted without via a var.
(defn vec->array-list ^java.util.List [^List v]
  (ArrayList. v))

(defmethod t/assert-expr 'thrown-with-cause? [msg form]
  (let [klass (second form)
        body (nthnext form 2)]
    `(try
       (let [res# ~@body]
         (t/do-report {:type :fail, :message ~msg,
                       :expected '~form, :actual res#}))
       (catch Exception e#
         (try
           (if-let [cause# (.getCause e#)]
             (throw cause#)
             (t/do-report {:type :fail, :message ~msg,
                           :expected '~form, :actual nil}))
           (catch ~klass e#
             (t/do-report {:type :pass, :message ~msg,
                           :expected '~form, :actual e#})
             e#))))))

(comment
  ;; regen stats
  (with-open [^AutoCloseable is (db/open-index-snapshot (:index-store (xt/db (dev/xtdb-node))))]
    (->> (db/all-attrs is)
         (map (fn [attr]
                [attr {:doc-count (db/doc-count is attr)
                       :doc-value-count (db/doc-value-count is attr)
                       :eids (Math/rint (db/eid-cardinality is attr))
                       :vals (Math/rint (db/value-cardinality is attr))}]))
         (into (sorted-map)))))

(defn ->attr-stats [file]
  (let [stats (->> (read-string (slurp file))
                   (into {} (map (juxt (comp keyword key) val))))]
    (reify db/AttributeStats
      (all-attrs [_] (set (keys stats)))
      (doc-count [_ attr] (get-in stats [attr :doc-count]))
      (doc-value-count [_ attr] (get-in stats [attr :doc-value-count]))
      (eid-cardinality [_ attr] (get-in stats [attr :eids]))
      (value-cardinality [_ attr] (get-in stats [attr :vals])))))
