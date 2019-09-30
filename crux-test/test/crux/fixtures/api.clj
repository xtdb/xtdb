(ns crux.fixtures.api
  (:import [crux.api Crux ICruxAPI]))

(def ^:dynamic ^ICruxAPI *api*)
(def ^:dynamic *opts* nil)

(defn with-opts [opts f]
  (binding [*opts* (merge *opts* opts)]
    (f)))

(defn with-node [f]
  (with-open [node (Crux/startNode *opts*)]
    (binding [*api* node]
      (f))))
