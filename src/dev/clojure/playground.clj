(ns playground
  (:require [integrant.core :as ig]
            [integrant.repl :as ir]
            [xtdb.util :as util]
            [xtdb.pgwire.playground :as pgpg]))

(defmethod ig/init-key ::playground [_ _]
  (pgpg/open-playground))

(defmethod ig/halt-key! ::playground [_ playground]
  (util/close playground))

(ir/set-prep! (fn []
                {::playground {}}))


#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def go ir/go)

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def halt ir/halt)

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def reset ir/reset)
