(ns juxt.crux-ui.frontend.events.default-db
  (:require [clojure.string :as s]
            [juxt.crux-ui.frontend.cookies :as c]))

(def example-query-str
  (s/join "\n"
          ["{:find [e]"
           " :where"
           " [[e :crux.db/id _]]"
           "; options"
           " :full-results? true}"]))

(def ^:private now (js/Date.))

(def default-db
  {:db.query/input           example-query-str
   :db.query/time            {:time/vt now :time/tt now}
   :db.query/input-committed example-query-str
   :db.query/limit           10000
   :db.sys/host              "localhost:8080"
   :db.ui/root-tab           :db.ui.root-tab/query-ui
   :db.ui/output-side-tab    nil ;:db.ui.output-tab/table
   :db.ui/output-main-tab    nil ;:db.ui.output-tab/table
   :db.ui/editor-key         0
   :db.ui.examples/closed?   (c/get :db.ui.examples/closed? false)
   :db.query/key             0
   :db.query/error           nil
   :db.query/result          nil})
