(ns xtdb.remote
  "External `remotes` config (the named connections an external source draws on,
   e.g. a Postgres CDC upstream). Parallels `xtdb.log`'s log-cluster handling:
   `->remote-factory` resolves a `[tag opts]` to a `Remote$Factory`, auto-requiring
   the implementing namespace, and `apply-config! :remotes` wires each alias onto
   the node config."
  (:require [xtdb.node :as xtn])
  (:import [xtdb.api Xtdb$Config]))

(defmulti ->remote-factory
  (fn [k _opts]
    (when-let [ns (namespace k)]
      (doseq [k [(symbol ns)
                 (symbol (str ns "." (name k)))]]
        (try
          (require k)
          (catch Throwable _))))
    k)
  :default ::default)

(defmethod ->remote-factory ::default [k _]
  (throw (ex-info (format "Unknown remote type: %s" k) {:remote-type k})))

;; unqualified shorthand → the implementing namespaced key (which auto-requires
;; its ns on dispatch), mirroring xtdb.log's `:kafka` alias.
(defmethod ->remote-factory :postgres [_ opts]
  (->remote-factory :xtdb.postgres/remote opts))

(defmethod xtn/apply-config! ::remotes [^Xtdb$Config config _ remotes]
  (doseq [[alias [tag opts]] remotes]
    (.remote config (str (symbol alias)) (->remote-factory tag opts)))
  config)
