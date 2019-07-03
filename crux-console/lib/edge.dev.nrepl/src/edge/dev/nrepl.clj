(ns ^{:clojure.tools.namespace.repl/load false} edge.dev.nrepl
  (:require
   [nrepl.server]
   [clojure.java.io :as io]
   [cider.nrepl]
   [refactor-nrepl.middleware :as refactor.nrepl]
   [io.aviso.ansi]))

(def piggieback
  (try
    (require 'cider.piggieback)
    (resolve 'cider.piggieback/wrap-cljs-repl)
    (catch java.io.FileNotFoundException _
      ;; Ignore, piggieback isn't present
      )))

(defn start-nrepl
  [opts]
  (let [server
        (nrepl.server/start-server
          :port (:port opts)
          :handler
          (apply nrepl.server/default-handler
                 (cond-> (map #'cider.nrepl/resolve-or-fail cider.nrepl/cider-middleware)
                   true
                   (conj #'refactor.nrepl/wrap-refactor)
                   piggieback
                   (conj piggieback))))]
    (let [port (:port server)
          port-file (io/file ".nrepl-port")]
      (.deleteOnExit port-file)
      (spit port-file port))
    (println (io.aviso.ansi/yellow (str "[Edge] nREPL client can be connected to port " (:port server))))
    server))

(def port (or (some->
                (System/getenv "NREPL_PORT")
                Integer/parseInt)
              5600))

(println "[Edge] Starting nREPL server on port" port)

(def server (start-nrepl {:port port}))
