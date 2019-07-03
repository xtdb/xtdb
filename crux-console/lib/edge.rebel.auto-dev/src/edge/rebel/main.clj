(ns edge.rebel.main
  (:require
    [clojure.pprint :as pp]
    [fipp.edn :as fipp]
    rebel-readline.clojure.main
    rebel-readline.core
    [rebel-readline.jline-api :as api]
    [rebel-readline.clojure.line-reader :as clj-line-reader]
    io.aviso.ansi))

(defn syntax-highlight-pprint
  "Print a syntax highlighted clojure value.

  This printer respects the current color settings set in the
  service.

  The `rebel-readline.jline-api/*line-reader*` and
  `rebel-readline.jline-api/*service*` dynamic vars have to be set for
  this to work."
  [x]
  (binding [*out* (.. api/*line-reader* getTerminal writer)]
    (try
      (print (api/->ansi
               (clj-line-reader/highlight-clj-str
                 (with-out-str (pp/pprint x)))))
      (catch java.lang.StackOverflowError e
        (pp/pprint x)))))

(defn syntax-highlight-fipp
  "Print a syntax highlighted clojure value.

  This printer respects the current color settings set in the
  service.

  The `rebel-readline.jline-api/*line-reader*` and
  `rebel-readline.jline-api/*service*` dynamic vars have to be set for
  this to work."
  [x]
  (binding [*out* (.. api/*line-reader* getTerminal writer)]
    (try
      (print (api/->ansi
               (clj-line-reader/highlight-clj-str
                 (with-out-str (fipp/pprint x)))))
      (catch java.lang.StackOverflowError _
        (try
          (fipp/pprint x)
          ;; Just in case of
          ;; https://github.com/brandonbloom/fipp/issues/28
          (catch java.lang.StackOverflowError _
            (prn x)))))))

(defn -main
  [& args]
  (rebel-readline.core/ensure-terminal
    (rebel-readline.clojure.main/repl
      :init (fn []
              (try
                (println "[Edge] Loading Clojure code, please wait...")
                (require 'dev)
                (in-ns 'dev)
                (println (str
                           (io.aviso.ansi/yellow "[Edge] Now enter ")
                           (io.aviso.ansi/bold-yellow "(go)")
                           (io.aviso.ansi/yellow " to start the dev system")))

                (catch Exception e
                  (if (= (.getMessage e)
                         "Could not locate dev__init.class, dev.clj or dev.cljc on classpath.")
                    (do
                      (println (io.aviso.ansi/red "[Edge] Failed to require dev. Falling back to `user`. "))
                      (println (io.aviso.ansi/bold-red "[Edge] Make sure to supply `-A:dev` when running `../bin/rebel`.")))
                    
                    (do
                      (.printStackTrace e)
                      (println "[Edge] Failed to require dev, this usually means there was a syntax error. See exception above.")
                      (println "[Edge] Please correct it, and enter (fixed!) to resume development."))))))
      :print syntax-highlight-pprint)
    ;; When the REPL stops, stop:
    (System/exit 0)))
