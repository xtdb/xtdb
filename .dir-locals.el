((nil . ((cider-preferred-build-tool . gradle)
         (cider-gradle-parameters . ":clojureRepl")
         (cider-ns-refresh-before-fn . "integrant.repl/suspend")
         (cider-ns-refresh-after-fn  . "integrant.repl/resume")))
 (clojure-mode . ((eval . (define-clojure-indent
                            (match 1)
                            (for-all 1)
                            (as-> 2))))))
