((nil . ((cider-preferred-build-tool . gradle)
         (cider-ns-refresh-before-fn . "integrant.repl/suspend")
         (cider-ns-refresh-after-fn  . "integrant.repl/resume")
         (cider-clojure-cli-aliases . ":xtdb:dev")))
 (clojure-mode . ((eval . (define-clojure-indent
                            (match 1)
                            (for-all 1)))
                  (eval . (add-to-list 'cider-test-defining-forms "def-slt-test")))))
