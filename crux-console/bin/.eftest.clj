(require '[eftest.runner :refer [find-tests run-tests]])
(require '[eftest.report :refer [report-to-file]])
(require 'eftest.report.progress)
(require 'clojure.test)
(require 'orchard.query)

(def junit? (= (first *command-line-args*) "--junit"))
(def junit-path (when junit?
                  (second *command-line-args*)))

(defn combined-reporter
  "Combines the reporters by running first one directly,
  and others with clojure.test/*report-counters* bound to nil."
  [report rst]
  (fn [m]
    (report m)
    (doseq [report rst]
      (binding [clojure.test/*report-counters* nil]
        (report m)))))

(let [summary (run-tests
                (find-tests
                  ;; This is slow, because requiring namespaces is slow
                  (orchard.query/namespaces {:load-project-ns? true
                                             :project? true}))
                {:report
                 (combined-reporter
                   eftest.report.progress/report
                   (filter
                     identity
                     [(when junit?
                        (report-to-file
                          (requiring-resolve 'eftest.report.junit/report)
                          junit-path))]))})]
  (System/exit (+ (:error summary) (:fail summary))))
