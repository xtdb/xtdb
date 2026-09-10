(ns xtdb.codegen-report
  "Reports reflection and boxed-math warnings in the code the expression engine generates at runtime.

   Runs the EE test namespaces as a code generator - the warnings come from the compiler at the
   moment the EE `eval`s a generated form, so the tests' own verdicts are neither read nor reported.
   Reports; never gates."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [clojure.tools.namespace.find :as ns-find])
  (:import (java.io PrintWriter StringWriter)))

(def ^:private generator-ns-prefixes
  ["xtdb.expression" "xtdb.operator" "xtdb.sql"])

(def ^:private excluded-ns-prefixes
  ["xtdb.sql.logic-test"])

;; `t/test-vars` honours no JUnit tags, so nothing here inherits the exclusions `test` applies
;; in build.gradle.kts - a var carrying one of these would run, and some of them want Docker.
(def ^:private excluded-var-tags
  [:integration :property :jdbc :timescale :s3 :minio :slt :docker :azure :google-cloud])

(def ^:private warning-pattern
  #"^(Reflection|Boxed math) warning, (\S+?):\d+:\d+ - (.+)$")

(defn- generator-nses []
  (->> (ns-find/find-namespaces-in-dir (io/file "src/test/clojure"))
       (filter (fn [n]
                 (let [nm (str n)]
                   (and (some #(str/starts-with? nm %) generator-ns-prefixes)
                        (not-any? #(str/starts-with? nm %) excluded-ns-prefixes)))))
       sort))

(defn- runnable-vars [ns-sym]
  (->> (ns-publics ns-sym)
       vals
       (filter (comp :test meta))
       (remove (fn [v] (some (meta v) excluded-var-tags)))
       (sort-by (comp :name meta))))

(defn- generate-code! []
  (let [nses (generator-nses)
        failures (atom [])]
    (doseq [n nses]
      (try
        (require n)
        (catch Throwable t
          (swap! failures conj (format "%s failed to load: %s" n (ex-message t))))))

    ;; only once loading is done: `:warn-on-boxed` also selects the unchecked arithmetic ops, so from
    ;; here the EE's generated code wraps on overflow instead of raising ::overflow-error.
    ;; that is why the test verdicts below are discarded, and why this can never be folded into `test`.
    (alter-var-root #'*unchecked-math* (constantly :warn-on-boxed))

    (doseq [n nses]
      ;; into the same stream the compiler writes to, so each warning can be attributed to the
      ;; namespace that was running - which is a repro recipe, since that one from cold recompiles
      ;; the shape the whole run only compiles once.
      (doto ^java.io.Writer *err*
        (.write (str "### " n "\n"))
        (.flush))

      (binding [*out* (StringWriter.), t/*test-out* (StringWriter.)]
        (try
          (t/test-vars (runnable-vars n))
          (catch Throwable t
            (swap! failures conj (format "%s died part-way: %s" n (ex-message t)))))))

    {:ns-count (count nses), :failures @failures}))

(defn- parse-warnings [captured]
  (:warnings
   (reduce (fn [{:keys [ns-sym] :as acc} line]
             (if (str/starts-with? line "### ")
               (assoc acc :ns-sym (subs line 4))
               (if-let [[_ kind loc msg] (re-matches warning-pattern line)]
                 (update acc :warnings conj
                         ;; a generated form has no file: the compiler reports whatever `*file*` the
                         ;; compiling thread holds, and the EE compiles off any load context.
                         {:generated? (= "NO_SOURCE_PATH" loc)
                          :kind (if (= "Reflection" kind) :reflection :boxed)
                          :message msg
                          :ns-sym ns-sym})
                 acc)))
           {:ns-sym nil, :warnings []}
           (str/split-lines captured))))

(defn- report [{:keys [ns-count failures]} captured]
  (let [warnings (parse-warnings captured)
        {generated true, elsewhere false} (group-by :generated? warnings)
        by-kind (group-by :kind generated)]

    (with-out-str
      (println (format "%d namespaces, %d generated warnings (%d reflection, %d boxed), %d elsewhere"
                       ns-count (count generated)
                       (count (:reflection by-kind)) (count (:boxed by-kind))
                       (count elsewhere)))

      ;; in the report rather than on the console, so that the file is the whole of what a run found -
      ;; a namespace that didn't run is missing coverage, which matters as much as any warning here.
      (when (seq failures)
        (println)
        (println (format "did not complete (%d)" (count failures)))
        (doseq [failure failures]
          (println (format "  %s" failure))))

      (doseq [[title ws] [["generated - reflection" (:reflection by-kind)]
                          ["generated - boxed math" (:boxed by-kind)]
                          ["elsewhere - ordinary load-time compilation" elsewhere]]]
        (println)
        (println (format "%s (%d)" title (count ws)))
        (doseq [[message ws] (->> ws (group-by :message) (sort-by (comp - count val)))]
          (println (format "  %4d  %s" (count ws) message))
          (doseq [ns-sym (sort (distinct (keep :ns-sym ws)))]
            (println (format "          repro: %s" ns-sym))))))))

(defn -main [& _]
  (let [sw (StringWriter.)
        result (atom nil)]

    ;; roots rather than `binding`/`set!`, with the work on a plain thread so that nothing shadows them.
    ;; clojure.main binds both flags for its own thread, and the EE compiles on threads that inherit no bindings from ours; a raw Thread conveys none either, so here every thread reads the root.
    (alter-var-root #'*warn-on-reflection* (constantly true))
    (alter-var-root #'*err* (constantly (PrintWriter. sw)))

    (doto (Thread. #(reset! result (generate-code!)))
      (.start)
      (.join))

    ;; to a file as well as the console: stdout also carries whatever the nodes these tests stand up
    ;; logged, so the file is the copy anything downstream should read.
    (let [report (report @result (str sw))
          out-file (io/file "build" "codegen-report.txt")]
      (print report)
      (io/make-parents out-file)
      (spit out-file report)))

  (shutdown-agents)
  (System/exit 0))
