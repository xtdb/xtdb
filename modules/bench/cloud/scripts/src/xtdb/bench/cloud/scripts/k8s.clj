(ns xtdb.bench.cloud.scripts.k8s
  "Kubernetes operations for benchmark workflows.
   Migrated from complex bash scripts in GitHub workflows."
  (:require [babashka.process :as proc]
            [cheshire.core :as json]
            [clojure.string :as str]))

;; kubectl helpers

(defn- log-stderr [& args]
  "Log to stderr for workflow visibility."
  (binding [*out* *err*]
    (apply println args)))

(defn kubectl
  "Execute kubectl command and return parsed JSON output.
   Returns nil if command fails."
  [& args]
  (try
    (let [full-args (concat ["kubectl"] args ["-o" "json"])
          result (apply proc/shell {:out :string :err :string} full-args)]
      (when (zero? (:exit result))
        (json/parse-string (:out result) true)))
    (catch Exception _
      nil)))

(defn kubectl-raw
  "Execute kubectl command and return raw output string."
  [& args]
  (try
    (let [full-args (concat ["kubectl"] args)
          result (apply proc/shell {:out :string :err :string} full-args)]
      (when (zero? (:exit result))
        (str/trim (:out result))))
    (catch Exception _
      nil)))

(defn kubectl-get-jobs
  "Get all jobs in a namespace as parsed JSON."
  [namespace]
  (or (kubectl "get" "jobs" "-n" namespace)
      {:items []}))

(defn kubectl-get-pods
  "Get pods in a namespace, optionally filtered by label selector."
  ([namespace]
   (or (kubectl "get" "pods" "-n" namespace)
       {:items []}))
  ([namespace selector]
   (or (kubectl "get" "pods" "-n" namespace "-l" selector)
       {:items []})))

;; inspect-deployment - Migrated from run-nightly-benchmark.yml:120-169

(defn- classify-job
  "Classify a k8s job into status categories."
  [job]
  (let [status (:status job)
        active (or (:active status) 0)
        succeeded (or (:succeeded status) 0)
        failed (or (:failed status) 0)
        conditions (or (:conditions status) [])
        has-complete? (some #(and (= "Complete" (:type %)) (= "True" (:status %))) conditions)
        has-failed? (some #(and (= "Failed" (:type %)) (= "True" (:status %))) conditions)]
    {:name (get-in job [:metadata :name])
     :running? (pos? active)
     :failed? (or (pos? failed) has-failed?)
     :succeeded? (or (pos? succeeded) has-complete?)
     :terminal? (or (pos? succeeded) (pos? failed) has-complete? has-failed?)
     :pending? (and (zero? active) (zero? succeeded) (zero? failed) (empty? conditions))}))

(defn inspect-deployment
  "Inspect existing k8s deployment and determine status.

   Returns JSON map:
   {:status \"in_progress\"|\"completed\"|\"failed\"
    :jobNames [...]
    :terminalJobs [...]
    :failedJobs [...]
    :succeededJobs [...]}"
  ([] (inspect-deployment "cloud-benchmark"))
  ([namespace]
   (log-stderr "Inspecting deployment in namespace:" namespace)
   (let [jobs-response (kubectl-get-jobs namespace)
         jobs (:items jobs-response)
         classified (map classify-job jobs)

         job-names (mapv :name classified)
         running-jobs (filterv :running? classified)
         failed-jobs (filterv :failed? classified)
         succeeded-jobs (filterv :succeeded? classified)
         terminal-jobs (filterv :terminal? classified)
         pending-jobs (filterv :pending? classified)

         status (cond
                  (empty? jobs) "completed"
                  (seq running-jobs) "in_progress"
                  (seq failed-jobs) "failed"
                  (or (seq succeeded-jobs) (seq terminal-jobs)) "completed"
                  (seq pending-jobs) "in_progress"
                  :else "in_progress")]

     (log-stderr "Deployment status:" status)
     (log-stderr "Jobs discovered:" (str/join ", " job-names))
     (log-stderr "Running:" (count running-jobs) "Failed:" (count failed-jobs)
                 "Succeeded:" (count succeeded-jobs))

     {:status status
      :jobNames job-names
      :terminalJobs (mapv :name terminal-jobs)
      :failedJobs (mapv :name failed-jobs)
      :succeededJobs (mapv :name succeeded-jobs)})))

;; check-immediate-failure - Migrated from run-nightly-benchmark.yml:314-438

(def ^:private failure-reasons
  "Container state reasons that indicate a failure."
  #{"CrashLoopBackOff" "ImagePullBackOff" "ErrImagePull"
    "CreateContainerConfigError" "RunContainerError" "ContainerCannotRun" "Error"})

(defn- container-failing?
  "Check if a container status indicates failure."
  [container-status]
  (let [terminated (get-in container-status [:state :terminated])
        waiting (get-in container-status [:state :waiting])
        terminated-exit (when terminated (:exitCode terminated))
        waiting-reason (when waiting (:reason waiting))]
    (or (and terminated (not= 0 terminated-exit))
        (and waiting-reason (contains? failure-reasons waiting-reason)))))

(defn- pod-failing?
  "Check if a pod is in a failing state."
  [pod]
  (let [phase (get-in pod [:status :phase])
        container-statuses (or (get-in pod [:status :containerStatuses]) [])
        init-statuses (or (get-in pod [:status :initContainerStatuses]) [])]
    (or (= "Failed" phase)
        (some container-failing? container-statuses)
        (some container-failing? init-statuses))))

(defn- pod-running-stable?
  "Check if a pod is running with all containers started and init containers completed."
  [pod]
  (let [phase (get-in pod [:status :phase])
        container-statuses (or (get-in pod [:status :containerStatuses]) [])
        init-statuses (or (get-in pod [:status :initContainerStatuses]) [])
        containers-running? (and (seq container-statuses)
                                 (some #(get-in % [:state :running]) container-statuses))
        inits-completed? (every? (fn [init]
                                   (let [term (get-in init [:state :terminated])]
                                     (and term (= 0 (:exitCode term)))))
                                 init-statuses)]
    (and (= "Running" phase)
         containers-running?
         inits-completed?)))

(defn- get-pod-logs
  "Get logs from a pod, returns nil on failure."
  [namespace pod-name & {:keys [tail all-containers] :or {tail 200 all-containers true}}]
  (try
    (let [args (cond-> ["logs" pod-name "-n" namespace]
                 tail (conj (str "--tail=" tail))
                 all-containers (conj "--all-containers=true"))
          result (apply kubectl-raw args)]
      result)
    (catch Exception _ nil)))

(defn check-immediate-failure
  "Poll benchmark pods for immediate failures after deployment.

   Returns JSON map:
   {:failed false}
   or
   {:failed true
    :podName \"pod-name\"
    :logPreview \"first 60 lines of logs\"
    :cleanupTriggered false}"
  ([] (check-immediate-failure {}))
  ([{:keys [namespace attempts sleep-seconds required-running-seconds]
     :or {namespace "cloud-benchmark"
          attempts 36
          sleep-seconds 10
          required-running-seconds 120}}]
   (log-stderr "Checking for immediate benchmark pod failures...")
   (loop [attempt 1
          running-since nil]
     (if (> attempt attempts)
       (do
         (log-stderr "No immediate benchmark pod failures detected in the initial monitoring window.")
         {:failed false})

       (let [pods-response (kubectl-get-pods namespace "app.kubernetes.io/component=benchmark")
             pods (:items pods-response)
             benchmark-pods (filterv #(= "benchmark"
                                         (get-in % [:metadata :labels (keyword "app.kubernetes.io/component")]))
                                     pods)]

         (if (empty? benchmark-pods)
           (do
             (log-stderr (format "Attempt %d/%d: no benchmark pods detected yet." attempt attempts))
             (Thread/sleep (* sleep-seconds 1000))
             (recur (inc attempt) running-since))

           (let [failing-pods (filterv pod-failing? benchmark-pods)
                 completed-pods (filterv #(= "Succeeded" (get-in % [:status :phase])) benchmark-pods)
                 running-pods (filterv pod-running-stable? benchmark-pods)]

             (cond
               ;; Found failing pods
               (seq failing-pods)
               (let [pod-names (mapv #(get-in % [:metadata :name]) failing-pods)
                     primary-pod (first pod-names)
                     log-preview (when primary-pod
                                   (some-> (get-pod-logs namespace primary-pod :tail 60)
                                           (str/split-lines)
                                           (->> (take 60))
                                           (str/join "\n")))
                     cleanup-triggered? (and log-preview
                                             (str/includes? log-preview "Triggered cleanup workflow"))]
                 (log-stderr (format "Error detected: %d failing benchmark pod(s) soon after deployment."
                                     (count failing-pods)))
                 ;; Log pod details to stderr
                 (doseq [pod-name pod-names]
                   (log-stderr "Pod:" pod-name)
                   (when-let [logs (get-pod-logs namespace pod-name)]
                     (log-stderr logs)))
                 {:failed true
                  :podName primary-pod
                  :logPreview log-preview
                  :cleanupTriggered cleanup-triggered?})

               ;; Pods already completed successfully
               (seq completed-pods)
               (do
                 (log-stderr "Benchmark job already completed successfully.")
                 {:failed false})

               ;; Check running stability
               (seq running-pods)
               (let [now (System/currentTimeMillis)
                     since (or running-since now)
                     elapsed-seconds (double (/ (- now since) 1000))]
                 (if (nil? running-since)
                   (do
                     (log-stderr "Benchmark pods entered running state; starting stability timer.")
                     (Thread/sleep (* sleep-seconds 1000))
                     (recur (inc attempt) now))
                   (if (>= elapsed-seconds required-running-seconds)
                     (do
                       (log-stderr (format "Benchmark pods have been running for at least %ds; considering startup successful."
                                           required-running-seconds))
                       {:failed false})
                     (do
                       (log-stderr (format "Benchmark pods running for %.0fs (target: %ds)."
                                           elapsed-seconds required-running-seconds))
                       (Thread/sleep (* sleep-seconds 1000))
                       (recur (inc attempt) since)))))

               ;; Still waiting
               :else
               (do
                 (when running-since
                   (log-stderr "Benchmark pods no longer running; resetting stability timer."))
                 (Thread/sleep (* sleep-seconds 1000))
                 (recur (inc attempt) nil))))))))))

;; capture-benchmark-logs - Migrated from nightly-benchmark-cleanup.yml:191-300

(defn- extract-params-edn
  "Extract benchmark parameters EDN from log content."
  [log-content]
  (when log-content
    (let [pattern #"INFO\s+xtdb\.bench\.(tpch|auctionmark|yakbench|readings|clickbench)\s+-\s+(\{.*\})"
          match (re-find pattern log-content)]
      (when match
        (nth match 2)))))

(defn- extract-scale-factor
  "Extract scale-factor from params EDN string."
  [params-edn]
  (when params-edn
    (when-let [match (re-find #":scale-factor\s+([0-9.]+)" params-edn)]
      (Double/parseDouble (second match)))))

(defn- extract-time-taken-ms
  "Extract last time-taken-ms from log content."
  [log-content]
  (when log-content
    (let [matches (re-seq #"\"time-taken-ms\":(\d+)" log-content)]
      (when (seq matches)
        (Long/parseLong (second (last matches)))))))

(defn- fetch-pod-logs-with-retry
  "Fetch logs from a pod with retry logic."
  [namespace pod-name max-attempts]
  (loop [attempt 1]
    (if (> attempt max-attempts)
      nil
      (let [;; Try primary container first
            logs (or (kubectl-raw "logs" pod-name "-n" namespace "-c" "xtdb-node")
                     (kubectl-raw "logs" pod-name "-n" namespace))]
        (if (and logs (not (str/blank? logs)))
          logs
          (do
            (log-stderr (format "Waiting for logs from %s (attempt %d)..." pod-name attempt))
            (Thread/sleep 5000)
            (recur (inc attempt))))))))

(defn- fetch-previous-logs
  "Try to get previous container logs (for restarted pods)."
  [namespace pod-name]
  (or (kubectl-raw "logs" pod-name "-n" namespace "-c" "xtdb-node" "--previous")
      (kubectl-raw "logs" pod-name "-n" namespace "--previous")))

(defn capture-benchmark-logs
  "Capture logs from all benchmark pods.

   Returns JSON map:
   {:logs {\"pod-name\" \"log-content\"}
    :primaryLog \"content of primary log\"
    :paramsEdn \"{:scale-factor 1.0 ...}\"
    :scaleFactor 1.0
    :totalTimeMs 12345
    :timeTakenIso \"PT1H2M3.456S\"}"
  ([job-name] (capture-benchmark-logs job-name {}))
  ([job-name {:keys [namespace max-attempts]
              :or {namespace "cloud-benchmark" max-attempts 12}}]
   (log-stderr "Capturing benchmark logs for job:" job-name)
   (let [;; Try job-name label first, fall back to component label
         pods-by-job (kubectl-get-pods namespace (str "job-name=" job-name))
         pods-by-component (when (empty? (:items pods-by-job))
                             (kubectl-get-pods namespace "app.kubernetes.io/name=xtdb,app.kubernetes.io/component=benchmark"))
         pods (:items (if (seq (:items pods-by-job)) pods-by-job pods-by-component))
         pod-names (mapv #(get-in % [:metadata :name]) pods)

         ;; Collect logs from each pod
         logs-map (into {}
                        (for [pod-name pod-names]
                          (let [logs (or (fetch-pod-logs-with-retry namespace pod-name max-attempts)
                                         (fetch-previous-logs namespace pod-name))]
                            [pod-name logs])))

         ;; Find primary log (first non-empty)
         primary-log (->> logs-map vals (filter (complement str/blank?)) first)

         ;; Extract metrics from logs
         all-logs-content (str/join "\n" (vals logs-map))
         params-edn (extract-params-edn all-logs-content)
         scale-factor (extract-scale-factor params-edn)

         ;; Calculate average time across pods
         times-ms (keep (fn [[_ log]] (extract-time-taken-ms log)) logs-map)
         total-ms (when (seq times-ms)
                    (quot (reduce + times-ms) (count times-ms)))

         ;; Format as ISO duration
         time-iso (when total-ms
                    (let [total-seconds (quot total-ms 1000)
                          rem-ms (mod total-ms 1000)
                          hours (quot total-seconds 3600)
                          minutes (quot (mod total-seconds 3600) 60)
                          seconds (mod total-seconds 60)]
                      (str "PT"
                           (when (pos? hours) (str hours "H"))
                           (when (pos? minutes) (str minutes "M"))
                           (when (or (pos? seconds) (pos? rem-ms) (and (zero? hours) (zero? minutes)))
                             (if (pos? rem-ms)
                               (format "%d.%03dS" seconds rem-ms)
                               (str seconds "S"))))))]

     (log-stderr "Captured logs from" (count logs-map) "pods")
     (when params-edn (log-stderr "Params EDN:" params-edn))
     (when scale-factor (log-stderr "Scale factor:" scale-factor))
     (when total-ms (log-stderr "Average time:" total-ms "ms"))

     {:logs logs-map
      :primaryLog primary-log
      :paramsEdn params-edn
      :scaleFactor scale-factor
      :totalTimeMs total-ms
      :timeTakenIso time-iso})))

;; compute-grafana-time-range - Migrated from nightly-benchmark-cleanup.yml:317-349

(defn compute-grafana-time-range
  "Compute Grafana time range from benchmark log.
   Handles midnight crossing by trying both today and yesterday.

   Returns JSON map:
   {:from epoch-ms-or-\"now-2h\"
    :to epoch-ms-or-\"now\"}"
  ([log-path] (compute-grafana-time-range log-path {}))
  ([log-path _opts]
   (let [default-result {:from "now-2h" :to "now"}]
     (if-not (and log-path (.exists (java.io.File. log-path)))
       default-result
       (try
         (let [content (slurp log-path)
               ;; Extract first timestamp like HH:MM:SS.mmm
               pattern #"^(\d{2}):(\d{2}):(\d{2})\.(\d{3})"
               match (re-find pattern content)]
           (if-not match
             default-result
             (let [[_ hours-str mins-str secs-str ms-str] match
                   hours (Integer/parseInt hours-str)
                   mins (Integer/parseInt mins-str)
                   secs (Integer/parseInt secs-str)
                   ms (Integer/parseInt ms-str)

                   ;; Get today and yesterday dates
                   now (java.time.Instant/now)
                   now-epoch-ms (.toEpochMilli now)
                   zone (java.time.ZoneOffset/UTC)
                   today (.atZone now zone)
                   yesterday (.minusDays today 1)

                   ;; Build candidate timestamps for today and yesterday
                   make-candidate (fn [zdt]
                                    (-> zdt
                                        (.withHour hours)
                                        (.withMinute mins)
                                        (.withSecond secs)
                                        (.withNano (* ms 1000000))
                                        (.toInstant)
                                        (.toEpochMilli)))

                   cand1 (make-candidate today)
                   cand2 (make-candidate yesterday)

                   ;; Pick the candidate that's in the past and closest to now
                   candidates (filter #(<= % now-epoch-ms) [cand1 cand2])
                   pick (when (seq candidates)
                          (apply max-key #(- now-epoch-ms %) candidates))]

               (if pick
                 {:from pick :to now-epoch-ms}
                 default-result))))
         (catch Exception e
           (log-stderr "Error computing Grafana time range:" (.getMessage e))
           default-result))))))

;; wait-for-benchmark-completion - Migrated from nightly-benchmark-cleanup.yml:92-163

(defn wait-for-benchmark-completion
  "Wait for benchmark job and pods to complete.

   Returns JSON map:
   {:status \"success\"|\"failure\"|\"unknown\"
    :timedOut false}"
  ([job-name] (wait-for-benchmark-completion job-name {}))
  ([job-name {:keys [namespace max-iterations sleep-seconds]
              :or {namespace "cloud-benchmark" max-iterations 720 sleep-seconds 10}}]
   (log-stderr "Waiting for benchmark job" job-name "to complete...")

   ;; Check current state
   (let [selector "app.kubernetes.io/name=xtdb,app.kubernetes.io/component=benchmark"
         initial-pods (kubectl-get-pods namespace selector)
         initial-items (:items initial-pods)
         succeeded (count (filter #(= "Succeeded" (get-in % [:status :phase])) initial-items))
         failed (count (filter #(= "Failed" (get-in % [:status :phase])) initial-items))
         non-terminal (- (count initial-items) succeeded failed)]

     (log-stderr (format "Current pod state: %d succeeded, %d failed, %d non-terminal"
                         succeeded failed non-terminal)))

   ;; Poll job status
   (loop [iteration 1]
     (if (> iteration max-iterations)
       (do
         (log-stderr "Timed out waiting for job completion")
         {:status "unknown" :timedOut true})

       (let [job (kubectl "get" "job" job-name "-n" namespace)
             conditions (get-in job [:status :conditions] [])
             complete? (some #(and (= "Complete" (:type %)) (= "True" (:status %))) conditions)
             failed? (some #(and (= "Failed" (:type %)) (= "True" (:status %))) conditions)]

         (cond
           complete?
           (do
             (log-stderr "Job" job-name "completed successfully")
             {:status "success" :timedOut false})

           failed?
           (do
             (log-stderr "Job" job-name "failed")
             {:status "failure" :timedOut false})

           :else
           ;; Check pods - try job-name label first, fall back to component label
           (let [pods-by-job (kubectl-get-pods namespace (str "job-name=" job-name))
                 pods-by-component (kubectl-get-pods namespace "app.kubernetes.io/name=xtdb,app.kubernetes.io/component=benchmark")
                 pod-items (if (seq (:items pods-by-job))
                             (:items pods-by-job)
                             (:items pods-by-component))
                 all-terminal? (and (seq pod-items)
                                    (every? #(contains? #{"Succeeded" "Failed"}
                                                        (get-in % [:status :phase]))
                                            pod-items))]
             (if all-terminal?
               (do
                 (log-stderr "All benchmark pods are terminal; stopping job wait")
                 ;; Determine final status from pod phases
                 (let [any-failed? (some #(= "Failed" (get-in % [:status :phase])) pod-items)]
                   {:status (if any-failed? "failure" "success") :timedOut false}))
               (do
                 (Thread/sleep (* sleep-seconds 1000))
                 (recur (inc iteration)))))))))))

;; derive-benchmark-status - Helper for determining final benchmark status

(defn derive-benchmark-status
  "Derive benchmark status from job and pod states.

   Returns JSON map:
   {:status \"success\"|\"failure\"|\"unknown\"}"
  ([job-name] (derive-benchmark-status job-name {}))
  ([job-name {:keys [namespace] :or {namespace "cloud-benchmark"}}]
   (let [job (kubectl "get" "job" job-name "-n" namespace)
         ;; Helper: infer status from pod phases (for multi-pod jobs where job
         ;; conditions may lag behind pod completion)
         infer-from-pods (fn []
                           (let [selector "app.kubernetes.io/name=xtdb,app.kubernetes.io/component=benchmark"
                                 pods (:items (kubectl-get-pods namespace selector))
                                 any-failed? (some #(= "Failed" (get-in % [:status :phase])) pods)
                                 all-succeeded? (and (seq pods)
                                                     (every? #(= "Succeeded" (get-in % [:status :phase])) pods))]
                             (cond
                               any-failed? {:status "failure"}
                               all-succeeded? {:status "success"}
                               :else {:status "unknown"})))]
     (if job
       (let [conditions (get-in job [:status :conditions] [])
             complete? (some #(and (= "Complete" (:type %)) (= "True" (:status %))) conditions)
             failed? (some #(and (= "Failed" (:type %)) (= "True" (:status %))) conditions)]
         (cond
           failed? {:status "failure"}
           complete? {:status "success"}
           :else (infer-from-pods)))
       (infer-from-pods)))))
