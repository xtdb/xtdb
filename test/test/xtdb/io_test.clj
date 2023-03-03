(ns xtdb.io-test
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.fixtures :as f]
            [xtdb.io :as xio]))

(t/deftest exp-backoff-test
  (let [tl-sleep-counter (ThreadLocal.)
        sleep-behaviour (atom
                          (fn [ms]
                            (.set tl-sleep-counter (+ (or (.get tl-sleep-counter) 0) ms))
                            (when (.isInterrupted (Thread/currentThread)) (throw (InterruptedException.)))))
        log-behaviour (atom (constantly nil))
        spin (partial f/spin-until-true 100)
        run-exp-backoff (fn t
                          [{:keys [f
                                   exp-backoff-opts
                                   req-interrupt
                                   max-retries]}]
                          (let [attempts (atom 0)
                                slept (atom 0)
                                fut-actually-done (promise)
                                fut (future
                                      (.set tl-sleep-counter 0)
                                      (try
                                        (xio/exp-backoff (fn []
                                                           (when (= max-retries (dec (swap! attempts inc)))
                                                             (throw (InterruptedException.)))
                                                           (f)) exp-backoff-opts)
                                        (finally
                                          (reset! slept (.get tl-sleep-counter))
                                          (deliver fut-actually-done true))))]

                            (when req-interrupt
                              (let [attempt-limit (if (number? max-retries) max-retries 10)]
                                (spin #(<= attempt-limit @attempts))
                                (future-cancel fut)))

                            (when-not (spin #(realized? fut-actually-done))
                              (future-cancel fut))

                            (let [[cex ret] (try [nil @fut] (catch Throwable t [t]))]
                              {:ret ret
                               :attempts @attempts
                               :ex (ex-cause cex)
                               :done (realized? fut-actually-done)
                               :interrupted (future-cancelled? fut)
                               :slept @slept})))]
    (with-redefs [xio/sleep (fn [& args] (apply @sleep-behaviour args))
                  log/log* (fn [& args] (apply @log-behaviour args))]

      (t/testing "No exception, returns immediately"
        (let [{:keys [ret, attempts, slept]} (run-exp-backoff {:f (constantly 42)})]
          (t/is (= 42 ret))
          (t/is (= 1 attempts))
          (t/is (= 0 slept))))

      (t/testing "Exception, no anomaly - retries until recovery"
        (let [ctr (atom 0)
              f (fn [] (when (not= 10 (swap! ctr inc)) (throw (Exception. "nope"))))
              {:keys [attempts, done, slept]} (run-exp-backoff {:f f})]
          (t/is done)
          (t/is (= 10 attempts))
          (t/is (pos? slept))))

      (t/testing "Exception, no anomaly, no recovery - retries until interrupted"
        (let [{:keys [interrupted, slept]} (run-exp-backoff {:f #(throw (Exception. "nope")), :req-interrupt true})]
          (t/is interrupted)
          (t/is (pos? slept))))

      (t/testing "Non recoverable exception is rethrown"
        (let [{:keys [slept, attempts, ex]} (run-exp-backoff {:f #(throw (Error. "foo"))})]
          (t/is (instance? Error ex))
          (t/is (= "foo" (ex-message ex)))
          (t/is (= 1 attempts))
          (t/is (= 0 slept))))

      (t/testing "cognitect anomalies can be used to hint recoverability"
        (doseq [cat [:cognitect.anomalies/busy
                     :cognitect.anomalies/unavailable]]
          (let [{:keys [interrupted, slept]}
                (run-exp-backoff {:f #(throw (ex-info "" {:cognitect.anomalies/category cat}))
                                  :req-interrupt true})]
            (t/is interrupted)
            (t/is (pos? slept)))))

      (t/testing "Non recoverable anomalies rethrow, including interrupted"
        (doseq [cat [:cognitect.anomalies/not-a-cat
                     :cognitect.anomalies/forbidden
                     :cognitect.anomalies/interrupted
                     nil]]
          (let [{:keys [ex]}
                (run-exp-backoff {:f #(throw (ex-info "foo" {:cognitect.anomalies/category cat}))})]
            (t/is (= "foo" (ex-message ex))))))

      (t/testing ":retryable can be used to determine recoverability"
        (let [{:keys [interrupted, slept]} (run-exp-backoff {:f #(throw (Error.)),
                                                             :exp-backoff-opts {:retryable (constantly true)}
                                                             :req-interrupt true,})]
          (t/is interrupted)
          (t/is (pos? slept))))

      (t/testing "Vary the max wait"
        (let [{:keys [slept]} (run-exp-backoff {:f #(throw (Exception.)),
                                                :exp-backoff-opts {:max-wait-ms 1}
                                                :max-retries 10})]
          (t/is (= 10 slept))))

      (t/testing "Vary starting wait"
        (let [{:keys [slept]} (run-exp-backoff {:f #(throw (Exception.)),
                                                :exp-backoff-opts {:start-wait-ms 42}
                                                :max-retries 1})]
          (t/is (= 42 slept))))

      (t/testing "Vary mul"
        (let [{:keys [slept]} (run-exp-backoff {:f #(throw (Exception.)),
                                                :exp-backoff-opts {:start-wait-ms 1, :wait-mul 3}
                                                :max-retries 3})]
          (t/is (= (+ 1 3 9) slept))))

      (t/testing "All together option exponent opts"
        (let [{:keys [slept]} (run-exp-backoff {:f #(throw (Exception.)),
                                                :exp-backoff-opts {:start-wait-ms 2, :wait-mul 2, :max-wait-ms 32}
                                                :max-retries 7})]
          (t/is (= (+ 2 4 8 16 32 32 32) slept))))

      (t/testing "Log behaviour can be customized"
        (let [errs (atom [])]
          (run-exp-backoff {:f #(throw (Exception. (str (count @errs))))
                            :exp-backoff-opts {:on-retry #(swap! errs conj %)}
                            :max-retries 3})
          (t/is (= ["0" "1" "2"] (map ex-message @errs))))))))
