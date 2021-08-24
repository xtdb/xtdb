(ns crux.checkpoint-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [crux.checkpoint :as cp]
            [crux.fixtures :as fix]
            [crux.fixtures.checkpoint-store :as fix.cp-store])
  (:import [java.io Closeable File]
           [java.time Duration Instant]
           java.time.temporal.ChronoUnit
           java.util.Date))

(t/deftest test-cp-seq
  (let [start (Instant/parse "2020-01-01T00:00:00Z")
        an-hour (Duration/ofHours 1)]
    (t/is (= (->> (iterate #(.plus ^Instant % an-hour) start)
                  (take 20))
             (->> (cp/cp-seq start an-hour)
                  (map #(.truncatedTo ^Instant % ChronoUnit/HOURS))
                  (take 20))))))

(defn checkpoint [{:keys [approx-frequency ::cp/cp-format dir]} checkpoints]
  (let [!checkpoints (atom checkpoints)]
    (cp/checkpoint {::cp/cp-format cp-format
                    :dir dir
                    :approx-frequency approx-frequency
                    :store (reify cp/CheckpointStore
                             (available-checkpoints [_ {:keys [::cp/cp-format]}]
                               (->> (reverse @!checkpoints)
                                    (filter #(= cp-format (::cp/cp-format %)))))
                             (upload-checkpoint [_ dir {:keys [tx ::cp/cp-format]}]
                               (let [cp {:tx tx,
                                         ::cp/cp-format cp-format
                                         ::cp/checkpoint-at (Date.)
                                         :files (->> (file-seq dir)
                                                     (filter #(.isFile ^File %))
                                                     (into {} (map (juxt #(.getName ^File %)
                                                                         (comp read-string slurp)))))}]
                                 (swap! !checkpoints conj cp)
                                 cp))),
                    :src (let [!tx-id (atom 0)]
                           (reify cp/CheckpointSource
                             (save-checkpoint [_ dir]
                               (let [tx-id (swap! !tx-id inc)]
                                 (spit (doto (io/file dir "hello.edn")
                                         (io/make-parents))
                                       (pr-str {:msg "Hello world!", :tx-id tx-id}))
                                 {:tx {:xt/tx-id tx-id}}))))})
    @!checkpoints))

(t/deftest test-checkpoint
  (fix/with-tmp-dir "cp" [cp-dir]
    (let [cp-1 {:tx {:xt/tx-id 1},
                ::cp/cp-format ::foo-format,
                :files {"hello.edn" {:msg "Hello world!", :tx-id 1}}}]
      (t/testing "first checkpoint"
        (t/is (= [cp-1]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir}
                                  [])
                      (map #(dissoc % ::cp/checkpoint-at))))))

      (t/testing "doesn't do a second checkpoint within half a second"
        (t/is (= [cp-1]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir}
                                  [(assoc cp-1 ::cp/checkpoint-at (Date.))])
                      (map #(dissoc % ::cp/checkpoint-at)))))))))

(t/deftest test-checkpointer
  (fix/with-tmp-dir "cp" [cp-dir]
    (let [checkpointer (cp/map->ScheduledCheckpointer {:store (reify cp/CheckpointStore)
                                                       :checkpoint-dir (.toPath ^File cp-dir)
                                                       :approx-frequency (Duration/ofMillis 50)})
          !cps (atom [])
          !latch (promise)]
      (with-redefs [cp/checkpoint (fn [cp]
                                    (let [cps (swap! !cps conj cp)]
                                      (when (>= (count cps) 5)
                                        (deliver !latch cps))))]
        (let [cps (with-open [^Closeable
                              _job (cp/start checkpointer
                                             (reify cp/CheckpointSource)
                                             {::cp/cp-format ::cp-format})]
                    (deref !latch 500 ::timeout))]
          (t/is (not= ::timeout cps))
          (t/is (= 5 (count cps))))))))

(t/deftest test-fs-checkpoint-store
  (fix/with-tmp-dirs #{cp-store-dir}
    (fix.cp-store/test-checkpoint-store (cp/->filesystem-checkpoint-store {:path (.toPath cp-store-dir)}))))
