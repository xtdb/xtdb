(ns crux.checkpoint-test
  (:require [crux.checkpoint :as cp]
            [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.fixtures :as fix])
  (:import (java.util Date)
           (java.io Closeable File)
           (java.time Instant Duration)
           (java.time.temporal ChronoUnit)))

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
                                 {:tx {:crux.tx/tx-id tx-id}}))))})
    @!checkpoints))

(t/deftest test-checkpoint
  (fix/with-tmp-dir "cp" [cp-dir]
    (let [cp-1 {:tx {:crux.tx/tx-id 1},
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
