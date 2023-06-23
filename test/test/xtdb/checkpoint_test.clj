(ns xtdb.checkpoint-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.bus :as bus]
            [xtdb.checkpoint :as cp]
            [xtdb.fixtures :as fix]
            [xtdb.fixtures.checkpoint-store :as fix.cp-store])
  (:import [java.io Closeable File]
           [java.time Duration Instant]
           java.time.temporal.ChronoUnit
           java.util.Date
           [java.time Duration]))

(t/deftest test-cp-seq
  (let [start (Instant/parse "2020-01-01T00:00:00Z")
        an-hour (Duration/ofHours 1)]
    (t/is (= (->> (iterate #(.plus ^Instant % an-hour) start)
                  (take 20))
             (->> (cp/cp-seq start an-hour)
                  (map #(.truncatedTo ^Instant % ChronoUnit/HOURS))
                  (take 20))))))

(defn checkpoint [{:keys [approx-frequency tx-id-override ::cp/cp-format dir]} checkpoints]
  (let [!checkpoints (atom checkpoints)]
    (with-open [bus ^Closeable (bus/->bus)
                _ (bus/->bus-stop {:bus bus})]
      (cp/checkpoint {::cp/cp-format cp-format
                      :dir dir
                      :bus bus
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
                                 (let [tx-id (or tx-id-override (swap! !tx-id inc))]
                                   (spit (doto (io/file dir "hello.edn")
                                           (io/make-parents))
                                         (pr-str {:msg "Hello world!", :tx-id tx-id}))
                                   {:tx {::xt/tx-id tx-id}}))))}))
    @!checkpoints))

(t/deftest test-checkpoint
  (fix/with-tmp-dir "cp" [cp-dir]
    (let [cp-1 {:tx {::xt/tx-id 1},
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

(t/deftest test-check-for-changes
  (fix/with-tmp-dir "cp" [cp-dir]
    (let [cp-1 {:tx {::xt/tx-id 1},
                ::cp/cp-format ::foo-format,
                :files {"hello.edn" {:msg "Hello world!", :tx-id 1}}}
          cp-2 {:tx {::xt/tx-id 2},
                ::cp/cp-format ::foo-format,
                :files {"hello.edn" {:msg "Hello world!", :tx-id 2}}}]
      (t/testing "first checkpoint"
        (t/is (= [cp-1]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir
                                   ;; explicitly set tx-id to 1
                                   :tx-id-override 1}
                                  [])
                      (map #(dissoc % ::cp/checkpoint-at))))))

      (t/testing "doesn't make a second checkpoint as it's the same latest tx"
        (t/is (= [cp-1]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir
                                   ;; would be same tx-id as above (ie, same tx)
                                   :tx-id-override 1}
                                  [(assoc cp-1 ::cp/checkpoint-at (-> (Date.) 
                                                                      (.toInstant)
                                                                      (.minus (Duration/ofSeconds 2))
                                                                      (Date/from)))])
                      (map #(dissoc % ::cp/checkpoint-at))))))
      
      (t/testing "makes a second checkpoint as it has a new latest tx"
        (t/is (= [cp-1 cp-2]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir
                                   ;; different tx-id to above (ie, new)
                                   :tx-id-override 2}
                                  [(assoc cp-1 ::cp/checkpoint-at (-> (Date.)
                                                                      (.toInstant)
                                                                      (.minus (Duration/ofSeconds 2))
                                                                      (Date/from)))])
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

(t/deftest test-fs-checkpoint-store-failed-download
  (fix/with-tmp-dirs #{cp-store-dir}
    (fix.cp-store/test-checkpoint-broken-store-failed-download
      (cp/->filesystem-checkpoint-store {:path (.toPath cp-store-dir)}))))

(t/deftest test-fs-checkpoint-store-failed-upload
  (fix/with-tmp-dirs #{cp-store-dir dir}
    (with-open [bus ^Closeable (bus/->bus)
                _ (bus/->bus-stop {:bus bus})]
      (with-redefs [xtdb.checkpoint/sync-path @#'fix.cp-store/sync-path-throw]
        (let [store (cp/->filesystem-checkpoint-store {:path (.toPath cp-store-dir)})]
          (t/testing "no leftovers after failed checkpoint"
            (t/is (thrown-with-msg?
                   Exception #"broken!"
                   (cp/checkpoint {::cp/cp-format ::foo-format
                                   :dir dir
                                   :bus bus
                                   :approx-frequency (Duration/ofHours 1)
                                   :store store
                                   :src (let [!tx-id (atom 0)]
                                          (reify cp/CheckpointSource
                                            (save-checkpoint [_ dir]
                                              (let [tx-id (swap! !tx-id inc)]
                                                (spit (doto (io/file dir "hello.edn")
                                                        (io/make-parents))
                                                      (pr-str {:msg "Hello world!", :tx-id tx-id}))
                                                {:tx {::xt/tx-id tx-id}}))))})))
            (t/is (= 0 (.count (java.nio.file.Files/list (.toPath cp-store-dir)))))))))))
