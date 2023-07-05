(ns xtdb.checkpoint-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.bus :as bus]
            [xtdb.checkpoint :as cp]
            [xtdb.fixtures :as fix]
            [xtdb.fixtures.checkpoint-store :as fix.cp-store]
            [xtdb.io :as xio])
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

(defn checkpoint [{:keys [approx-frequency tx-id-override ::cp/cp-format dir retention-policy]} checkpoints]
  (let [!checkpoints (atom checkpoints)]
    (with-open [bus ^Closeable (bus/->bus)
                _ (bus/->bus-stop {:bus bus})]
      (cp/checkpoint {::cp/cp-format cp-format
                      :dir dir
                      :bus bus
                      :approx-frequency approx-frequency
                      :retention-policy retention-policy
                      :store (reify cp/CheckpointStore
                               (available-checkpoints [_ {:keys [::cp/cp-format]}]
                                 (->> (reverse @!checkpoints)
                                      (filter #(= cp-format (::cp/cp-format %)))))
                               (upload-checkpoint [_ dir {:keys [tx cp-at ::cp/cp-format]}]
                                 (let [cp {:tx tx,
                                           ::cp/cp-format cp-format
                                           ::cp/checkpoint-at cp-at
                                           :files (->> (file-seq dir)
                                                       (filter #(.isFile ^File %))
                                                       (into {} (map (juxt #(.getName ^File %)
                                                                           (comp read-string slurp)))))}]
                                   (swap! !checkpoints conj cp)
                                   cp))
                               (cleanup-checkpoint [_ {:keys [tx cp-at ::cp/cp-format] :as keys}]
                                 (let [cp-removed (remove
                                                   (fn [cp]
                                                     (and (= (:tx cp) tx)
                                                          (= (::cp/cp-format cp) cp-format)
                                                          (= (::cp/checkpoint-at cp) cp-at)))
                                                   @!checkpoints)]
                                   (reset! !checkpoints cp-removed)))),
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
      (t/testing "first checkpoint, no prior checkpoints"
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

(t/deftest test-checkpoint-meta-sent-to-uploaded-checkpoints
  (fix/with-tmp-dir "cp" [cp-dir]
    (t/testing "testing all the necessary keys sent when uploading checkpoint"
      (let [[res] (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                               ::cp/cp-format ::foo-format
                               :dir cp-dir}
                            [])]
        (t/is (= {::xt/tx-id 1} (:tx res)))
        (t/is (= {"hello.edn" {:msg "Hello world!", :tx-id 1}} (:files res)))
        (t/is (= ::foo-format (::cp/cp-format res)))
                 ;; Check if checkpoint-at (sent to upload-checkpoint as cp-at) satisfies Inst, should be a Date
        (t/is (inst? (::cp/checkpoint-at res)))))))

(t/deftest test-retention-retain-at-least
  (fix/with-tmp-dir "cp" [cp-dir]
    (let [cp-1 {:tx {::xt/tx-id 1},
                ::cp/cp-format ::foo-format,
                :files {"hello.edn" {:msg "Hello world!", :tx-id 1}}}
          cp-2 {:tx {::xt/tx-id 2},
                ::cp/cp-format ::foo-format,
                :files {"hello.edn" {:msg "Hello world!", :tx-id 2}}}]
      (t/testing "first checkpoint, retain at least 1 - should return 1 checkpoint"
        (t/is (= [cp-1]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir
                                   :retention-policy {:retain-at-least 1}}
                                  [])
                      (map #(dissoc % ::cp/checkpoint-at))))))

      (t/testing "second checkpoint, 1 prior checkpoint, retain at least 1 - old checkpoint should be removed"
        (t/is (= [cp-2]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir
                                   :retention-policy {:retain-at-least 1}
                                   ;; different tx-id to available-checkpoint (ie, new)
                                   :tx-id-override 2}
                                  [cp-1])
                      (map #(dissoc % ::cp/checkpoint-at))))))

      (t/testing "second checkpoint, 1 prior checkpoint, retain at least 2 - should return both checkpoints"
        (t/is (= [cp-1 cp-2]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir
                                   :retention-policy {:retain-at-least 2}
                                   ;; different tx-id to available-checkpoint (ie, new)
                                   :tx-id-override 2}
                                  [cp-1])
                      (map #(dissoc % ::cp/checkpoint-at)))))))))

(t/deftest test-retention-retain-newer-than
  (fix/with-tmp-dir "cp" [cp-dir]
    (let [cp-1 {:tx {::xt/tx-id 1},
                ::cp/cp-format ::foo-format,
                :files {"hello.edn" {:msg "Hello world!", :tx-id 1}}}
          cp-2 {:tx {::xt/tx-id 2},
                ::cp/cp-format ::foo-format,
                :files {"hello.edn" {:msg "Hello world!", :tx-id 2}}}]
      (t/testing "first checkpoint, retain newer than 1 day ago - should return 1 checkpoint"
        (t/is (= [cp-1]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir
                                   :retention-policy {:retain-newer-than (Duration/ofDays 1)}}
                                  [])
                      (map #(dissoc % ::cp/checkpoint-at))))))

      (t/testing "second checkpoint, 1 prior checkpoint, retain newer than 1 day ago - old checkpoint should be removed (as it is 2 days old)"
        (t/is (= [cp-2]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir
                                   :retention-policy {:retain-newer-than (Duration/ofDays 1)}
                                   ;; different tx-id to available-checkpoint (ie, new)
                                   :tx-id-override 2}
                                  [(assoc cp-1 ::cp/checkpoint-at (-> (Date.)
                                                                      (.toInstant)
                                                                      (.minus (Duration/ofDays 2))
                                                                      (Date/from)))])
                      (map #(dissoc % ::cp/checkpoint-at))))))

      (t/testing "second checkpoint, 1 prior checkpoint, retain newer than 3 days ago - should return both checkpoints"
        (t/is (= [cp-1 cp-2]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir
                                   :retention-policy {:retain-newer-than (Duration/ofDays 3)}
                                   ;; different tx-id to available-checkpoint (ie, new)
                                   :tx-id-override 2}
                                  [(assoc cp-1 ::cp/checkpoint-at (-> (Date.)
                                                                      (.toInstant)
                                                                      (.minus (Duration/ofDays 2))
                                                                      (Date/from)))])
                      (map #(dissoc % ::cp/checkpoint-at)))))))))

(t/deftest test-retention-both-args
  (fix/with-tmp-dir "cp" [cp-dir]
    (let [cp-1 {:tx {::xt/tx-id 1},
                ::cp/cp-format ::foo-format,
                :files {"hello.edn" {:msg "Hello world!", :tx-id 1}}}
          cp-2 {:tx {::xt/tx-id 2},
                ::cp/cp-format ::foo-format,
                :files {"hello.edn" {:msg "Hello world!", :tx-id 2}}}]

      (t/testing "first checkpoint, retain at least 1 & retain newer than 1 day ago - should return 1 checkpoint"
        (t/is (= [cp-1]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir
                                   :retention-policy {:retain-at-least 1
                                                      :retain-newer-than (Duration/ofDays 1)}}
                                  [])
                      (map #(dissoc % ::cp/checkpoint-at))))))

      (t/testing "second checkpoint, 1 prior checkpoint from 2 days ago, retain at least 1 & retain newer than 1 day ago - old checkpoint should be removed"
        (t/is (= [cp-2]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir
                                   :retention-policy {:retain-at-least 1
                                                      :retain-newer-than (Duration/ofDays 1)}
                                   ;; different tx-id to available-checkpoint (ie, new)
                                   :tx-id-override 2}
                                  [(assoc cp-1 ::cp/checkpoint-at (-> (Date.)
                                                                      (.toInstant)
                                                                      (.minus (Duration/ofDays 2))
                                                                      (Date/from)))])
                      (map #(dissoc % ::cp/checkpoint-at))))))

      (t/testing "second checkpoint, 1 prior checkpoint from 2 days ago, retain at least 2 & retain newer than 1 day ago - old checkpoint should be kept"
        (t/is (= [cp-1 cp-2]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir
                                   :retention-policy {:retain-at-least 2
                                                      :retain-newer-than (Duration/ofDays 1)}
                                   ;; different tx-id to available-checkpoint (ie, new)
                                   :tx-id-override 2}
                                  [(assoc cp-1 ::cp/checkpoint-at (-> (Date.)
                                                                      (.toInstant)
                                                                      (.minus (Duration/ofDays 2))
                                                                      (Date/from)))])
                      (map #(dissoc % ::cp/checkpoint-at))))))

      (t/testing "second checkpoint, 1 prior checkpoint from 2 days ago, retain at least 1 & retain newer than 3 days ago - old checkpoint should be kept"
        (t/is (= [cp-1 cp-2]
                 (->> (checkpoint {:approx-frequency (Duration/ofSeconds 1)
                                   ::cp/cp-format ::foo-format
                                   :dir cp-dir
                                   :retention-policy {:retain-at-least 1
                                                      :retain-newer-than (Duration/ofDays 3)}
                                   ;; different tx-id to available-checkpoint (ie, new)
                                   :tx-id-override 2}
                                  [(assoc cp-1 ::cp/checkpoint-at (-> (Date.)
                                                                      (.toInstant)
                                                                      (.minus (Duration/ofDays 2))
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
          (t/testing "no leftovers (of what _was_ uploaded) after failed checkpoint - this doesn't include the temp EDN file as we fail prior to that"
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

(t/deftest test-fs-checkpoint-store-cleanup
  (fix/with-tmp-dirs #{cp-store-dir dir}
    (let [cp-store (cp/->filesystem-checkpoint-store {:path (.toPath cp-store-dir)})
          cp-at (Date.)]

      ;; create file for upload
      (spit (io/file dir "hello.txt") "Hello world")

      (let [{:keys [::cp/cp-uri]} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                      :tx {::xt/tx-id 1}
                                                                      :cp-at cp-at})]
        (t/testing "call to upload-checkpoint creates expected folder & checkpoint metadata file for the checkpoint"
          (t/is (.exists (io/file cp-uri)))
          (t/is (.exists (io/file (str cp-uri ".edn")))))

        (t/testing "call to `cleanup-checkpoints` entirely removes an uploaded checkpoint and metadata"
          (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                           :cp-at cp-at})
          (t/is (= false (.exists (io/file cp-uri))))
          (t/is (= false (.exists (io/file (str cp-uri ".edn"))))))))))

(t/deftest test-fs-checkpoint-store-failed-cleanup
  (fix/with-tmp-dirs #{cp-store-dir dir}
      (let [cp-store (cp/->filesystem-checkpoint-store {:path (.toPath cp-store-dir)})
            cp-at (Date.)
            ;; create file for upload
            _ (spit (io/file dir "hello.txt") "Hello world")
            {:keys [::cp/cp-uri]} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                      :tx {::xt/tx-id 1}
                                                                      :cp-at cp-at})]

        (t/testing "error in `cleanup-checkpoints` after deleting checkpoint metadata file still leads to checkpoint not being available"
          (with-redefs [xio/delete-dir (fn [_] (throw (Exception. "Test Exception")))]
            (t/is (thrown-with-msg? Exception
                                    #"Test Exception"
                                    (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                                                     :cp-at cp-at}))))
          ;; Only directory should be available - checkpoint metadata file should have been deleted
          (t/is (.exists (io/file cp-uri)))
          (t/is (= false (.exists (io/file (str cp-uri ".edn")))))
          ;; Should not be able to fetch checkpoint as checkpoint metadata file is gone
          (t/is (empty? (cp/available-checkpoints cp-store ::foo-cp-format)))))))


(t/deftest test-fs-retention-policy
  (fix/with-tmp-dirs #{cp-store-dir dir}
    (with-open [bus ^Closeable (bus/->bus)
                _ (bus/->bus-stop {:bus bus})]
      (let [store (cp/->filesystem-checkpoint-store {:path (.toPath cp-store-dir)})
            !tx-id (atom 0)
            checkpoint-opts {::cp/cp-format ::foo-format
                             :dir dir
                             :bus bus
                             :approx-frequency (Duration/ofMillis 1)
                             :store store
                             :retention-policy {:retain-at-least 2}
                             :src (reify cp/CheckpointSource
                                    (save-checkpoint [_ dir]
                                      (let [tx-id (swap! !tx-id inc)]
                                        (spit (doto (io/file dir "hello.edn")
                                                (io/make-parents))
                                              (pr-str {:msg "Hello world!", :tx-id tx-id}))
                                        {:tx {::xt/tx-id tx-id}})))}]

        ;; create file for upload
        (spit (io/file dir "hello.txt") "Hello world")

        (t/testing "make initial checkpoint"
          (cp/checkpoint checkpoint-opts)
          (t/is (= 2 (.count (java.nio.file.Files/list (.toPath cp-store-dir))))))

        (Thread/sleep 10)

        (t/testing "make second checkpoint (should have files for two checkpoints, as per retention policy)"
          (cp/checkpoint checkpoint-opts)
          (t/is (= 4 (.count (java.nio.file.Files/list (.toPath cp-store-dir))))))

        (Thread/sleep 10)

        (t/testing "make third checkpoint (should have files for two checkpoints, as per retention policy)"
          (cp/checkpoint checkpoint-opts)
          ;; Files.delete is NOT synchronous, so need to wait for it to complete
          (Thread/sleep 100)
          (t/is (= 4 (.count (java.nio.file.Files/list (.toPath cp-store-dir))))))))))

(t/deftest test-fs-checkpoint-store-cleanup-no-edn-file
  (fix/with-tmp-dirs #{cp-store-dir dir}
    (let [cp-store (cp/->filesystem-checkpoint-store {:path (.toPath cp-store-dir)})
          cp-at (Date.)]

      ;; create file for upload
      (spit (io/file dir "hello.txt") "Hello world")

      (let [{:keys [::cp/cp-uri]} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                      :tx {::xt/tx-id 1}
                                                                      :cp-at cp-at})]

        (.delete (io/file (str cp-uri ".edn")))

        (t/testing "checkpoint files present, edn file should be deleted"
          (t/is (.exists (io/file cp-uri)))
          (t/is (= false (.exists (io/file (str cp-uri ".edn"))))))

        (t/testing "call to `cleanup-checkpoints` with no edn file should still remove an uploaded checkpoint and metadata"
          (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                           :cp-at cp-at})
          (t/is (= false (.exists (io/file cp-uri))))
          (t/is (= false (.exists (io/file (str cp-uri ".edn"))))))))))
