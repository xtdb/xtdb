(ns xtdb.operator.scan-test
  (:require [clojure.test :as t]
            [xtdb.datalog :as xt]
            [xtdb.node :as node]
            [xtdb.operator :as op]
            [xtdb.operator.scan :as scan]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import xtdb.operator.IRaQuerySource))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-simple-scan
  (with-open [node (node/start-node {})]
    (xt/submit-tx node [[:put :xt_docs {:id :foo, :col1 "foo1"}]
                        [:put :xt_docs {:id :bar, :col1 "bar1", :col2 "bar2"}]
                        [:put :xt_docs {:id :foo, :col2 "baz2"}]])

    (t/is (= [{:id :bar, :col1 "bar1", :col2 "bar2"}
              {:id :foo, :col2 "baz2"}]
             (tu/query-ra '[:scan {:table xt_docs} [id col1 col2]]
                          {:node node})))))


(t/deftest test-simple-scan-with-namespaced-attributes
  (with-open [node (node/start-node {})]
    (xt/submit-tx node [[:put :xt_docs {:id :foo, :the-ns/col1 "foo1"}]
                        [:put :xt_docs {:id :bar, :the-ns/col1 "bar1", :col2 "bar2"}]
                        [:put :xt_docs {:id :foo, :the-ns/col2 "baz2"}]])

    (t/is (= [{:id :bar, :the-ns__col1 "bar1", :col2 "bar2"}
              {:id :foo}]
             (tu/query-ra '[:scan {:table xt_docs} [id the-ns__col1 col2]]
                          {:node node})))))

(t/deftest test-duplicates-in-scan-1
  (with-open [node (node/start-node {})]
    (xt/submit-tx node [[:put :xt_docs {:id :foo}]])

    (t/is (= [{:id :foo}]
             (tu/query-ra '[:scan {:table xt_docs} [id id]]
                          {:node node})))))

(t/deftest test-scanning-temporal-cols
  (with-open [node (node/start-node {})]
    (xt/submit-tx node [[:put :xt_docs {:id :doc}
                         {:app-time-start #inst "2021"
                          :app-time-end #inst "3000"}]])

    (let [res (first (tu/query-ra '[:scan {:table xt_docs}
                                    [id
                                     application_time_start application_time_end
                                     system_time_start system_time_end]]
                                  {:node node}))]
      (t/is (= #{:id :application_time_start :application_time_end :system_time_end :system_time_start}
               (-> res keys set)))

      (t/is (= {:id :doc, :application_time_start (util/->zdt #inst "2021"), :application_time_end (util/->zdt #inst "3000")}
               (dissoc res :system_time_start :system_time_end))))

    (t/is (= {:id :doc, :app-time-start (util/->zdt #inst "2021"), :app-time-end (util/->zdt #inst "3000")}
             (-> (first (tu/query-ra '[:project [id
                                                 {app-time-start application_time_start}
                                                 {app-time-end application_time_end}]
                                       [:scan {:table xt_docs}
                                        [id application_time_start application_time_end]]]
                                     {:node node}))
                 (dissoc :system_time_start :system_time_end))))))

(t/deftest test-only-scanning-temporal-cols-45
  (with-open [node (node/start-node {})]
    (let [{tt :sys-time} (xt/submit-tx node [[:put :xt_docs {:id :doc}]])]

      (t/is (= [{:application_time_start (util/->zdt tt)
                 :application_time_end (util/->zdt util/end-of-time)
                 :system_time_start (util/->zdt tt),
                 :system_time_end (util/->zdt util/end-of-time)}]
               (tu/query-ra '[:scan {:table xt_docs}
                              [application_time_start application_time_end
                               system_time_start system_time_end]]
                            {:node node}))))))

(t/deftest test-aligns-temporal-columns-correctly-363
  (with-open [node (node/start-node {})]
    (xt/submit-tx node [[:put :foo {:id :my-doc, :last_updated "tx1"}]] {:sys-time #inst "3000"})

    (xt/submit-tx node [[:put :foo {:id :my-doc, :last_updated "tx2"}]] {:sys-time #inst "3001"})

    (t/is (= [{:system_time_start (util/->zdt #inst "3000")
               :system_time_end (util/->zdt #inst "3001")
               :last_updated "tx1"}
              {:system_time_start (util/->zdt #inst "3001")
               :system_time_end (util/->zdt util/end-of-time)
               :last_updated "tx1"}
              {:system_time_start (util/->zdt #inst "3001")
               :system_time_end (util/->zdt util/end-of-time)
               :last_updated "tx2"}]
             (tu/query-ra '[:scan {:table foo, :for-sys-time :all-time}
                            [{system_time_start (< system_time_start #time/zoned-date-time "3002-01-01T00:00Z")}
                             {system_time_end (> system_time_end #time/zoned-date-time "2999-01-01T00:00Z")}
                             last_updated]]
                          {:node node :default-all-app-time? true})))))

(t/deftest test-for-app-time-in-params
  (let [tt1 (util/->zdt #inst "2020-01-01")
        tt2 (util/->zdt #inst "2020-01-02")
        eot (util/->zdt util/end-of-time)]
    (with-open [node (node/start-node {})]
      (xt/submit-tx node [[:put :foo {:id 1, :version "version 1" :last_updated "tx1"}
                           {:app-time-start tt1 :app-time-end eot}]])

      (xt/submit-tx node [[:put :foo {:id 2, :version "version 2" :last_updated "tx2"}
                           {:app-time-start tt2 :app-time-end eot}]])
      (t/is (= [{:id 1, :version "version 1"} {:id 2, :version "version 2"}]
               (tu/query-ra '[:scan {:table foo,
                                     :default-all-app-time? false
                                     :for-app-time [:between ?_start ?_end]}
                              [id version]]
                            {:node node :params {'?_start (util/->instant tt1)
                                                 '?_end (util/->instant eot)}})))
      (t/is (= [{:id 1, :version "version 1"} {:id 2, :version "version 2"}]
               (tu/query-ra '[:scan {:table foo,
                                     :default-all-app-time? false
                                     :for-app-time :all-time}
                              [id version]]
                            {:node node :params {'?_start (util/->instant tt1)
                                                 '?_end (util/->instant eot)}}))))))

(t/deftest test-scan-col-types
  (with-open [node (node/start-node {})]
    (let [^IRaQuerySource ra-src (util/component node :xtdb.operator/ra-query-source)]
      (letfn [(->col-types [tx]
                (-> (.prepareRaQuery ra-src '[:scan {:table xt_docs} [id]])
                    (.bind (util/component node :xtdb/indexer) {:node node, :basis {:tx tx}})
                    (.columnTypes)))]

        (let [tx (-> (xt/submit-tx node [[:put :xt_docs {:id :doc}]])
                     (tu/then-await-tx* node))]
          (tu/finish-chunk! node)

          (t/is (= '{id :keyword}
                   (->col-types tx))))

        (let [tx (-> (xt/submit-tx node [[:put :xt_docs {:id "foo"}]])
                     (tu/then-await-tx* node))]

          (t/is (= '{id [:union #{:keyword :utf8}]}
                   (->col-types tx))))))))

(t/deftest can-create-temporal-min-max-range
  (let [μs-2018 (util/instant->micros (util/->instant #inst "2018"))
        μs-2019 (util/instant->micros (util/->instant #inst "2019"))]
    (letfn [(transpose [[mins maxs]]
              (->> (map vector mins maxs)
                   (zipmap [:sys-end :id :sys-start :row-id :app-time-start :app-time-end])
                   (into {} (remove (comp #{[Long/MIN_VALUE Long/MAX_VALUE]} val)))))]
      (t/is (= {:app-time-start [Long/MIN_VALUE μs-2019]
                :app-time-end [(inc μs-2019) Long/MAX_VALUE]}
               (transpose (scan/->temporal-min-max-range
                           nil nil nil
                           {"application_time_start" '(<= application_time_start #inst "2019")
                            "application_time_end" '(> application_time_end #inst "2019")}))))

      (t/is (= {:app-time-start [μs-2019 μs-2019]}
               (transpose (scan/->temporal-min-max-range
                           nil nil nil
                           {"application_time_start" '(= application_time_start #inst "2019")}))))

      (t/testing "symbol column name"
        (t/is (= {:app-time-start [μs-2019 μs-2019]}
                 (transpose (scan/->temporal-min-max-range
                             nil nil nil
                             {'application_time_start '(= application_time_start #inst "2019")})))))

      (t/testing "conjunction"
        (t/is (= {:app-time-start [Long/MIN_VALUE μs-2019]}
                 (transpose (scan/->temporal-min-max-range
                             nil nil nil
                             {"application_time_start" '(and (<= application_time_start #inst "2019")
                                                             (<= application_time_start #inst "2020"))})))))

      (t/testing "disjunction not supported"
        (t/is (= {}
                 (transpose (scan/->temporal-min-max-range
                             nil nil nil
                             {"application_time_start" '(or (= application_time_start #inst "2019")
                                                            (= application_time_start #inst "2020"))})))))

      (t/testing "ignores non-ts literals"
        (t/is (= {:app-time-start [μs-2019 μs-2019]}
                 (transpose (scan/->temporal-min-max-range
                             nil nil nil
                             {"application_time_start" '(and (= application_time_start #inst "2019")
                                                             (= application_time_end nil))})))))

      (t/testing "parameters"
        (t/is (= {:app-time-start [μs-2018 Long/MAX_VALUE]
                  :app-time-end [Long/MIN_VALUE (dec μs-2018)]
                  :sys-start [Long/MIN_VALUE μs-2019]
                  :sys-end [(inc μs-2019) Long/MAX_VALUE]}
                 (with-open [params (tu/open-params {'?sys-time (util/->instant #inst "2019")
                                                     '?app-time (util/->instant #inst "2018")})]
                   (transpose (scan/->temporal-min-max-range
                               params nil nil
                               {"system_time_start" '(>= ?sys-time system_time_start)
                                "system_time_end" '(< ?sys-time system_time_end)
                                "application_time_start" '(<= ?app-time application_time_start)
                                "application_time_end" '(> ?app-time application_time_end)})))))))))
