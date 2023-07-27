(ns xtdb.operator.scan-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.node :as node]
            [xtdb.operator :as op]
            [xtdb.operator.scan :as scan]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import xtdb.operator.IRaQuerySource))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-simple-scan
  (with-open [node (node/start-node {})]
    (xt/submit-tx node [[:put :xt_docs {:xt/id :foo, :col1 "foo1"}]
                        [:put :xt_docs {:xt/id :bar, :col1 "bar1", :col2 "bar2"}]
                        [:put :xt_docs {:xt/id :foo, :col2 "baz2"}]])

    (t/is (= #{{:xt/id :bar, :col1 "bar1", :col2 "bar2"}
               {:xt/id :foo, :col2 "baz2"}}
             (set (tu/query-ra '[:scan {:table xt_docs} [xt/id col1 col2]]
                               {:node node}))))))

(t/deftest test-simple-scan-with-namespaced-attributes
  (with-open [node (node/start-node {})]
    (xt/submit-tx node [[:put :xt_docs {:xt/id :foo, :the-ns/col1 "foo1"}]
                        [:put :xt_docs {:xt/id :bar, :the-ns/col1 "bar1", :col2 "bar2"}]
                        [:put :xt_docs {:xt/id :foo, :the-ns/col2 "baz2"}]])

    (t/is (= #{{:xt/id :bar, :the-ns/col1 "bar1", :col2 "bar2"}
               {:xt/id :foo}}
             (set (tu/query-ra '[:scan {:table xt_docs} [xt/id the-ns/col1 col2]]
                               {:node node}))))))

(t/deftest test-duplicates-in-scan-1
  (with-open [node (node/start-node {})]
    (xt/submit-tx node [[:put :xt_docs {:xt/id :foo}]])

    (t/is (= [{:xt/id :foo}]
             (tu/query-ra '[:scan {:table xt_docs} [xt/id xt/id]]
                          {:node node})))))

(t/deftest test-chunk-boundary
  (with-open [node (node/start-node {:xtdb/live-chunk {:rows-per-block 20, :rows-per-chunk 20}})]
    (->> (for [i (range 100)]
           [:put :xt_docs {:xt/id i}])
         (partition 10)
         (mapv #(xt/submit-tx node %)))

    (t/is (= (set (for [i (range 100)] {:xt/id i}))
             (set (tu/query-ra '[:scan {:table xt_docs} [xt/id]]
                               {:node node}))))))

;; TODO correctly project out valid-to/valid-from
(t/deftest test-past-point-point-queries
  (with-open [node (node/start-node {})]
    (let [tx1 (xt/submit-tx node [[:put :xt_docs {:xt/id :doc1 :v 1} {:for-valid-time [:from #inst "2015"]}]
                                  [:put :xt_docs {:xt/id :doc2 :v 1} {:for-valid-time [:from #inst "2015"]}]
                                  [:put :xt_docs {:xt/id :doc3 :v 1} {:for-valid-time [:from #inst "2018"]}]])

          tx2 (xt/submit-tx node [[:put :xt_docs {:xt/id :doc1 :v 2} {:for-valid-time [:from #inst "2020"]}]
                                  [:put :xt_docs {:xt/id :doc2 :v 2} {:for-valid-time [:from #inst "2100"]}]
                                  [:delete :xt_docs :doc3]])]

      ;; valid-time
      (t/is (= #{{:v 1, :xt/id :doc1} {:v 1, :xt/id :doc2}}
               (set (tu/query-ra '[:scan
                                   {:table xt_docs, :for-valid-time [:at #inst "2017"], :for-system-time nil}
                                   [xt/id v]]
                                 {:node node}))))

      (t/is (= #{{:v 1, :xt/id :doc2} {:v 2, :xt/id :doc1}}
               (set (tu/query-ra '[:scan
                                   {:table xt_docs, :for-valid-time [:at :now], :for-system-time nil}
                                   [xt/id v]]
                                 {:node node}))))
      ;; system-time
      (t/is (= #{{:v 1, :xt/id :doc1} {:v 1, :xt/id :doc2} {:v 1, :xt/id :doc3}}
               (set (tu/query-ra '[:scan
                                   {:table xt_docs, :for-valid-time [:at :now], :for-system-time nil}
                                   [xt/id v]]
                                 {:node node :basis {:tx tx1}}))))

      (t/is (= #{{:v 1, :xt/id :doc1} {:v 1, :xt/id :doc2}}
               (set (tu/query-ra '[:scan
                                   {:table xt_docs, :for-valid-time [:at #inst "2017"], :for-system-time nil}
                                   [xt/id v]]
                                 {:node node :basis {:tx tx1}}))))

      (t/is (= #{{:v 2, :xt/id :doc1} {:v 1, :xt/id :doc2}}
               (set (tu/query-ra '[:scan
                                   {:table xt_docs, :for-valid-time [:at :now], :for-system-time nil}
                                   [xt/id v]]
                                 {:node node :basis {:tx tx2}}))))

      (t/is (= #{{:v 2, :xt/id :doc1} {:v 2, :xt/id :doc2}}
               (set (tu/query-ra '[:scan
                                   {:table xt_docs, :for-valid-time [:at #inst "2100"], :for-system-time nil}
                                   [xt/id v]]
                                 {:node node :basis {:tx tx2}})))))))

;; FIXME to remove once point/point queries support valid-to/valid-rom
(t/deftest test-no-with-tries-for-valid-to-and-valid-from
  (with-open [node (node/start-node {})]
    (xt/submit-tx node [[:put :xt_docs {:xt/id :doc1 :v 1} {:for-valid-time [:from #inst "2015"]}]])
    (xt/submit-tx node [[:put :xt_docs {:xt/id :doc1 :v 2} {:for-valid-time [:from #inst "2023"]}]])

    (with-redefs [scan/->4r-cursor (fn [& _args] (throw (ex-info "Should throw!!!" {})))]
      (t/is (= #{{:v 1, :xt/id :doc1 :xt/valid-to #time/zoned-date-time "2023-01-01T00:00Z[UTC]"}}
               (set (tu/query-ra '[:scan
                                   {:table xt_docs, :for-valid-time [:at #inst "2017"] , :for-system-time nil}
                                   [xt/id v xt/valid-to]]
                                 {:node node})))))))


(t/deftest test-scanning-temporal-cols
  (with-open [node (node/start-node {})]
    (xt/submit-tx node [[:put :xt_docs {:xt/id :doc}
                         {:for-valid-time [:in #inst "2021" #inst "3000"]}]])

    (let [res (first (tu/query-ra '[:scan {:table xt_docs}
                                    [xt/id
                                     xt/valid-from xt/valid-to
                                     xt/system-from xt/system-to]]
                                  {:node node}))]
      (t/is (= #{:xt/id :xt/valid-from :xt/valid-to :xt/system-to :xt/system-from}
               (-> res keys set)))

      (t/is (= {:xt/id :doc, :xt/valid-from (util/->zdt #inst "2021"), :xt/valid-to (util/->zdt #inst "3000")}
               (dissoc res :xt/system-from :xt/system-to))))

    (t/is (= {:xt/id :doc, :app-time-start (util/->zdt #inst "2021"), :app-time-end (util/->zdt #inst "3000")}
             (-> (first (tu/query-ra '[:project [xt/id
                                                 {app-time-start xt/valid-from}
                                                 {app-time-end xt/valid-to}]
                                       [:scan {:table xt_docs}
                                        [xt/id xt/valid-from xt/valid-to]]]
                                     {:node node}))
                 (dissoc :xt/system-from :xt/system-to))))))

(t/deftest test-only-scanning-temporal-cols-45
  (with-open [node (node/start-node {})]
    (let [{tt :system-time} (xt/submit-tx node [[:put :xt_docs {:xt/id :doc}]])]

      (t/is (= #{{:xt/valid-from (util/->zdt tt)
                  :xt/valid-to (util/->zdt util/end-of-time)
                  :xt/system-from (util/->zdt tt),
                  :xt/system-to (util/->zdt util/end-of-time)}}
               (set (tu/query-ra '[:scan {:table xt_docs}
                                   [xt/valid-from xt/valid-to
                                    xt/system-from xt/system-to]]
                                 {:node node})))))))

(t/deftest test-aligns-temporal-columns-correctly-363
  (with-open [node (node/start-node {})]
    (xt/submit-tx node [[:put :foo {:xt/id :my-doc, :last_updated "tx1"}]] {:system-time #inst "3000"})

    (xt/submit-tx node [[:put :foo {:xt/id :my-doc, :last_updated "tx2"}]] {:system-time #inst "3001"})

    #_(tu/finish-chunk! node)

    (t/is (= [{:xt/system-from (util/->zdt #inst "3000")
               :xt/system-to (util/->zdt #inst "3001")
               :last_updated "tx1"}
              {:xt/system-from (util/->zdt #inst "3001")
               :xt/system-to (util/->zdt util/end-of-time)
               :last_updated "tx1"}
              {:xt/system-from (util/->zdt #inst "3001")
               :xt/system-to (util/->zdt util/end-of-time)
               :last_updated "tx2"}]
             (tu/query-ra '[:scan {:table foo, :for-system-time :all-time}
                            [{xt/system-from (< xt/system-from #time/zoned-date-time "3002-01-01T00:00Z")}
                             {xt/system-to (> xt/system-to #time/zoned-date-time "2999-01-01T00:00Z")}
                             last_updated]]
                          {:node node :default-all-valid-time? true})))))

(t/deftest test-for-valid-time-in-params
  (let [tt1 (util/->zdt #inst "2020-01-01")
        tt2 (util/->zdt #inst "2020-01-02")
        eot (util/->zdt util/end-of-time)]
    (with-open [node (node/start-node {})]
      (xt/submit-tx node [[:put :foo {:xt/id 1, :version "version 1" :last_updated "tx1"}
                           {:app-time-start tt1 :app-time-end eot}]])

      (xt/submit-tx node [[:put :foo {:xt/id 2, :version "version 2" :last_updated "tx2"}
                           {:app-time-start tt2 :app-time-end eot}]])
      (t/is (= #{{:xt/id 1, :version "version 1"} {:xt/id 2, :version "version 2"}}
               (set (tu/query-ra '[:scan {:table foo,
                                          :for-valid-time [:between ?_start ?_end]}
                                   [xt/id version]]
                                 {:node node :params {'?_start (util/->instant tt1)
                                                      '?_end (util/->instant eot)}}))))
      (t/is (= #{{:xt/id 1, :version "version 1"} {:xt/id 2, :version "version 2"}}
               (set
                (tu/query-ra '[:scan {:table foo,
                                      :for-valid-time :all-time}
                               [xt/id version]]
                             {:node node :params {'?_start (util/->instant tt1)
                                                  '?_end (util/->instant eot)}})))))))

(t/deftest test-scan-col-types
  (with-open [node (node/start-node {})]
    (let [^IRaQuerySource ra-src (util/component node :xtdb.operator/ra-query-source)]
      (letfn [(->col-types [tx]
                (-> (.prepareRaQuery ra-src '[:scan {:table xt_docs} [xt/id]])
                    (.bind (util/component node :xtdb/indexer) {:node node, :basis {:tx tx}})
                    (.columnTypes)))]

        (let [tx (-> (xt/submit-tx node [[:put :xt_docs {:xt/id :doc}]])
                     (tu/then-await-tx node))]
          (tu/finish-chunk! node)

          (t/is (= '{xt/id :keyword}
                   (->col-types tx))))

        (let [tx (-> (xt/submit-tx node [[:put :xt_docs {:xt/id "foo"}]])
                     (tu/then-await-tx node))]

          (t/is (= '{xt/id [:union #{:keyword :utf8}]}
                   (->col-types tx))))))))

(t/deftest can-create-temporal-min-max-range
  (let [μs-2018 (util/instant->micros (util/->instant #inst "2018"))
        μs-2019 (util/instant->micros (util/->instant #inst "2019"))]
    (letfn [(transpose [[mins maxs]]
              (->> (map vector mins maxs)
                   (zipmap [:sys-end :xt/id :sys-start :row-id :app-time-start :app-time-end])
                   (into {} (remove (comp #{[Long/MIN_VALUE Long/MAX_VALUE]} val)))))]
      (t/is (= {:app-time-start [Long/MIN_VALUE μs-2019]
                :app-time-end [(inc μs-2019) Long/MAX_VALUE]}
               (transpose (scan/->temporal-min-max-range
                           nil nil nil
                           {'xt/valid-from '(<= xt/valid-from #inst "2019")
                            'xt/valid-to '(> xt/valid-to #inst "2019")}))))

      (t/is (= {:app-time-start [μs-2019 μs-2019]}
               (transpose (scan/->temporal-min-max-range
                           nil nil nil
                           {'xt/valid-from '(= xt/valid-from #inst "2019")}))))

      (t/testing "symbol column name"
        (t/is (= {:app-time-start [μs-2019 μs-2019]}
                 (transpose (scan/->temporal-min-max-range
                             nil nil nil
                             {'xt/valid-from '(= xt/valid-from #inst "2019")})))))

      (t/testing "conjunction"
        (t/is (= {:app-time-start [Long/MIN_VALUE μs-2019]}
                 (transpose (scan/->temporal-min-max-range
                             nil nil nil
                             {'xt/valid-from '(and (<= xt/valid-from #inst "2019")
                                                   (<= xt/valid-from #inst "2020"))})))))

      (t/testing "disjunction not supported"
        (t/is (= {}
                 (transpose (scan/->temporal-min-max-range
                             nil nil nil
                             {'xt/valid-from '(or (= xt/valid-from #inst "2019")
                                                  (= xt/valid-from #inst "2020"))})))))

      (t/testing "ignores non-ts literals"
        (t/is (= {:app-time-start [μs-2019 μs-2019]}
                 (transpose (scan/->temporal-min-max-range
                             nil nil nil
                             {'xt/valid-from '(and (= xt/valid-from #inst "2019")
                                                   (= xt/valid-from nil))})))))

      (t/testing "parameters"
        (t/is (= {:app-time-start [μs-2018 Long/MAX_VALUE]
                  :app-time-end [Long/MIN_VALUE (dec μs-2018)]
                  :sys-start [Long/MIN_VALUE μs-2019]
                  :sys-end [(inc μs-2019) Long/MAX_VALUE]}
                 (with-open [params (tu/open-params {'?system-time (util/->instant #inst "2019")
                                                     '?app-time (util/->instant #inst "2018")})]
                   (transpose (scan/->temporal-min-max-range
                               params nil nil
                               {'xt/system-from '(>= ?system-time xt/system-from)
                                'xt/system-to '(< ?system-time xt/system-to)
                                'xt/valid-from '(<= ?app-time xt/valid-from)
                                'xt/valid-to '(> ?app-time xt/valid-to)})))))))))

(t/deftest test-content-pred
  (with-open [node (node/start-node {})]
    (xt/submit-tx node [[:put :xt_docs {:xt/id :ivan, :first-name "Ivan", :last-name "Ivanov"}]
                        [:put :xt_docs {:xt/id :petr, :first-name "Petr", :last-name "Petrov"}]])
    (t/is (= [{:first-name "Ivan", :xt/id :ivan}]
             (tu/query-ra '[:scan
                            {:table xt_docs,  :for-valid-time nil, :for-system-time nil}
                            [{first-name (= first-name "Ivan")} xt/id]]
                          {:node node})))))

(t/deftest test-absent-columns
  (with-open [node (node/start-node {})]
    (xt/submit-tx node [[:put :xt_docs {:xt/id :foo, :col1 "foo1"}]
                        [:put :xt_docs {:xt/id :bar, :col1 "bar1", :col2 "bar2"}]])

    ;; column not existent in all docs
    (t/is (= [{:col2 "bar2", :xt/id :bar}]
             (tu/query-ra '[:scan {:table xt_docs} [xt/id {col2 (= col2 "bar2")}]]
                          {:node node})))

    ;; column not existent at all
    (t/is (= []
             (tu/query-ra
              '[:scan {:table xt_docs} [xt/id {col-x (= col-x "toto")}]]
              {:node node})))))
