(ns core2.operator-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.expression :as expr]
            [core2.metadata :as meta]
            [core2.test-util :as tu]
            [core2.util :as util])
  (:import core2.metadata.IMetadataManager
           core2.operator.IOperatorFactory
           java.util.function.Consumer
           org.apache.arrow.vector.util.Text))

(t/deftest test-find-gt-ivan
  (let [node-dir (doto (util/->path "target/test-find-gt-ivan")
                   util/delete-dir)]
    (with-open [node (tu/->local-node {:node-dir node-dir, :max-rows-per-chunk 10, :max-rows-per-block 2})
                tx-producer (tu/->local-tx-producer {:node-dir node-dir})]
      (let [^IMetadataManager metadata-mgr (:core2/metadata-manager @(:!system node))]

        @(-> (c2/submit-tx tx-producer [{:op :put, :doc {:name "Håkan", :_id 0}}])
             (tu/then-await-tx node))

        (tu/finish-chunk node)

        @(c2/submit-tx tx-producer [{:op :put, :doc {:name "James", :_id 1}}
                                 {:op :put, :doc {:name "Dan", :_id 2}}])

        @(-> (c2/submit-tx tx-producer [{:op :put, :doc {:name "Jon", :_id 3}}])
             (tu/then-await-tx node))

        (tu/finish-chunk node)

        (let [^IOperatorFactory op-factory (:op-factory node)
              metadata-pred (expr/->metadata-selector (expr/form->expr '(> name "Ivan")))]
          (letfn [(query-ivan [watermark]
                    (let [!results (atom [])]
                      (with-open [chunk-scanner (.scan op-factory watermark
                                                       ["name"]
                                                       metadata-pred
                                                       {"name" (expr/->expression-vector-selector (expr/form->expr '(> name "Ivan")))}
                                                       nil
                                                       nil)]
                        (while (.tryAdvance chunk-scanner
                                            (reify Consumer
                                              (accept [_ root]
                                                (swap! !results into (tu/root->rows root)))))))
                      (set @!results)))]
            (with-open [watermark (c2/open-watermark node)]
              (t/is (= #{0 1} (.knownChunks metadata-mgr)))
              (t/is (= [1] (meta/matching-chunks metadata-mgr watermark metadata-pred))
                    "only needs to scan chunk 1")

              @(-> (c2/submit-tx tx-producer [{:op :put, :doc {:name "Jeremy", :_id 4}}])
                   (tu/then-await-tx node))

              (t/is (= #{[(Text. "James")]
                         [(Text. "Jon")]}
                       (query-ivan watermark))))

            (with-open [watermark (c2/open-watermark node)]
              (t/is (= #{[(Text. "James")]
                         [(Text. "Jon")]
                         [(Text. "Jeremy")]}
                       (query-ivan watermark))))))))))

(t/deftest test-fixpoint-operator
  (let [node-dir (doto (util/->path "target/test-fixpoint-operator")
                   util/delete-dir)]
    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (t/testing "factorial"
        (with-open [watermark (c2/open-watermark node)
                    fixpoint-cursor (c2/open-q node watermark '[:fixpoint Fact
                                                                [:union
                                                                 [:table [{:a 0 :b 1}]]
                                                                 [:select
                                                                  (<= a 8)
                                                                  [:project
                                                                   [{a (+ a 1)}
                                                                    {b (* (+ a 1) b)}]
                                                                   Fact]]]])]
          (t/is (= [[{:a 0, :b 1}
                     {:a 1, :b 1}
                     {:a 2, :b 2}
                     {:a 3, :b 6}
                     {:a 4, :b 24}
                     {:a 5, :b 120}
                     {:a 6, :b 720}
                     {:a 7, :b 5040}
                     {:a 8, :b 40320}]] (tu/<-cursor fixpoint-cursor)))))

      (t/testing "transitive closure"
        (with-open [watermark (c2/open-watermark node)
                    fixpoint-cursor (c2/open-q node watermark '[:fixpoint Path
                                                                [:union
                                                                 [:table [{:x "a" :y "b"}
                                                                          {:x "b" :y "c"}
                                                                          {:x "c" :y "d"}
                                                                          {:x "d" :y "a"}]]
                                                                 [:project [x y]
                                                                  [:join {z z}
                                                                   [:rename {y z} Path]
                                                                   [:rename {x z} Path]]]]])]

          (t/is (= [[{:x (Text. "a"), :y (Text. "b")}
                     {:x (Text. "b"), :y (Text. "c")}
                     {:x (Text. "c"), :y (Text. "d")}
                     {:x (Text. "d"), :y (Text. "a")}
                     {:x (Text. "d"), :y (Text. "b")}
                     {:x (Text. "a"), :y (Text. "c")}
                     {:x (Text. "b"), :y (Text. "d")}
                     {:x (Text. "c"), :y (Text. "a")}
                     {:x (Text. "c"), :y (Text. "b")}
                     {:x (Text. "d"), :y (Text. "c")}
                     {:x (Text. "a"), :y (Text. "d")}
                     {:x (Text. "b"), :y (Text. "a")}
                     {:x (Text. "b"), :y (Text. "b")}
                     {:x (Text. "c"), :y (Text. "c")}
                     {:x (Text. "d"), :y (Text. "d")}
                     {:x (Text. "a"), :y (Text. "a")}]]
                   (tu/<-cursor fixpoint-cursor))))))))

(t/deftest test-assignment-operator
  (let [node-dir (doto (util/->path "target/test-assignment-operator")
                   util/delete-dir)]
    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (with-open [watermark (c2/open-watermark node)
                  assignment-cursor (c2/open-q node watermark '[:assign [X [:table [{:a 1}]]
                                                                         Y [:table [{:b 1}]]]
                                                                [:join {a b} X Y]])]
        (t/is (= [[{:a 1 :b 1}]] (tu/<-cursor assignment-cursor))))

      (t/testing "can see earlier assignments"
        (with-open [watermark (c2/open-watermark node)
                    assignment-cursor (c2/open-q node watermark '[:assign [X [:table [{:a 1}]]
                                                                           Y [:join {a b} X [:table [{:b 1}]]]
                                                                           X Y]
                                                                  X])]
          (t/is (= [[{:a 1 :b 1}]] (tu/<-cursor assignment-cursor))))))))
