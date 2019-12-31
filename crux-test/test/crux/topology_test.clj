(ns crux.topology-test
  (:require [clojure.test :as t]
            [crux.topology :as topo]))

(def node-topology
  {:crux/node {:start-fn (fn [deps args]
                           ;; deps should contain the indexer
                           {:node {:deps deps :args args}})
               :deps #{:crux.node/indexer}}

   :crux.node/indexer {:start-fn (fn [deps args]
                                   {:node-indexer {:deps deps :args args}})
                       :deps #{}}})

(def metrics-topology
  {:crux.metrics/pull-server {:start-fn (fn [deps args]
                                          {:crux.metrics/pull-server {:deps deps :args args}})
                              :deps #{:crux.metrics/indexer}}

   :crux.metrics/indexer {:start-fn (fn [deps args]
                                      {:metrics-indexer {:deps deps :args args}})

                          :deps #{}
                          :wraps :crux.node/indexer}})

(def yadecorator-topology
  {:crux.yadecorator/indexer {:start-fn (fn [deps args]
                                          {:yadecorator {:deps deps, :args args}})
                              :deps #{}
                              :wraps :crux.node/indexer}})

(def overrides-topology
  {:crux.node/indexer {:start-fn (fn [deps args]
                                   {:over-top {:deps deps :args args}})
                       :deps #{}}})

(def hairy-topologies
  [{:n1 {}}
   {:n1-w1 {:wraps :n1}}
   {:n1-w2 {:wraps :n1}}
   {:n1-dep {:deps #{:n1}}}
   {:n2 {:deps #{:n1-w1}}}
   {:n3 {:deps #{:n1}}}
   {:n4 {:deps #{:n3}}}
   {:n5 {:wraps :n3}}])

(t/deftest test-resolves-modules
  (t/testing "simple wrappers"
    (t/is (= {:crux/node {:deps #{:crux.node/indexer}}
              :crux.node/indexer {:deps #{}, :wrappers [:crux.metrics/indexer]}
              :crux.metrics/indexer {:deps #{}, :wraps :crux.node/indexer}
              :crux.metrics/pull-server {:deps #{:crux.metrics/indexer}}}
             (#'topo/resolve-modules [node-topology metrics-topology]))))

  (t/testing "double wrappers"
    (t/is (= {:crux/node {:deps #{:crux.node/indexer}}
              :crux.node/indexer {:deps #{}
                                  :wrappers [:crux.metrics/indexer :crux.yadecorator/indexer]}
              :crux.metrics/indexer {:deps #{}, :wraps :crux.node/indexer}
              :crux.metrics/pull-server {:deps #{:crux.metrics/indexer}}
              :crux.yadecorator/indexer {:deps #{}, :wraps :crux.node/indexer}}
             (#'topo/resolve-modules [node-topology metrics-topology yadecorator-topology]))))

  (t/testing "overrides"
    (t/is (= {:crux/node {:deps #{:crux.node/indexer}}
              :crux.node/indexer {:deps #{}}}
             (#'topo/resolve-modules [node-topology metrics-topology overrides-topology])))

    (t/is (= {:crux/node {:deps #{:crux.node/indexer}}
              :crux.node/indexer {:deps #{}}}
             (#'topo/resolve-modules [node-topology metrics-topology yadecorator-topology overrides-topology]))))

  (t/testing "hairy topologies"
    (t/is (= {:n1 {:wrappers [:n1-w1 :n1-w2]},
              :n1-w1 {:wraps :n1},
              :n1-w2 {:wraps :n1},
              :n1-dep {:deps #{:n1}},
              :n2 {:deps #{:n1-w1}},
              :n3 {:deps #{:n1}, :wrappers [:n5]},
              :n4 {:deps #{:n3}},
              :n5 {:wraps :n3}}
             (#'topo/resolve-modules hairy-topologies)))

    (t/is (= {:n1 {:deps #{}},
              :n1-dep {:deps #{:n1}},
              :n3 {:deps #{:n1}, :wrappers [:n5]},
              :n4 {:deps #{:n3}},
              :n5 {:wraps :n3}}
             (#'topo/resolve-modules (conj hairy-topologies
                                           ;; override
                                           {:n1 {:deps #{}}}))))))

(t/deftest testing-start-graph
  (t/testing "simple"
    (t/is (= {::topo/system #{:crux/node :crux.node/kv-store :crux.node/indexer}
              :crux/node #{:crux.node/indexer :crux.node/kv-store}
              :crux.node/indexer #{:crux.node/kv-store}}
             (-> (#'topo/module-start-graph {:crux/node {:deps #{:crux.node/indexer
                                                                 :crux.node/kv-store}}
                                             :crux.node/indexer {:deps #{:crux.node/kv-store}}
                                             :crux.node/kv-store {:deps #{}}})
                 :dependencies))))

  (t/testing "wrapping"
    (t/is (= {::topo/system #{:crux/node :crux.node/indexer :crux.yadecorator/indexer
                              :crux.metrics/pull-server :crux.metrics/indexer}
              :crux/node #{:crux.yadecorator/indexer}
              :crux.metrics/pull-server #{:crux.metrics/indexer}
              :crux.metrics/indexer #{:crux.node/indexer}
              :crux.yadecorator/indexer #{:crux.metrics/indexer}}

             (-> {:crux/node {:deps #{:crux.node/indexer}}
                  :crux.node/indexer {:deps #{}
                                      :wrappers [:crux.metrics/indexer :crux.yadecorator/indexer]}
                  :crux.metrics/pull-server {:deps #{:crux.metrics/indexer}}
                  :crux.metrics/indexer {:deps #{}, :wraps :crux.node/indexer}
                  :crux.yadecorator/indexer {:deps #{}, :wraps :crux.node/indexer}}
                 (#'topo/module-start-graph)
                 :dependencies))))

  (t/testing "hairy topologies"
    (t/is (= {::topo/system #{:n1 :n1-w1 :n1-w2 :n1-dep :n2 :n3 :n4 :n5}
              :n1-w1 #{:n1},
              :n1-w2 #{:n1-w1},
              :n1-dep #{:n1-w2},
              :n2 #{:n1-w1},
              :n3 #{:n1-w2},
              :n4 #{:n5},
              :n5 #{:n3}}

             (-> (#'topo/resolve-modules hairy-topologies)
                 (#'topo/module-start-graph)
                 :dependencies)))))
