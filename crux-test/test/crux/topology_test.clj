(ns crux.topology-test
  (:require [crux.topology :as topo]
            [clojure.spec.alpha :as s]
            [clojure.test :as t]))

(t/deftest test-properties-to-topology
  (let [t (topo/options->topology {:crux.node/topology ['crux.jdbc/topology]
                                   :crux.node/kv-store :crux.kv.rocksdb/kv})]

    (t/is (= (-> @(requiring-resolve 'crux.jdbc/topology) :crux.node/tx-log)
             (-> t :crux.node/tx-log)))
    (t/is (= (s/conform ::topo/component crux.kv.rocksdb/kv)
             (-> t :crux.node/kv-store))))

  (t/testing "override module in topology"
    (let [t (topo/options->topology {:crux.node/topology ['crux.jdbc/topology]
                                     :crux.node/kv-store 'crux.kv.memdb/kv})]

      (t/is (= (-> crux.jdbc/topology :crux.node/tx-log)
               (-> t :crux.node/tx-log)))
      (t/is (= (s/conform ::topo/component @(requiring-resolve 'crux.kv.memdb/kv))
               (-> t :crux.node/kv-store))))))

(t/deftest test-option-parsing
  (t/is (= {:foo 2, :bar false, :baz 5, :quux 5}
           (topo/parse-opts {:foo {:crux.config/type :crux.config/int
                                   :doc "An argument"
                                   :default 3}
                             :bar {:crux.config/type :crux.config/boolean
                                   :doc "An argument"
                                   :default true}
                             :baz {:crux.config/type :crux.config/nat-int
                                   :doc "An argument"
                                   :default 5}
                             :quux {:crux.config/type :crux.config/nat-int
                                    :doc "An argument"
                                    :default 6}}

                            {:foo 2
                             :bar false
                             :quux "5"}))))

(t/deftest test-components-shutdown-in-order
  (let [started (atom [])
        stopped (atom [])
        [topo close-fn] (topo/start-topology
                         {:crux.node/topology
                          {:a {:deps [:b]
                               :start-fn (fn [{:keys [b]} _]
                                           (assert b)
                                           (swap! started conj :a)
                                           (reify java.io.Closeable
                                             (close [_]
                                               (swap! stopped conj :a))))}
                           :b {:start-fn (fn [_ _]
                                           (swap! started conj :b)
                                           (reify java.io.Closeable
                                             (close [_]
                                               (swap! stopped conj :b))))}}})]
    (t/is (= [:b :a] @started))
    (close-fn)
    (t/is (= [:a :b] @stopped)))

  (t/testing "with before"
    (let [started (atom [])
          stopped (atom [])
          [topo close-fn] (topo/start-topology
                           {:crux.node/topology
                            {:a {:start-fn (fn [_ _]
                                             (swap! started conj :a)
                                             (reify java.io.Closeable
                                               (close [_]
                                                 (swap! stopped conj :a))))}
                             :b {:before #{:a}
                                 :start-fn (fn [_ _]
                                             (swap! started conj :b)
                                             (reify java.io.Closeable
                                               (close [_]
                                                 (swap! stopped conj :b))))}}})]
      (t/is (= [:b :a] @started))
      (close-fn)
      (t/is (= [:a :b] @stopped))))

  (t/testing "with exception"
    (let [started (atom [])
          stopped (atom [])]
      (try
        (let [[topo close-fn] (topo/start-topology
                               {:crux.node/topology
                                {:a {:deps [:b]
                                     :start-fn (fn [_ _]
                                                 (throw (Exception.)))}
                                 :b {:start-fn (fn [_ _]
                                                 (swap! started conj :b)
                                                 (reify java.io.Closeable
                                                   (close [_]
                                                     (swap! stopped conj :b))))}}})]
          (t/is false))
        (catch Exception e))
      (t/is (= [:b] @started))
      (t/is (= [:b] @stopped)))))
