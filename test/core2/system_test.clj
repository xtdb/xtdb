(ns core2.system-test
  (:require [core2.system :as sys]
            [clojure.test :as t]
            [juxt.clojars-mirrors.dependency.v1v0v0.com.stuartsierra.dependency :as dep]))

(t/deftest simplest-prep
  (let [{:keys [start-fn]} (-> (sys/prep-system {:foo (fn [opts]
                                                        {:opts opts})})
                               (get [:foo]))]
    (t/is (= {:opts {:a 1, :b 2}}
             (start-fn {:a 1, :b 2})))))

(t/deftest test-option-parsing
  (t/is (= {:foo 2, :bar false, :baz 5, :quux 5}
           (sys/parse-opts {:foo [2], :bar [false], :quux ["5"]}
                           {:foo {:spec ::sys/int, :default 3}
                            :bar {:spec ::sys/boolean, :default true}
                            :baz {:spec ::sys/nat-int, :default 5}
                            :quux {:spec ::sys/nat-int, :default 6}})))

  (t/is (thrown? IllegalArgumentException
                 (sys/parse-opts {}
                                 {:str {:spec ::sys/string
                                        :required? true}})))

  (t/is (thrown? IllegalArgumentException
                 (sys/parse-opts {:str 10}
                                 {:str {:spec ::sys/string
                                        :required? true}})))

  (t/is (= {:with-default 42, :str nil}
           (-> (sys/prep-system {:foo (-> (fn [opts])
                                          (vary-meta assoc
                                                     ::sys/args {:str {:spec ::sys/string}
                                                                 :with-default {:default 42
                                                                                :required? true
                                                                                :spec ::sys/nat-int}}))})
               (get-in [[:foo] :opts])))))

(t/deftest test-merge-opts
  (t/is (= {:foo 1, :bar 2}
           (-> (sys/prep-system [{:module (fn [opts])}
                                 {:module {:foo 0, :bar 2}}
                                 {:module {:foo 1}}])
               (get-in [[:module] :opts])))))

(defn- ->override-foo {::sys/args {:arg {:default :foo-default, :spec ::sys/keyword}}} [opts] [:foo opts])
(defn- ->override-foo2 {::sys/args {:arg {:default :foo2-default, :spec ::sys/keyword}}} [opts] [:foo2 opts])
(defn- ->override-bar {::sys/deps {::override-foo {}}} [opts] [:bar opts])
(defn- ->override-bar2 {::sys/deps {::override-foo {:arg :bar2-override}}} [opts] [:bar2 opts])
(defn- ->override-bar3 {::sys/deps {::override-foo {:core2/module `->override-foo2, :arg :bar3-override}}} [opts] [:bar3 opts])

(t/deftest test-overriding
  (t/is (= {:arg :foo-default}
           (-> (sys/prep-system {::override-foo {}})
               (get-in [[::override-foo] :opts])))
        "defaulting arg, basic case")

  (t/is (= {:arg :user-override}
           (-> (sys/prep-system {::override-foo {:arg :user-override}})
               (get-in [[::override-foo] :opts])))
        "user can override arg at the top level")

  (t/is (= {:arg :user-override}
           (-> (sys/prep-system {::override-bar {::override-foo {:arg :user-override}}})
               (get-in [[::override-bar ::override-foo] :opts])))
        "user can override arg in a nested component")

  (t/are [msg expected opts] (t/is (= expected
                                      (into {} (-> (sys/prep-system opts)
                                                   (sys/start-system))))
                                   msg)

    "user can override implementation"
    {::override-foo [:foo2 {:arg :foo2-default}]}
    {::override-foo {:core2/module `->override-foo2}}

    "user can override implementation and arg"
    {::override-foo [:foo2 {:arg :user-override}]}
    {::override-foo {:core2/module `->override-foo2, :arg :user-override}}

    "bar - defaulting nested component"
    {::override-bar [:bar {::override-foo [:foo {:arg :foo-default}]}]}
    {::override-bar {}}

    "bar2 - module can override nested config"
    {::override-bar [:bar2 {::override-foo [:foo {:arg :bar2-override}]}]}
    {::override-bar `->override-bar2}

    "bar2 - user can override nested config"
    {::override-bar [:bar2 {::override-foo [:foo {:arg :user-override}]}]}
    {::override-bar {:core2/module `->override-bar2, ::override-foo {:arg :user-override}}}

    "bar2 - user can override nested implementation"
    {::override-bar [:bar2 {::override-foo [:foo2 {:arg :user-override}]}]}
    {::override-bar {:core2/module `->override-bar2,
                     ::override-foo {:core2/module ->override-foo2, :arg :user-override}}}

    "bar3 - module can override nested implementation"
    {::override-bar [:bar3 {::override-foo [:foo2 {:arg :bar3-override}]}]}
    {::override-bar `->override-bar3}

    "bar3 - user can override nested config"
    {::override-bar [:bar3 {::override-foo [:foo2 {:arg :user-override}]}]}
    {::override-bar {:core2/module `->override-bar3, ::override-foo {:arg :user-override}}}

    "bar3 - user can override nested config, but with strings"
    {::override-bar [:bar3 {::override-foo [:foo2 {:arg :user-override}]}]}
    {"core2.system-test/override-bar" {"core2/module" "core2.system-test/->override-bar3"
                                       "core2.system-test/override-foo" {"arg" "user-override"}}}

    "bar3 - user can override nested implementation"
    {::override-bar [:bar3 {::override-foo [:foo {:arg :user-override}]}]}
    {::override-bar {:core2/module `->override-bar3,
                     ::override-foo {:core2/module `->override-foo, :arg :user-override}}}))

(t/deftest test-dep-order
  (let [g (-> (sys/prep-system {::after (-> (fn [opts])
                                            (vary-meta assoc ::sys/deps {::before (fn [opts])}))})
              (sys/->dep-graph))]
    (t/is (dep/depends? g [::after] [::after ::before]))
    (t/is (= [[::after ::before] [::after] ::sys/system]
             (dep/topo-sort g))))

  (let [g (-> (sys/prep-system {::before (fn [opts])
                                ::after (-> (fn [opts])
                                            (vary-meta assoc ::sys/deps {:before ::before}))})
              (sys/->dep-graph))]
    (t/is (dep/depends? g [::after] [::before]))
    (t/is (= [[::before]
              [::after :before] ; virtual component for the reference
              [::after]
              ::sys/system]
             (dep/topo-sort g)))))

(t/deftest test-before
  (let [g (-> (sys/prep-system {::before (-> (fn [opts])
                                             (vary-meta assoc ::sys/before #{[::after]}))
                                ::after (fn [opts])})
              (sys/->dep-graph))]
    (t/is (dep/depends? g [::after] [::before]))
    (t/is (= [[::before] [::after] ::sys/system] (dep/topo-sort g)))))

(t/deftest test-components-shutdown-in-order
  (let [started (atom [])
        stopped (atom [])
        sys (sys/start-system {[:a] {:refs {:b [:b]}
                                     :start-fn (fn [{:keys [b]}]
                                                 (assert b)
                                                 (swap! started conj :a)
                                                 (reify java.io.Closeable
                                                   (close [_]
                                                     (swap! stopped conj :a))))}
                               [:b] {:start-fn (fn [_]
                                                 (swap! started conj :b)
                                                 (reify java.io.Closeable
                                                   (close [_]
                                                     (swap! stopped conj :b))))}})]
    (t/is (= [:b :a] @started))
    (.close sys)
    (t/is (= [:a :b] @stopped)))

  (t/testing "with exception"
    (let [started (atom [])
          stopped (atom [])]
      (t/is (thrown-with-msg? Exception #"Error starting system"
                              (sys/start-system {[:a] {:refs {:b [:b]}
                                                       :start-fn (fn [_]
                                                                   (throw (UnsupportedOperationException.)))}
                                                 [:b] {:start-fn (fn [_]
                                                                   (swap! started conj :b)
                                                                   (reify java.io.Closeable
                                                                     (close [_]
                                                                       (swap! stopped conj :b))))}})))
      (t/is (= [:b] @started))
      (t/is (= [:b] @stopped)))))
