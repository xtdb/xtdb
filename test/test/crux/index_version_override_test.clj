(ns crux.index-version-override-test
  (:require [clojure.test :as t]
            [crux.api :as xt]
            [crux.codec :as c]
            [crux.fixtures :as fix])
  (:import crux.api.IndexVersionOutOfSyncException))

(t/deftest test-index-version-override
  (fix/with-tmp-dir "db-dir" [db-dir]
    (let [index-version c/index-version
          inc-index-version (inc index-version)
          topo {:xt/index-store {:kv-store {:xt/module 'xtdb.rocksdb/->kv-store
                                            :db-dir db-dir}}}
          with-flag (fn [topo flag]
                      (-> topo
                          (assoc-in [:xt/index-store :skip-index-version-bump] flag)))]

      (doto (xt/start-node topo) .close)

      (with-redefs [c/index-version inc-index-version]
        (t/testing "standard IVOOSE"
          (t/is (thrown-with-cause? IndexVersionOutOfSyncException
                                    (doto (xt/start-node topo)
                                      (.close)))))

        (t/testing "version numbers have to match exactly"
          (t/is (thrown-with-cause? IndexVersionOutOfSyncException
                                    (doto (xt/start-node (-> topo (with-flag [(dec index-version) inc-index-version])))
                                      (.close))))


          (t/is (thrown-with-cause? IndexVersionOutOfSyncException
                                    (doto (xt/start-node (-> topo (with-flag [index-version (inc inc-index-version)])))
                                      (.close)))))

        (t/testing "supplying skip flag"
          (with-open [node (xt/start-node (-> topo (with-flag [index-version inc-index-version])))]
            (t/is node)))

        (t/testing "only need to supply skip-index-version-bump once"
          (with-open [node (xt/start-node topo)]
            (t/is node)))))))
