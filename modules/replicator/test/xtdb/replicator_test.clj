(ns xtdb.replicator-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [cognitect.transit :as transit]
            [xtdb.api :as xt]
            [xtdb.fixtures :as fix]
            [xtdb.replicator :as replicator])
  (:import java.io.File))

(def ^:dynamic ^File *log-dir*)

(t/use-fixtures :each
  (fn [f]
    (fix/with-tmp-dirs #{log-dir}
      (binding [*log-dir* log-dir]
        (fix/with-opts {:xtdb/tx-indexer {:replicator {:xtdb/module `replicator/->local-dir-replicator
                                                       :path log-dir
                                                       :tx-events-per-file 10}}}
          f))))

  fix/with-node)

(defn- force-close-file! []
  (replicator/force-close-file! (-> @(:!system fix/*api*)
                                    (get-in [:xtdb/tx-indexer :replicator]))))

(defn- read-log-files []
  (force-close-file!)

  (for [^File log-file (->> (seq (.listFiles *log-dir*))
                            (sort-by #(.getName ^File %)))]
    [(.getName log-file)
     (with-open [is (io/input-stream log-file)]
       (let [rdr (transit/reader is :json)]
         (loop [evts []]
           (if-let [evt (try
                          (transit/read rdr)
                          (catch Throwable _))]
             (recur (conj evts evt))
             evts))))]))

(t/deftest test-replicator
  (let [{tt0 ::xt/tx-time} (fix/submit+await-tx [[::xt/put {:xt/id :foo}]
                                                 [::xt/put {:xt/id :bar}]])

        {tt1 ::xt/tx-time} (fix/submit+await-tx [[::xt/delete :bar #inst "2021"]
                                                 [::xt/put {:xt/id :baz}]])]

    (t/is (= [["log.0.transit.json"
               [[:commit #::xt{:tx-id 0, :tx-time tt0}
                 [[:put {:eid :foo, :doc {:crux.db/id :foo}
                         :start-valid-time tt0, :end-valid-time nil}]
                  [:put {:eid :bar, :doc {:crux.db/id :bar}
                         :start-valid-time tt0, :end-valid-time nil}]]]
                [:commit #::xt{:tx-id 1, :tx-time tt1}
                 [[:delete {:eid :bar, :doc nil
                            :start-valid-time #inst "2021", :end-valid-time tt0}]
                  [:put {:eid :baz, :doc {:crux.db/id :baz}
                         :start-valid-time tt1, :end-valid-time nil,}]]]]]]

             (read-log-files)))))

(t/deftest splits-files
  (let [{tt0 ::xt/tx-time} (fix/submit+await-tx (for [n (range 6)]
                                                  [::xt/put {:xt/id n}]))

        {tt1 ::xt/tx-time} (fix/submit+await-tx (for [n (range 6 12)]
                                                  [::xt/put {:xt/id n}]))

        {tt2 ::xt/tx-time} (fix/submit+await-tx (for [n (range 12 18)]
                                                  [::xt/put {:xt/id n}]))]

    (t/is (= [["log.0.transit.json"
               [[:commit #::xt{:tx-id 0, :tx-time tt0}
                 (for [n (range 6)]
                   [:put {:eid n, :doc {:crux.db/id n}
                          :start-valid-time tt0, :end-valid-time nil}])]
                [:commit #::xt{:tx-id 1, :tx-time tt1}
                 (for [n (range 6 12)]
                   [:put {:eid n, :doc {:crux.db/id n}
                          :start-valid-time tt1, :end-valid-time nil}])]]]
              ["log.1.transit.json"
               [[:commit #::xt{:tx-id 2, :tx-time tt2}
                 (for [n (range 12 18)]
                   [:put {:eid n, :doc {:crux.db/id n}
                          :start-valid-time tt2, :end-valid-time nil}])]]]]

             (read-log-files)))))

(comment
  (require '[xtdb.fixtures.tpch :as tpch])

  (with-open [node (xt/start-node {:xtdb/tx-indexer {:replicator {:xtdb/module `replicator/->local-dir-replicator
                                                                  :path "/tmp/tpch"}}})]
    (tpch/submit-docs! node 0.01)
    (xt/sync node)))
