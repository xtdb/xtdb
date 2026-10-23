(ns xtdb.compactor.iid-runs-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.test-util :as tu])
  (:import (java.nio.file Files FileVisitOption Path)
           (org.apache.arrow.vector.types.pojo ArrowType$RunEndEncoded)
           (xtdb.arrow Relation RunEndEncodedVector)))

(t/use-fixtures :each tu/with-allocator)

(defn- foo-l1-data-files [^Path node-dir]
  (with-open [paths (Files/walk node-dir (make-array FileVisitOption 0))]
    (->> (iterator-seq (.iterator paths))
         (filter #(re-find #"public\$foo/data/l01-.*\.arrow$" (str %)))
         vec)))

(defn- iid-layout [^Path data-file]
  (with-open [loader (Relation/loader tu/*allocator* (Files/readAllBytes data-file))]
    (reduce (fn [acc ^long page-idx]
              (with-open [rel (.loadPage loader page-idx tu/*allocator*)]
                (let [iid (.vectorFor rel "_iid")]
                  (-> acc
                      (update :rows + (.getRowCount rel))
                      (update :runs + (if (instance? RunEndEncodedVector iid)
                                        (.getRunCount ^RunEndEncodedVector iid)
                                        (.getRowCount rel)))))))
            {:encoded? (instance? ArrowType$RunEndEncoded (.getType (.findField (.getSchema loader) "_iid")))
             :rows 0, :runs 0}
            (range (.getPageCount loader)))))

(defn- compact! [node-dir]
  (with-open [node (tu/->local-node {:node-dir node-dir})]
    (doseq [[v system-time] [[0 #inst "2020-01-01"] [1 #inst "2020-01-02"] [2 #inst "2020-01-03"]]]
      (xt/execute-tx node [(into [:put-docs :foo] (for [id (range 20)] {:xt/id id, :v v}))]
                     {:system-time system-time}))
    (tu/flush-block! node)
    (c/compact-all! node #xt/duration "PT5S")

    {:rows (xt/q node '(-> (from :foo {:for-valid-time :all-time, :bind [{:xt/id id} v]})
                           (order-by id v)))
     :layouts (->> (foo-l1-data-files node-dir)
                   (into {} (map (juxt #(if (re-find #"/l01-rc-" (str %)) :current :historical) iid-layout))))}))

(t/deftest compaction-writes-iids-as-runs-when-configured
  (tu/with-tmp-dirs #{plain-dir encoded-dir}
    (let [plain (compact! plain-dir)
          encoded (binding [c/*run-encode-iids?* true]
                    (compact! encoded-dir))]

      (t/is (= {:current {:encoded? false, :rows 20, :runs 20}
                :historical {:encoded? false, :rows 40, :runs 40}}
               (:layouts plain))
            "plain by default")

      (t/is (= {:current {:encoded? true, :rows 20, :runs 20}
                :historical {:encoded? true, :rows 40, :runs 20}}
               (:layouts encoded))
            "one run per entity in each file")
      (t/is (= (:rows plain) (:rows encoded))))))
