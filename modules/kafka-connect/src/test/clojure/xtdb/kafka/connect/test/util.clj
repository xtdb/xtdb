(ns xtdb.kafka.connect.test.util
  (:require [clojure.test :refer :all]
            [xtdb.api :as xt]))

(defn query-col-types [node]
  (let [col-types-res (xt/q node "SELECT column_name, data_type
                                  FROM information_schema.columns
                                  WHERE table_name = 'my_table'")]
    (->> col-types-res
      (map (fn [{:keys [column-name data-type]}]
             [(keyword column-name) (read-string data-type)]))
      (into {}))))
