(ns crux.fixtures
  (:require [crux.kv :as cr]
            [crux.kv-store :as kv-store]
            [crux.core]
            crux.rocksdb))

;; From Datascript:

(defn transact-schemas! [db]
  (cr/transact-schema! db {:crux.kv.attr/ident :name      :crux.kv.attr/type :string})
  (cr/transact-schema! db {:crux.kv.attr/ident :last-name :crux.kv.attr/type :string})
  (cr/transact-schema! db {:crux.kv.attr/ident :sex       :crux.kv.attr/type :keyword})
  (cr/transact-schema! db {:crux.kv.attr/ident :age       :crux.kv.attr/type :long})
  (cr/transact-schema! db {:crux.kv.attr/ident :salary    :crux.kv.attr/type :long}))

(def next-eid (atom 0))

(defn random-person [] {:crux.kv/id (swap! next-eid dec)
                        :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
                        :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
                        :sex       (rand-nth [:male :female])
                        :age       (rand-int 10)
                        :salary    (rand-int 100000)})

(def people (repeatedly random-person))

(defn transact-people!
  ([db people-mixins]
   (transact-people! db people-mixins (java.util.Date.)))
  ([db people-mixins ts]
   (let [people (->> people-mixins (map #(merge %1 %2) people))
         ids (cr/-put db people ts)]
     (map #(update % :crux.kv/id ids) people))))

(def ^:dynamic *kv*)

(defn start-system [f]
  (let [db-name :test]
    (binding [*kv* (kv-store/open (crux.core/kv db-name))]
      (try
        (cr/transact-schema! *kv* {:crux.kv.attr/ident :foo :crux.kv.attr/type :string})
        (cr/transact-schema! *kv* {:crux.kv.attr/ident :tar :crux.kv.attr/type :string})
        (transact-schemas! *kv*)
        (f)
        (finally
          (kv-store/close *kv*)
          (kv-store/destroy *kv*))))))
