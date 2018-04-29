(ns crux.fixtures
  (:require [crux.kv :as cr]
            [crux.kv-store :as kv-store]
            [crux.core]
            crux.rocksdb))

;; From Datascript:

(defn transact-schemas! [db]
  (cr/transact-schema! db {:attr/ident :name      :attr/type :string})
  (cr/transact-schema! db {:attr/ident :last-name :attr/type :string})
  (cr/transact-schema! db {:attr/ident :sex       :attr/type :keyword})
  (cr/transact-schema! db {:attr/ident :age       :attr/type :long})
  (cr/transact-schema! db {:attr/ident :salary    :attr/type :long}))

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

(def ^:dynamic kv)

(defn start-system [f]
  (let [db-name :test]
    (binding [kv (kv-store/open (crux.core/kv db-name))]
      (try
        (cr/transact-schema! kv {:attr/ident :foo :attr/type :string})
        (cr/transact-schema! kv {:attr/ident :tar :attr/type :string})
        (transact-schemas! kv)
        (f)
        (finally
          (kv-store/close kv)
          (kv-store/destroy kv))))))
