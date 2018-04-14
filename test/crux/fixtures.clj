(ns crux.fixtures
  (:require [crux.core :as cr]
            [crux.kv :as kv]))

;; From Datascript:

(defn transact-schemas! [db]
  (cr/transact-schema! db {:attr/ident :name      :attr/type :string})
  (cr/transact-schema! db {:attr/ident :last-name :attr/type :string})
  (cr/transact-schema! db {:attr/ident :sex       :attr/type :keyword})
  (cr/transact-schema! db {:attr/ident :age       :attr/type :long})
  (cr/transact-schema! db {:attr/ident :salary    :attr/type :long}))

(def next-eid (atom 1000))

(defn random-person [] {:crux.core/id (swap! next-eid inc)
                        :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
                        :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
                        :sex       (rand-nth [:male :female])
                        :age       (rand-int 10)
                        :salary    (rand-int 100000)})

(def people (repeatedly random-person))

(def ^:dynamic db)

(defn start-system [f]
  (let [db-name :test]
    (binding [db (kv/open (crux.rocksdb/crux-rocks-kv db-name))]
      (try
        (cr/transact-schema! db {:attr/ident :foo :attr/type :string})
        (cr/transact-schema! db {:attr/ident :tar :attr/type :string})
        (transact-schemas! db)
        (f)
        (finally
          (kv/close db)
          (kv/destroy db))))))
