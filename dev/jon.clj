(ns jon
  (:require [datomic.api :as d]))

(defn random-person [] {:db/id (d/tempid :db.part/user)
                        :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
                        :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
                        ;; :sex       (rand-nth [:male :female])
                        ;; :age       (rand-int 100)
                        ;; :salary    (rand-int 100000)
                        })

(def schema
  [#:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident :name}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident :last-name}])

(defn attr-id->ident [db ^datomic.db.Datum datom]
  (:ident (d/attribute db (.a datom))))

(defn datomic->crux [conn]
  (let [db (d/db conn)]
    (for [[eid datoms] (->> (seq (d/datoms db :eavt))
                            (remove #(some->> % (attr-id->ident db) namespace (re-find #"^db|^fressian")))
                            (group-by #(.e ^datomic.db.Datum %)))]
      (into {}
            (for [^datomic.db.Datum d datoms]
              [(attr-id->ident db d) (.v d)])))))

(comment
  (def datomic-uri (str "datomic:mem://" (d/squuid)))
  (d/create-database datomic-uri)
  (def conn (d/connect datomic-uri))
  @(d/transact conn schema)
  @(d/transact conn (take 5 (repeatedly random-person)))
  (datomic->crux conn))
