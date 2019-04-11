;; tag::a-tale/create-ns[]
(ns a-tale
  (:require [crux.api :as crux]))
;; end::a-tale/create-ns[]


;; a shopkeeper Charles has an artefact [limestone comb]
;; pirate Mary steals it
;; rogue traveller Joe aims to recover it


;; tag::a-tale/def-system[]
(def system
  (crux/start-standalone-system
    {:kv-backend "crux.kv.memdb.MemKv" :db-dir "data/db-dir-1"}))
;; end::a-tale/def-system[]


;; Building some story
;; Story time 1740

; First character
;; tag::a-tale/def-character[]
(crux/submit-tx
  system
  [[:crux.tx/put :ids.people/Charles  ; id for the storage (mem or Kafka)
    {:crux.db/id :ids.people/Charles  ; id for Crux
     :person/name "Charles"
     ; age 40
     :person/born #inst "1700-05-18"
     :person/location :ids.places/rarities-shop
     :person/str  40
     :person/int  40
     :person/dex  40
     :person/hp   40
     :person/gold 10000}
    #inst "1700-05-18"]]) ; valid time (optional)
;; end::a-tale/def-character[]


;; tag::a-tale/rest[]
; Load the remaining part of the set
(crux/submit-tx
  system
  [; rest of characters
   [:crux.tx/put :ids.people/Mary
    {:crux.db/id :ids.people/Mary
     :person/name "Mary"
     ; age  30
     :person/born #inst "1710-05-18"
     :person/location :ids.places/carribean
     :person/str  40
     :person/int  50
     :person/dex  50
     :person/hp   50}
    #inst "1710-05-18"]
   [:crux.tx/put :ids.people/Joe
    {:crux.db/id :ids.people/Joe
     :person/name "Joe"
     ; age  25
     :person/born #inst "1715-05-18"
     :person/location :ids.places/city
     :person/str  39
     :person/int  40
     :person/dex  60
     :person/hp   60
     :person/gold 70}
    #inst "1715-05-18"]

   ; artefacts
   ; There was a Cozy Mug
   [:crux.tx/put :ids.artefacts/cozy-mug
    {:crux.db/id :ids.artefacts/cozy-mug
     :artefact/title "A Rather Cozy Mug"
     :artefact.perks/int 3}
    #inst "1625-05-18"]
   [:crux.tx/put :ids.artefacts/forbidden-beans
    {:crux.db/id :ids.artefacts/forbidden-beans
     :artefact/title "Magic beans"
     :artefact.perks/int 30
     :artefact.perks/hp -20}
    #inst "1500-05-18"]
   [:crux.tx/put :ids.artefacts/pirate-sword
    {:crux.db/id :ids.artefacts/pirate-sword
     :artefact/title "A used sword"}
    #inst "1710-05-18"]
   [:crux.tx/put :ids.artefacts/flintlock-pistol
    {:crux.db/id :ids.artefacts/flintlock-pistol
     :artefact/title "Flintlock pistol"}
    #inst "1710-05-18"]
   [:crux.tx/put :ids.artefacts/unknown-key
    {:crux.db/id :ids.artefacts/unknown-key
     :artefact/title "Key from an unknown door"}
    #inst "1700-05-18"]
   [:crux.tx/put :ids.artefacts/laptop
    {:crux.db/id :ids.artefacts/laptop
     :artefact/title "A Tell DPS Laptop (what?)"}
    #inst "2016-05-18"]

   ; places
   [:crux.tx/put :ids.places/continent
    {:crux.db/id :ids.places/continent
     :place/title "Ah The Continent"}
    #inst "1000-01-01"]
   [:crux.tx/put :ids.places/carribean
    {:crux.db/id :ids.places/carribean
     :place/title "Ah The Good Ol Carribean Sea"
     :place/location :ids.places/carribean}
    #inst "1000-01-01"]
   [:crux.tx/put :ids.places/coconut-island
    {:crux.db/id :ids.places/coconut-island
     :place/title "Coconut Island"
     :place/location :ids.places/carribean}
    #inst "1000-01-01"]
   ])



;; Looking Around : Basic Queries

; Get a database value, read it until it's changed
(def db (crux/db system))

; Query entities
(crux/entity db :ids.people/Charles)

; Datalog syntax : query ids
(crux/q db
        '[:find ?e
          :where
          [?e :person/name "Charles"]])

; Datalog syntax : query more fields
(crux/q db
        '[:find ?e ?name ?int
          :where
          [?e :person/name "Charles"]
          [?e :person/name ?name]
          [?e :person/int  ?int]])

; See all artefact names
(crux/q db
        '[:find ?name
          :where
          [_ :artefact/title ?name]])
; yeilds
#{["Magic beans"] ["Doesn't belong to the realm"] ["A Rather Cozy Mug"]}


;; Balancing the world

; ok magic beans once were in the realm but left it, since a balance patch
(crux/submit-tx
  system
  [[:crux.tx/delete :ids.artefacts/forbidden-beans
    #inst "1690-05-18"]])

; and sometimes people enter data which doesn't belong to the place
(crux/submit-tx
  system
  [[:crux.tx/evict :ids.artefacts/laptop]])

; See all artefact names
(crux/q (crux/db system)
        '[:find ?name
          :where
          [_ :artefact/title ?name]])

; yeilds
#{["A Rather Cozy Mug"]}


; Historians will know about the beans though
(crux/q (crux/db system #inst "1599-01-01")
        '[:find ?name
          :where
          [_ :artefact/title ?name]])

; yeilds
#{["Magic beans"]}



;; Some character development

; Note that in Crux tranactions rewrite the whole entity.
; It's a bare bones product. Core is intentionally slim.
; So we're open for your convinience utils projects.

; Give em some artefacts
; Charles was 25 when he got the Cozy Mug
(crux/submit-tx system
  [(let [charles (crux/entity (crux/db system #inst "1725-05-18") :ids.people/Charles)]
     [:crux.tx/cas :ids.people/Charles
      charles
      (assoc charles
             :person/has
             #{:ids.artefacts/cozy-mug :ids.artefacts/unknown-key})
      #inst "1725-05-18"])
   (let [mary  (crux/entity (crux/db system #inst "1715-05-18") :ids.people/Mary)]
     [:crux.tx/cas :ids.people/Mary
      mary
      (assoc mary
             :person/has
             #{:ids.artefacts/pirate-sword :ids.artefacts/flintlock-pistol})
      #inst "1715-05-18"])])



; Who has what : basic joins
(def who-has-what-query
  '[:find ?name ?atitle
    :where
    [?p :person/name ?name]
    [?p :person/has ?artefact-id]
    [?artefact-id :artefact/title ?atitle]])

(crux/q (crux/db system) who-has-what-query)

; yeilds
#{["Mary" "A used sword"]
  ["Mary" "Flintlock pistol"]
  ["Charles" "A Rather Cozy Mug"]
  ["Charles" "Key from an unknown door"]}



; Lets not repeat ourselves
(defn entity-update
  [entity-id new-attrs valid-time]
  (let [entity-prev-value (crux/entity (crux/db system) entity-id)]
    (crux/submit-tx system
      [[:crux.tx/put entity-id
        (merge entity-prev-value new-attrs)
        valid-time]])))

(defn q
  [query]
  (crux/q (crux/db system) query))

(defn entity
  [entity-id]
  (crux/entity (crux/db system) entity-id))

(defn entity-at
  [entity-id valid-time]
  (crux/entity (crux/db system valid-time) entity-id))

(defn entity-with-adjacent
  [entity-id keys-to-pull]
  (let [db (crux/db system)
        ids->entities
        (fn [ids]
          (cond-> (map #(crux/entity db %) ids)
            (set? ids) set
            (vector? ids) vec))]
    (reduce
      (fn [e adj-k]
        (let [v (get e adj-k)]
          (assoc e adj-k
                 (cond
                   (keyword? v) (crux/entity db v)
                   (or (set? v)
                       (vector? v)) (ids->entities v)
                   :else v))))
      (crux/entity db entity-id)
      keys-to-pull)))

(entity-with-adjacent :ids.people/Charles [:person/has])

; yeilds
{:crux.db/id :ids.people/Charles,
 :person/str 40,
 :person/dex 40,
 :person/has
 #{{:crux.db/id :ids.artefacts/unknown-key,
    :artefact/title "Key from an unknown door"}
   {:crux.db/id :ids.artefacts/cozy-mug,
    :artefact/title "A Rather Cozy Mug",
    :artefact.perks/int 3}},
 :person/location :ids.places/rarities-shop,
 :person/hp 40,
 :person/int 40,
 :person/name "Charles",
 :person/gold 10000,
 :person/born #inst "1700-05-18T00:00:00.000-00:00"}


; Charles got smarter to his thirties
(entity-update :ids.people/Charles
  {:person/int  55}
  #inst "1730-05-18")


(entity :ids.people/Charles)

;yields
{:person/str 40,
 :person/dex 40,
 :person/has :ids.artefacts/cozy-mug,
 :person/location :ids.places/rarities-shop,
 :person/hp 40,
 :person/int 55,
 :person/name "Charles",
 :crux.db/id :ids.people/Charles,
 :person/gold 10000,
 :person/born #inst "1700-05-18T00:00:00.000-00:00"}


;; associations / joins
 ; who was where

;; use any clojure core function
;; use your own functions (subject to change)


; plot : Mary steals The Mug in June
(let [theft-date #inst "1740-06-18"]
  (crux/submit-tx
    system
    [; rest of characters
     [:crux.tx/put :ids.people/Charles
      (update (entity-at :ids.people/Charles theft-date)
              :person/has
              disj
              :ids.artefacts/cozy-mug)
      theft-date]
     [:crux.tx/put :ids.people/Mary
      (update (entity-at :ids.people/Mary theft-date)
              :person/has
              (comp set conj)
              :ids.artefacts/cozy-mug)
      theft-date]]))

; plot : Mary moves her operations to Carribean


; Now we think we're done with the story.
; We have a picture and we're ready to blame Mary for stealing the Mug.
; Then a new upstream data source kicks in. We uncover previously
; a previously unknown plast of history.

; turns out it was Mary's family mug all along
(let [marys-birth-inst #inst "1710-05-18"
      db        (crux/db system marys-birth-inst)
      baby-mary (crux/entity db :ids.people/Mary)]
  (crux/submit-tx
    system
    [[:crux.tx/cas :ids.people/Mary
      baby-mary
      (update baby-mary :person/has (comp set conj) :ids.artefacts/cozy-mug)
      marys-birth-inst]]))

; but she lost it in 1723
(let [mug-lost-date  #inst "1723-01-09"
      db        (crux/db system mug-lost-date)
      mary      (crux/entity db :ids.people/Mary)]
  (crux/submit-tx
    system
    [[:crux.tx/cas :ids.people/Mary
      mary
      (update mary :person/has (comp set disj) :ids.artefacts/cozy-mug)
      mug-lost-date]]))

(crux/q
  (crux/db system #inst "1715-05-18")
  who-has-what-query)

#{["Mary" "A Rather Cozy Mug"]}

(crux/q
  (crux/db system #inst "1723-05-18")
  who-has-what-query)
; todo yeilds

(crux/q
  (crux/db system #inst "1726-05-18")
  who-has-what-query)

(crux/q
  (crux/db system #inst "1740-06-19")
  who-has-what-query)
;; end::a-tale/rest[]
