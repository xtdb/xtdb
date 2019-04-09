;; tag::a-story/create-ns[]
(ns a-story
  (:require [crux.api :as crux]))
;; end::a-story/create-ns[]


;; a shopkeeper Charles has an artifact [limestone comb]
;; pirate Mary steals it
;; rogue traveller Joe aims to recover it


;; tag::a-story/def-system[]
(def system
  (crux/start-standalone-system
    {:kv-backend "crux.kv.memdb.MemKv" :db-dir "data/db-dir-1"}))
;; end::a-story/def-system[]


;; Building some story
;; Story time 1740


; First character
;; tag::a-story/def-character[]
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
;; end::a-story/def-character[]


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

   ; artifacts
   ; There was a Cozy Mug
   [:crux.tx/put :ids.artifacts/cozy-mug
    {:crux.db/id :ids.artifacts/cozy-mug
     :artifact/title "A Rather Cozy Mug"
     :artifact.perks/int 3}
    #inst "1625-05-18"]

   [:crux.tx/put :ids.artifacts/forbidden-beans
    {:crux.db/id :ids.artifacts/forbidden-beans
     :artifact/title "Magic beans"
     :artifact.perks/int 30
     :artifact.perks/hp -20}
    #inst "1500-05-18"]

   [:crux.tx/put :ids.artifacts/laptop
    {:crux.db/id :ids.artifacts/laptop
     :artifact/title "Doesn't belong to the realm"}
    #inst "2018-05-18"]

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


;; Looking Around (Queries)

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


;; Some character development

; Note that in Crux tranactions rewrite the whole entity.
; It's a bare bones product. Core is intentionally slim.
; So we're open for your convinience utils projects.

; Set up ownership
; Charles was 25 when he got the Cozy Mug
(let [charles (crux/entity db :ids.people/Charles)]
  (crux/submit-tx system
    [[:crux.tx/cas :ids.people/Charles
      charles
      (assoc charles :person/has :ids.artifacts/cozy-mug)
      #inst "1725-05-18"]]))


; Let's not repeat ourselves
(defn entity-update
  [entity-id new-attrs valid-time]
  (let [entity-prev-value (crux/entity (crux/db system) entity-id)]
    (crux/submit-tx system
      [[:crux.tx/put entity-id
        (merge entity-prev-value new-attrs)
        valid-time]])))

(defn entity
  [entity-id]
  (crux/entity (crux/db system) entity-id))


; Charles got smarter to his thirties
(entity-update :ids.people/Charles
  {:person/int  55}
  #inst "1730-05-18")

(entity :ids.people/Charles)

; {:crux.db/id :ids.people/Charles, :person/has :ids.artifacts/cozy-mug}

; Ah put updates only the whole entity

    character-one

   ; ownership
    ; charles was 25 when he got the cozy
   [:crux.tx/put :ids.people/Charles
    {:crux.db/id :ids.people/Charles
     :person/has :ids.artifacts/cozy-mug}
    #inst "1725-05-18"]

; Charles got smarter to his thirties
(crux/submit-tx
  system
  [[:crux.tx/put :ids.people/Charles
    {:crux.db/id :ids.people/Charles
     :person/int  35}
    #inst "1720-05-18"]])


; Query entities
(crux/entity (crux/db system) :ids.people/Charles)



; ok magic beans once were in the realm but left it, since a balance patch
(crux/submit-tx
  system
  [[:crux.tx/delete :ids.artifacts/forbidden-beans
    #inst "1600-05-18"]])

; and sometimes people enter data which doesn't belong to the place
(crux/submit-tx
  system
  [[:crux.tx/evict :ids.artifacts/laptop
    #inst "1500-01-01"]])


; plot : Mary steals Charles cozy-mug in June
(crux/submit-tx
  system
  [; rest of characters
   [:crux.tx/put :ids.people/Mary
    {:crux.db/id :ids.people/Mary
     :person/has :ids.artifacts/cozy-mug}]
   [:crux.tx/delete ; note delete
    {:crux.db/id :ids.people/Charles
     :person/has :ids.artifacts/cozy-mug}
    #inst "1740-06-18"]])

