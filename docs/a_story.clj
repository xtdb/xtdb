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


; Story time 1740
; First character
;; tag::a-story/insert-one[]
(crux/submit-tx
  system
  [[:crux.tx/put :ids.people/Charles  ; id for the storage (mem or Kafka)
    {:crux.db/id :ids.people/Charles  ; id for Crux
     :person/name "Charles"
     ; age 40
     :person/born "1700-05-18T09:20:27.966"
     :person/str  40
     :person/int  40
     :person/dex  40
     :person/hp   40
     :person/gold 10000}
    #inst "1700-05-18T09:20:27.966"]]) ; valid time
;; end::a-story/def-character[]

; Correct int
(crux/submit-tx
  system
  [[:crux.tx/cas :ids.people/Charles
    {:crux.db/id :ids.people/Charles
     :person/int  40}
    {:crux.db/id :ids.people/Charles
     :person/int  55}
    #inst "1730-05-18T09:20"]])

; Load the remaining part of the set
(crux/submit-tx
  system
  [; rest of characters
   [:crux.tx/put :ids.people/Mary
    {:crux.db/id :ids.people/Mary
     :person/name "Mary"
     ; age  30
     :person/born "1710-05-18T09:20"
     :person/str  40
     :person/int  50
     :person/dex  50
     :person/hp   50}
    #inst "1710-05-18T09:20:27.966"]
   [:crux.tx/put :ids.people/Joe
    {:crux.db/id :ids.people/Joe
     :person/name "Joe"
     ; age  25
     :person/born "1715-05-18T09:20:27"
     :person/str  39
     :person/int  40
     :person/dex  60
     :person/hp   60
     :person/gold 70}
    #inst "1715-05-18T09:20:27"]

   ; artifacts
   [:crux.tx/put :ids.artifacts/mug
    ; charles was 25 when he got the mug
    {:crux.db/id :ids.artifacts/mug
     :artifact/title "Charles Favourite Mug"
     :artifact.perks/int 3}
    #inst "1725-05-18T09:20:27"]

   [:crux.tx/put :ids.people/Charles
    {:crux.db/id :ids.people/Charles
     :person/owns :ids.artifacts/mug}]

   [:crux.tx/put :ids.artifacts/forbidden-beans
    {:crux.db/id :ids.artifacts/forbidden-beans
     :artifact/title "Magic beans"
     :artifact.perks/int 30
     :artifact.perks/hp -20}
    #inst "2018-05-18T09:20:27.966"]

   [:crux.tx/put :ids.artifacts/laptop
    {:crux.db/id :ids.artifacts/laptop
     :artifact/title "Doesn't belong to the realm"}
    #inst "2018-05-18T09:20:27.966"]

   ; places
   [:crux.tx/put :ids.places/coconut-island
    {:crux.db/id :ids.places/coconut-island
     :place/title "Coconut Island"
     :place/location :ids.places/carribean}
    #inst "-100000-01-01T09:20:27"]
   [:crux.tx/put :ids.places/rarities-shop
    {:crux.db/id :ids.places/rarities-shop
     :place/title "Charles shop of rarities"
     :place/location :ids.places/city}
    #inst "1690-01-01T09:20:27"]
   [:crux.tx/put :ids.places/city
    {:crux.db/id :ids.places/city
     :place/title "An Old City"
     :place/location :ids.places/continent}
    #inst "1000-05-18T09:20:27"]
   [:crux.tx/put :ids.places/continent
    {:crux.db/id :ids.places/continent
     :place/title "Ah The Continent"}
    #inst "-10000000-01-01T09:20:27"]
   ])

; ok magic beans once were in the realm but left it, since a balance patch
(crux/submit-tx
  system
  [[:crux.tx/delete :ids.artifacts/forbidden-beans]])

; and sometimes people enter data which doesn't belong to the place
(crux/submit-tx
  system
  [[:crux.tx/evict :ids.artifacts/laptop]])

; plot : Mary steals Charles mug
(crux/submit-tx
  system
  [; rest of characters
   [:crux.tx/put :ids.people/Mary
    {:crux.db/id :ids.people/Mary
     :person/owns :ids.artifacts/mug}]
   [:crux.tx/delete ; note delete
    {:crux.db/id :ids.people/Charles
     :person/owns :ids.artifacts/mug}
    #inst "1715-05-18T09:20:27.966"]

   [:crux.tx/put :ids.people/Joe
    {:crux.db/id :ids.people/Joe
     :person/name "Joe"
     :person/age  25
     :person/str  39
     :person/int  40
     :person/dex  60
     :person/hp   60
     :person/gold 70}
    #inst "2018-05-18T09:20:27.966"]])

