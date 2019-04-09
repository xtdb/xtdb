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


; First character
;; tag::a-story/insert-one[]
(crux/submit-tx
  system
  [[:crux.tx/put :ids.people/Charles  ; id for Kafka
    {:crux.db/id :ids.people/Charles  ; id for Crux
     :person/name "Charles"
     :person/age  40
     :person/str  40
     :person/int  40
     :person/dex  40
     :person/hp   40
     :person/gold 10000}
    #inst "2018-05-18T09:20:27.966"]]) ; valid time
;; end::a-story/def-character[]

; Correct int
(crux/submit-tx
  system
  [[:crux.tx/put :ids.people/Charles
    {:crux.db/id :ids.people/Charles
     :person/int  60}
    #inst "2018-05-18T09:20:27.966"]])

(crux/submit-tx
  system
  [; rest of characters
   [:crux.tx/put :ids.people/Mary
    {:crux.db/id :ids.people/Mary
     :person/name "Mary"
     :person/age  30
     :person/str  40
     :person/int  50
     :person/dex  50
     :person/hp   50}
    #inst "2018-05-18T09:20:27.966"]
   [:crux.tx/put :ids.people/Joe
    {:crux.db/id :ids.people/Joe
     :person/name "Joe"
     :person/age  25
     :person/str  39
     :person/int  40
     :person/dex  60
     :person/hp   60
     :person/gold 70}
    #inst "2018-05-18T09:20:27.966"]

   ; artifacts
   [:crux.tx/put :ids.artifacts/mug
    {:crux.db/id :ids.artifacts/mug
     :artifact/title "Charles Favourite Coffee Mug"
     :artifact.perks/int 3}
    #inst "2018-05-18T09:20:27.966"]

   ; places
   [:crux.tx/put :ids.places/coconut-island
    {:crux.db/id :ids.places/coconut-island
     :place/title "Coconut Island"
     :place/location :ids.places/carribean}
    #inst "2018-05-18T09:20:27.966"]
   [:crux.tx/put :ids.places/rarities-shop
    {:crux.db/id :ids.places/rarities-shop
     :place/title "Charles shop of rarities"
     :place/location :ids.places/continent}
    #inst "2018-05-18T09:20:27.966"]
   ])




