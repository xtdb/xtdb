(ns xtdb.docs.examples-test
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]))

(defn example-query-entity [node]
;; tag::query-entity[]
(xt/entity (xt/db node) :dbpedia.resource/Pablo-Picasso)
;; end::query-entity[]
)

(defn example-query-valid-time [node]
;; tag::query-valid-time[]
(xt/q (xt/db node #inst "2018-05-19T09:20:27.966-00:00")
        '{:find [e]
          :where [[e :name "Pablo"]]})
;; end::query-valid-time[]
)


(defn example-entity-history [node]
;; tag::history-full[]
(xt/submit-tx
  node
  [[::xt/put
    {:xt/id :ids.persons/Jeff
     :person/name "Jeff"
     :person/wealth 100}
    #inst "2018-05-18T09:20:27.966"]
   [::xt/put
    {:xt/id :ids.persons/Jeff
     :person/name "Jeff"
     :person/wealth 1000}
    #inst "2015-05-18T09:20:27.966"]])

; yields
{::xt/tx-id 1555314836178,
 ::xt/tx-time #inst "2019-04-15T07:53:56.178-00:00"}

; Returning the history in descending order
; To return in ascending order, use :asc in place of :desc
(xt/entity-history (xt/db node) :ids.persons/Jeff :desc)

; yields
[{::xt/tx-time #inst "2019-04-15T07:53:55.817-00:00",
  ::xt/tx-id 1555314835817,
  ::xt/valid-time #inst "2018-05-18T09:20:27.966-00:00",
  ::xt/content-hash ; sha1 hash of document contents
  "6ca48d3bf05a16cd8d30e6b466f76d5cc281b561"}
 {::xt/tx-time #inst "2019-04-15T07:53:56.178-00:00",
  ::xt/tx-id 1555314836178,
  ::xt/valid-time #inst "2015-05-18T09:20:27.966-00:00",
  ::xt/content-hash "a95f149636e0a10a78452298e2135791c0203529"}]
;; end::history-full[]

;; tag::history-with-docs[]
(xt/entity-history (xt/db node) :ids.persons/Jeff :desc {:with-docs? true})

; yields
[{::xt/tx-time #inst "2019-04-15T07:53:55.817-00:00",
  ::xt/tx-id 1555314835817,
  ::xt/valid-time #inst "2018-05-18T09:20:27.966-00:00",
  ::xt/content-hash
  "6ca48d3bf05a16cd8d30e6b466f76d5cc281b561"
  ::xt/doc
  {:xt/id :ids.persons/Jeff
   :person/name "Jeff"
   :person/wealth 100}}
 {::xt/tx-time #inst "2019-04-15T07:53:56.178-00:00",
  ::xt/tx-id 1555314836178,
  ::xt/valid-time #inst "2015-05-18T09:20:27.966-00:00",
  ::xt/content-hash "a95f149636e0a10a78452298e2135791c0203529"
  ::xt/doc
  {:xt/id :ids.persons/Jeff
   :person/name "Jeff"
   :person/wealth 1000}}]
;; end::history-with-docs[]

;; tag::history-range[]

; Passing the additional 'opts' map with the start/end bounds.
; As we are returning results in :asc order, the map contains the earlier starting coordinates -
; If returning history range in descending order, we pass the later coordinates as start coordinates to the map
(xt/entity-history
 (xt/db node)
 :ids.persons/Jeff
 :asc
 {:start-valid-time #inst "2015-05-18T09:20:27.966"
  :start-tx {::xt/tx-time #inst "2015-05-18T09:20:27.966"}
  :end-valid-time #inst "2020-05-18T09:20:27.966"
  :end-tx {::xt/tx-time #inst "2020-05-18T09:20:27.966"}})

; yields
[{::xt/tx-time #inst "2019-04-15T07:53:56.178-00:00",
  ::xt/tx-id 1555314836178,
  ::xt/valid-time #inst "2015-05-18T09:20:27.966-00:00",
  ::xt/content-hash
  "a95f149636e0a10a78452298e2135791c0203529"}
 {::xt/tx-time #inst "2019-04-15T07:53:55.817-00:00",
  ::xt/tx-id 1555314835817
  ::xt/valid-time #inst "2018-05-18T09:20:27.966-00:00",
  ::xt/content-hash "6ca48d3bf05a16cd8d30e6b466f76d5cc281b561"}]

;; end::history-range[]
)

(defn example-history-predicates [node]
;; tag::history-predicates[]
(xt/submit-tx node [[::xt/put
                     {:xt/id 1 :hello :world}
                     #inst "1900-08-29T15:05:31.530-00:00"
                     #inst "2100-08-29T15:05:31.530-00:00"]])

(xt/q (xt/db node)
      '{:find [e start-time end-time]
        :where [[e :hello :world]
                [(get-start-valid-time e) start-time]
                [(get-end-valid-time e) end-time]]})

;yields
#{[1
   #inst "1900-08-29T15:05:31.530-00:00"
   #inst "2100-08-29T15:05:31.530-00:00"]}

;; end::history-predicates[]
)
