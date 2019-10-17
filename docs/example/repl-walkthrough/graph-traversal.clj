;; load a repl with the latest crux-core dependency, e.g. using clj:
;; $ clj -Sdeps '{:deps {juxt/crux-core {:mvn/version "RELEASE"}}}'

(ns walkthrough.graph-traversal
  (:require [crux.api :as crux]
            [crux.decorators.aggregation.alpha :as aggr]
            [clojure.pprint :as pp])
  (:import (crux.api ICruxAPI)))

;; inspired by http://docs.neo4j.org/chunked/stable/cypher-cookbook-hyperedges.html and https://github.com/Datomic/day-of-datomic/blob/master/tutorial/graph.clj

;; Noteworthy aspects of Crux usage shown below:
;; 1) no schema needed
;; 2) ability to transact edges before nodes
;; 3) keyword IDs

;; nodes with edges
(def nodes
  (for
    [n [{:user/name :User1
         :hasRoleInGroups #{:U1G3R34 :U1G2R23}}
        {:user/name :User2
      :hasRoleInGroups #{:U2G2R34 :U2G3R56 :U2G1R25}}
     {:role/name :Role1}
     {:role/name :Role2}
     {:role/name :Role3}
     {:role/name :Role4}
     {:role/name :Role5}
     {:role/name :Role6}
     {:group/name :Group1}
     {:group/name :Group2}
     {:group/name :Group3}
     {:roleInGroup/name :U2G2R34
      :hasGroups #{:Group2}
      :hasRoles #{:Role3 :Role4}}
     {:roleInGroup/name :U1G2R23
      :hasGroups #{:Group2}
      :hasRoles #{:Role2 :Role3}}
     {:roleInGroup/name :U1G3R34
      :hasGroups #{:Group3}
      :hasRoles #{:Role3 :Role4}}
     {:roleInGroup/name :U2G3R56
      :hasGroups #{:Group3}
      :hasRoles #{:Role5 :Role6}}
     {:roleInGroup/name :U2G1R25
      :hasGroups #{:Group1}
      :hasRoles #{:Role2 :Role5}}
     {:roleInGroup/name :U1G1R12
      :hasGroups #{:Group1}
      :hasRoles #{:Role1 :Role2}}]]
      (assoc n :crux.db/id (get n (some
                                   #{:user/name
                                     :group/name
                                     :role/name
                                     :roleInGroup/name}
                                   (keys n))))))

(crux/submit-tx
  node
  (mapv (fn [n] [:crux.tx/put n]) nodes))
; this standalone configuration is the easiest way to try Crux, no Kafka needed
(def crux-options
  {:crux.node/topology :crux.standalone/topology
   :crux.node/kv-store "crux.kv.memdb.MemKv" ; in-memory, see docs for LMDB/RocksDB storage
   :crux.standalone/event-log-dir "data/event-log-dir-1" ; :event-log-dir is ignored when using MemKv
   :crux.kv/db-dir "data/db-dir-1"}) ; :db-dir is ignored when using MemKv


(def node (crux/start-node crux-options))

(crux/submit-tx
  node
  (mapv (fn [n] [:crux.tx/put n]) nodes))

(def db (crux/db node))

;; find roles for user and particular groups
(crux/q db '{:find [?roleName]
             :where
             [[?e :hasRoleInGroups ?roleInGroup]
              [?roleInGroup :hasGroups ?group]
              [?roleInGroup :hasRoles ?role]
              [?role :role/name ?roleName]]
             :args [{?e :User1 ?group :Group2}]})

;; find all groups and roles for a user
(pp/pprint
  (crux/q db '{:find [?groupName ?roleName]
               :where
               [[?e :hasRoleInGroups ?roleInGroup]
                [?roleInGroup :hasGroups ?group]
                [?group :group/name ?groupName]
                [?roleInGroup :hasRoles ?role]
                [?role :role/name ?roleName]]
               :args [{?e :User2}]}))

;; a datalog rule
(def rules '[[(user-roles-in-groups ?user ?role ?group)
              [?user :hasRoleInGroups ?roleInGroup]
              [?roleInGroup :hasGroups ?group]
              [?roleInGroup :hasRoles ?role]]])

;; find all groups and roles for a user, using a datalog rule
(pp/pprint
  (crux/q db {:find '[?groupName ?roleName]
               :where '[(user-roles-in-groups ?user ?role ?group)
                       [?group :group/name ?groupName]
                       [?role :role/name ?roleName]]
               :rules rules
               :args '[{?user :User1}]}))

;; find common groups based on shared roles and count the number of shared roles using the aggregation decorator, which wraps the default `crux/q` and looks for :aggr instead of :find
(aggr/q db {:aggr '{:partition-by [?groupName]
                    :select
                    {?roleCount [0 (inc acc) ?role]}}
            :where '[(user-roles-in-groups ?user1 ?role ?group)
                     (user-roles-in-groups ?user2 ?role ?group)
                     [?group :group/name ?groupName]]
            :rules rules
            :args '[{?user1 :User1 ?user2 :User2}]})

;; try adding additional :hasRoleInGroups values (e.g. `#{:U1G1R12 :U2G3R56 :U2G1R25}`) to :User1 by submitting a new version of the document
