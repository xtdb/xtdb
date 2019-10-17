(ns crux.repl-walkthrough-test
  (:require [crux.api :as crux]
            [crux.decorators.aggregation.alpha :as aggr]
            [clojure.pprint :as pp]
	    [clojure.test :as t])
  (:import (crux.api ICruxAPI)))

(t/deftest graph-traversal-test
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

  (def crux-options
    {:crux.node/topology :crux.standalone/topology
     :crux.node/kv-store "crux.kv.memdb/kv"
     :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"
     :crux.standalone/event-log-dir "data/event-log-dir-1"
     :crux.kv/db-dir "data/db-dir-1"})

  (def node (crux/start-node crux-options))
  (t/is node)

  (crux/sync node (:crux.tx/tx-time
                   (crux/submit-tx
                    node
                    (mapv (fn [n] [:crux.tx/put n]) nodes)))
             nil)

  (def db (crux/db node))

  (t/is (= #{[:Role2] [:Role3]}
           (crux/q db '{:find [?roleName]
                        :where
                        [[?e :hasRoleInGroups ?roleInGroup]
                         [?roleInGroup :hasGroups ?group]
                         [?roleInGroup :hasRoles ?role]
                         [?role :role/name ?roleName]]
                        :args [{?e :User1 ?group :Group2}]})))

  (t/is (= #{[:Group1 :Role5] [:Group3 :Role5] [:Group2 :Role4]
             [:Group3 :Role6] [:Group2 :Role3] [:Group1 :Role2]}
           (crux/q db '{:find [?groupName ?roleName]
                        :where
                        [[?e :hasRoleInGroups ?roleInGroup]
                         [?roleInGroup :hasGroups ?group]
                         [?group :group/name ?groupName]
                         [?roleInGroup :hasRoles ?role]
                         [?role :role/name ?roleName]]
                        :args [{?e :User2}]})))

  (def rules '[[(user-roles-in-groups ?user ?role ?group)
                [?user :hasRoleInGroups ?roleInGroup]
                [?roleInGroup :hasGroups ?group]
                [?roleInGroup :hasRoles ?role]]])

  (t/is (= #{[:Group3 :Role4] [:Group3 :Role3] [:Group2 :Role3] [:Group2 :Role2]}
           (crux/q db {:find '[?groupName ?roleName]
                       :where '[(user-roles-in-groups ?user ?role ?group)
                                [?group :group/name ?groupName]
                                [?role :role/name ?roleName]]
                       :rules rules
                       :args '[{?user :User1}]})))

  (t/is (= [{:groupName :Group2, :roleCount 1}]
           (aggr/q db {:aggr '{:partition-by [?groupName]
                               :select
                               {?roleCount [0 (inc acc) ?role]}}
                       :where '[(user-roles-in-groups ?user1 ?role ?group)
                                (user-roles-in-groups ?user2 ?role ?group)
                                [?group :group/name ?groupName]]
                       :rules rules
                       :args '[{?user1 :User1 ?user2 :User2}]}))))

(t/deftest walkthrough-test
  (def crux-options
    {:crux.node/topology :crux.standalone/topology
     :crux.node/kv-store "crux.kv.memdb/kv"
     :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"
     :crux.standalone/event-log-dir "data/event-log-dir-1"
     :crux.kv/db-dir "data/db-dir-1"})

  (def node (crux/start-node crux-options))

  (t/is node)

  (crux/sync node (:crux.tx/tx-time
                   (crux/submit-tx
                    node
                    [[:crux.tx/put
                      {:crux.db/id :dbpedia.resource/Pablo-Picasso
                       :name "Pablo"
                       :last-name "Picasso"
                       :location "Spain"}
                      #inst "1881-10-25T09:20:27.966-00:00"]
                     [:crux.tx/put
                      {:crux.db/id :dbpedia.resource/Pablo-Picasso
                       :name "Pablo"
                       :last-name "Picasso"
                       :location "Sain2"}
                      #inst "1881-10-25T09:20:27.966-00:00"]]))
             nil)

  (crux/sync node (:crux.tx/tx-time
                   (crux/submit-tx
                    node
                    [[:crux.tx/cas
                      {:crux.db/id :dbpedia.resource/Pablo-Picasso
                       :name "Pablo"
                       :last-name "Picasso"
                       :location "Spain"}
                      {:crux.db/id :dbpedia.resource/Pablo-Picasso
                       :name "Pablo"
                       :last-name "Picasso"
                       :height 1.63
                       :location "France"}
                      #inst "1973-04-08T09:20:27.966-00:00"]]))
             nil)

  (crux/sync node (:crux.tx/tx-time
                   (crux/submit-tx
                    node
                    [[:crux.tx/delete :dbpedia.resource/Pablo-Picasso
                      #inst "1973-04-08T09:20:27.966-00:00"]]))
             nil)

  (crux/sync node (:crux.tx/tx-time
                   (crux/submit-tx
                    node
                    [[:crux.tx/evict :dbpedia.resource/Pablo-Picasso
                      #inst "1973-04-07T09:20:27.966-00:00"
                      #inst "1973-04-09T09:20:27.966-00:00"
                      false
                      true]]))
             nil)

  (t/is nil? (crux/q
              (crux/db node)
              '{:find [e]
                :where [[e :name "Pablo"]]
                :full-results? true}))


  (t/is (= #{[{:crux.db/id :dbpedia.resource/Pablo-Picasso, :name "Pablo", :last-name "Picasso", :location "Sain2"}]}
           (crux/q
            (crux/db node #inst "1973-04-07T09:20:27.966-00:00")
            '{:find [e]
              :where [[e :name "Pablo"]]
              :full-results? true})))


  (crux/sync node (:crux.tx/tx-time (crux/submit-tx
                                     node
                                     [[:crux.tx/put
                                       {:crux.db/id :dbpedia.resource/Pablo-Picasso
                                        :name "Pablo"
                                        :last-name "Picasso"
                                        :height 1.63
                                        :location "France"}
                                       #inst "1973-04-08T09:20:27.966-00:00"]]))
             nil)

  (t/is (= #{[{:crux.db/id :dbpedia.resource/Pablo-Picasso, :name "Pablo", :last-name "Picasso", :height 1.63, :location "France"}]}
           (crux/q
            (crux/db node)
            '{:find [e]
              :where [[e :name "Pablo"]]
              :full-results? true})))

  (t/is (= #{[{:crux.db/id :dbpedia.resource/Pablo-Picasso, :name "Pablo", :last-name "Picasso", :location "Sain2"}]}
           (crux/q
            (crux/db node #inst "1973-04-07T09:20:27.966-00:00")
            '{:find [e]
              :where [[e :name "Pablo"]]
              :full-results? true})))
  )
