(ns xtdb.space-tutorial-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.codec :as c]
            [xtdb.fixtures :as fix :refer [*api*]]))

(t/use-fixtures :each fix/with-node)

(def manifest
  {:xt/id :manifest
   :pilot-name "Johanna"
   :id/rocket "SB002-sol"
   :id/employee "22910x2"
   :badges "SETUP"
   :cargo ["stereo" "gold fish" "slippers" "secret note"]})

(defn filter-appearance [description]
  (xt/q (xt/db *api*)
        {:find '[name IUPAC]
         :where '[[e :common-name name]
                  [e :IUPAC-name IUPAC]
                  [e :appearance appearance]]
         :args [{'appearance description}]}))

(defn put-all! [docs]
  (fix/submit+await-tx (for [doc docs]
                         [:xt/put doc])))

(defn stock-check
  [company-id item]
  {:result (xt/q (xt/db *api*)
                 {:find '[name funds stock]
                  :where ['[e :company-name name]
                          '[e :credits funds]
                          ['e item 'stock]]
                  :args [{'e company-id}]})
   :item item})

(defn format-stock-check [{:keys [result item] :as stock-check}]
  (for [[name funds commod] result]
    (str "Name: " name ", Funds: " funds ", " item " " commod)))


(defn full-query [node]
  (xt/q (xt/db node)
        '{:find [(pull id [*])]
          :where [[e :xt/id id]]}))

(t/deftest space-tutorial-test
  (t/testing "earth-test"
    (fix/submit+await-tx [[:xt/put manifest]])

    (t/is (= {:xt/id :manifest,
              :pilot-name "Johanna",
              :id/rocket "SB002-sol",
              :id/employee "22910x2",
              :badges "SETUP",
              :cargo ["stereo" "gold fish" "slippers" "secret note"]}
             (xt/entity (xt/db *api*) :manifest)))

    (t/is (= #{["secret note"]}
             (xt/q (xt/db *api*)
                   {:find '[belongings]
                    :where '[[e :cargo belongings]]
                    :args [{'belongings "secret note"}]}))))

  (t/testing "pluto-tests"
    (fix/submit+await-tx [[:xt/put
                           {:xt/id :commodity/Pu
                            :common-name "Plutonium"
                            :type :element/metal
                            :density 19.816
                            :radioactive true}]

                          [:xt/put
                           {:xt/id :commodity/N
                            :common-name "Nitrogen"
                            :type :element/gas
                            :density 1.2506
                            :radioactive false}]

                          [:xt/put
                           {:xt/id :commodity/CH4
                            :common-name "Methane"
                            :type :molecule/gas
                            :density 0.717
                            :radioactive false}]])


    (fix/submit+await-tx [[:xt/put {:xt/id :stock/Pu
                                    :commod :commodity/Pu
                                    :weight-ton 21}
                           #inst "2115-02-13T18"] ;; valid-time

                          [:xt/put {:xt/id :stock/Pu
                                    :commod :commodity/Pu
                                    :weight-ton 23}
                           #inst "2115-02-14T18"]

                          [:xt/put {:xt/id :stock/Pu
                                    :commod :commodity/Pu
                                    :weight-ton 22.2}
                           #inst "2115-02-15T18"]

                          [:xt/put {:xt/id :stock/Pu
                                    :commod :commodity/Pu
                                    :weight-ton 24}
                           #inst "2115-02-18T18"]

                          [:xt/put {:xt/id :stock/Pu
                                    :commod :commodity/Pu
                                    :weight-ton 24.9}
                           #inst "2115-02-19T18"]])

    (fix/submit+await-tx [[:xt/put {:xt/id :stock/N
                                    :commod :commodity/N
                                    :weight-ton 3}
                           #inst "2115-02-13T18" ;; start valid-time
                           #inst "2115-02-19T18"] ;; end valid-time

                          [:xt/put {:xt/id :stock/CH4
                                    :commod :commodity/CH4
                                    :weight-ton 92}
                           #inst "2115-02-15T18"
                           #inst "2115-02-19T18"]])

    (t/is (= {:xt/id :stock/Pu, :commod :commodity/Pu, :weight-ton 21}
             (xt/entity (xt/db *api* #inst "2115-02-14") :stock/Pu)))
    (t/is (= {:xt/id :stock/Pu, :commod :commodity/Pu, :weight-ton 22.2}
             (xt/entity (xt/db *api* #inst "2115-02-18") :stock/Pu)))

    (fix/submit+await-tx [[:xt/put (assoc manifest :badges ["SETUP" "PUT"])]])

    (t/is (= {:xt/id :manifest,
              :pilot-name "Johanna",
              :id/rocket "SB002-sol",
              :id/employee "22910x2",
              :badges ["SETUP" "PUT"],
              :cargo ["stereo" "gold fish" "slippers" "secret note"]}
             (xt/entity (xt/db *api*) :manifest))))

  (t/testing "mercury-tests"
    (put-all! [{:xt/id :commodity/Pu
                :common-name "Plutonium"
                :type :element/metal
                :density 19.816
                :radioactive true}

               {:xt/id :commodity/N
                :common-name "Nitrogen"
                :type :element/gas
                :density 1.2506
                :radioactive false}

               {:xt/id :commodity/CH4
                :common-name "Methane"
                :type :molecule/gas
                :density 0.717
                :radioactive false}

               {:xt/id :commodity/Au
                :common-name "Gold"
                :type :element/metal
                :density 19.300
                :radioactive false}

               {:xt/id :commodity/C
                :common-name "Carbon"
                :type :element/non-metal
                :density 2.267
                :radioactive false}

               {:xt/id :commodity/borax
                :common-name "Borax"
                :IUPAC-name "Sodium tetraborate decahydrate"
                :other-names ["Borax decahydrate" "sodium borate" "sodium tetraborate" "disodium tetraborate"]
                :type :mineral/solid
                :appearance "white solid"
                :density 1.73
                :radioactive false}])

    (t/is (={:xt/id :commodity/borax
             :common-name "Borax"
             :IUPAC-name "Sodium tetraborate decahydrate"
             :other-names ["Borax decahydrate" "sodium borate" "sodium tetraborate" "disodium tetraborate"]
             :type :mineral/solid
             :appearance "white solid"
             :density 1.73
             :radioactive false}
            (xt/entity (xt/db *api*) :commodity/borax)))
    (t/is (= #{[:commodity/Pu] [:commodity/Au]}
             (xt/q (xt/db *api*)
                   '{:find [element]
                     :where [[element :type :element/metal]]})))
    (t/is (=
           (xt/q (xt/db *api*)
                 '{:find [element]
                   :where [[element :type :element/metal]]} )

           (xt/q (xt/db *api*)
                 {:find '[element]
                  :where '[[element :type :element/metal]]} )

           (xt/q (xt/db *api*)
                 (quote
                  {:find [element]
                   :where [[element :type :element/metal]]}) )))

    (t/is (= #{["Gold"] ["Plutonium"]}
             (xt/q (xt/db *api*)
                   '{:find [name]
                     :where [[e :type :element/metal]
                             [e :common-name name]]} )))

    (t/is (= #{["Nitrogen" 1.2506] ["Carbon" 2.267] ["Methane" 0.717] ["Borax" 1.73] ["Gold" 19.3] ["Plutonium" 19.816]}
             (xt/q (xt/db *api*)
                   '{:find [name rho]
                     :where [[e :density rho]
                             [e :common-name name]]})))

    (t/is (= #{["Plutonium"]}
             (xt/q (xt/db *api*)
                   '{:find [name]
                     :where [[e :common-name name]
                             [e :radioactive true]]})))

    (t/is (= #{["Gold"] ["Plutonium"]}
             (xt/q (xt/db *api*)
                   {:find '[name]
                    :where '[[e :type t]
                             [e :common-name name]]
                    :args [{'t :element/metal}]})))

    (t/is (= #{["Gold"] ["Plutonium"]}
             (xt/q (xt/db *api*)
                   {:find '[name]
                    :where '[[e :type t]
                             [e :common-name name]]
                    :args [{'t :element/metal}]})))

    (t/is (= (filter-appearance "white solid")
             #{["Borax" "Sodium tetraborate decahydrate"]}
             ))

    (fix/submit+await-tx [[:xt/put (assoc manifest :badges ["SETUP" "PUT" "DATALOG-QUERIES"])]])

    (t/is (= {:xt/id :manifest,
              :pilot-name "Johanna",
              :id/rocket "SB002-sol",
              :id/employee "22910x2",
              :badges ["SETUP" "PUT" "DATALOG-QUERIES"],
              :cargo ["stereo" "gold fish" "slippers" "secret note"]}
             (xt/entity (xt/db *api*) :manifest))))

  (t/testing "neptune-test"
    (fix/submit+await-tx [[:xt/put
                           {:xt/id :consumer/RJ29sUU
                            :consumer-id :RJ29sUU
                            :first-name "Jay"
                            :last-name "Rose"
                            :cover? true
                            :cover-type :Full}
                           #inst "2114-12-03"]])
    (fix/submit+await-tx [[:xt/put ;; <1>
                           {:xt/id :consumer/RJ29sUU
                            :consumer-id :RJ29sUU
                            :first-name "Jay"
                            :last-name "Rose"
                            :cover? true
                            :cover-type :Full}
                           #inst "2113-12-03" ;; Valid time start
                           #inst "2114-12-03"] ;; Valid time end

                          [:xt/put ;; <2>
                           {:xt/id :consumer/RJ29sUU
                            :consumer-id :RJ29sUU
                            :first-name "Jay"
                            :last-name "Rose"
                            :cover? true
                            :cover-type :Full}
                           #inst "2112-12-03"
                           #inst "2113-12-03"]

                          [:xt/put ;; <3>
                           {:xt/id :consumer/RJ29sUU
                            :consumer-id :RJ29sUU
                            :first-name "Jay"
                            :last-name "Rose"
                            :cover? false}
                           #inst "2112-06-03"
                           #inst "2112-12-02"]

                          [:xt/put ;; <4>
                           {:xt/id :consumer/RJ29sUU
                            :consumer-id :RJ29sUU
                            :first-name "Jay"
                            :last-name "Rose"
                            :cover? true
                            :cover-type :Promotional}
                           #inst "2111-06-03"
                           #inst "2112-06-03"]])

    (t/is (= #{[true :Full]}
             (xt/q (xt/db *api* #inst "2115-07-03")
                   '{:find [cover type]
                     :where [[e :consumer-id :RJ29sUU]
                             [e :cover? cover]
                             [e :cover-type type]]})))

    (t/is (= #{}
             (xt/q (xt/db *api* #inst "2112-07-03")
                   '{:find [cover type]
                     :where [[e :consumer-id :RJ29sUU]
                             [e :cover? cover]
                             [e :cover-type type]]})))

    (t/is (= #{[true :Promotional]}
             (xt/q (xt/db *api* #inst "2111-07-03")
                   '{:find [cover type]
                     :where [[e :consumer-id :RJ29sUU]
                             [e :cover? cover]
                             [e :cover-type type]]})))
    (fix/submit+await-tx [[:xt/put
                           (assoc manifest :badges ["SETUP" "PUT" "DATALOG-QUERIES"
                                                    "BITEMP"])]])

    (t/is (= {:xt/id :manifest,
              :pilot-name "Johanna",
              :id/rocket "SB002-sol",
              :id/employee "22910x2",
              :badges ["SETUP" "PUT" "DATALOG-QUERIES" "BITEMP"],
              :cargo ["stereo" "gold fish" "slippers" "secret note"]}
             (xt/entity (xt/db *api*) :manifest))))

  (t/testing "saturn-tests"
    (put-all! [{:xt/id :gold-harmony
                :company-name "Gold Harmony"
                :seller? true
                :buyer? false
                :units/Au 10211
                :credits 51}

               {:xt/id :tombaugh-resources
                :company-name "Tombaugh Resources Ltd."
                :seller? true
                :buyer? false
                :units/Pu 50
                :units/N 3
                :units/CH4 92
                :credits 51}

               {:xt/id :encompass-trade
                :company-name "Encompass Trade"
                :seller? true
                :buyer? true
                :units/Au 10
                :units/Pu 5
                :units/CH4 211
                :credits 1002}

               {:xt/id :blue-energy
                :seller? false
                :buyer? true
                :company-name "Blue Energy"
                :credits 1000}])

    (fix/submit+await-tx [[:xt/match
                           :blue-energy
                           {:xt/id :blue-energy
                            :seller? false
                            :buyer? true
                            :company-name "Blue Energy"
                            :credits 1000}]
                          [:xt/put
                           {:xt/id :blue-energy
                            :seller? false
                            :buyer? true
                            :company-name "Blue Energy"
                            :credits 900
                            :units/CH4 10}]

                          [:xt/match
                           :tombaugh-resources
                           {:xt/id :tombaugh-resources
                            :company-name "Tombaugh Resources Ltd."
                            :seller? true
                            :buyer? false
                            :units/Pu 50
                            :units/N 3
                            :units/CH4 92
                            :credits 51}]
                          [:xt/put
                           {:xt/id :tombaugh-resources
                            :company-name "Tombaugh Resources Ltd."
                            :seller? true
                            :buyer? false
                            :units/Pu 50
                            :units/N 3
                            :units/CH4 82
                            :credits 151}]])
    (t/is (= ["Name: Tombaugh Resources Ltd., Funds: 151, :units/CH4 82"]
             (format-stock-check (stock-check :tombaugh-resources :units/CH4))))

    (t/is (= ["Name: Blue Energy, Funds: 900, :units/CH4 10"]
             (format-stock-check (stock-check :blue-energy :units/CH4))))
    (fix/submit+await-tx [[:xt/match
                           :gold-harmony
                           ;; Old doc
                           {:xt/id :gold-harmony
                            :company-name "Gold Harmony"
                            :seller? true
                            :buyer? false
                            :units/Au 10211
                            :credits 51}]
                          [:xt/put
                           ;; New doc
                           {:xt/id :gold-harmony
                            :company-name "Gold Harmony"
                            :seller? true
                            :buyer? false
                            :units/Au 211
                            :credits 51}]

                          [:xt/match
                           :encompass-trade
                           ;; Old doc
                           {:xt/id :encompass-trade
                            :company-name "Encompass Trade"
                            :seller? true
                            :buyer? true
                            :units/Au 10
                            :units/Pu 5
                            :units/CH4 211
                            :credits 100002}]
                          [:xt/put
                           ;; New doc
                           {:xt/id :encompass-trade
                            :company-name "Encompass Trade"
                            :seller? true
                            :buyer? true
                            :units/Au 10010
                            :units/Pu 5
                            :units/CH4 211
                            :credits 1002}]])

    (t/is (= '("Name: Gold Harmony, Funds: 51, :units/Au 10211")
             (format-stock-check (stock-check :gold-harmony :units/Au))))

    (t/is (= '("Name: Encompass Trade, Funds: 1002, :units/Au 10")
             (format-stock-check (stock-check :encompass-trade :units/Au))))

    (fix/submit+await-tx [[:xt/put (assoc manifest :badges ["SETUP" "PUT" "DATALOG-QUERIES" "BITEMP" "MATCH"])]])

    (t/is (= {:xt/id :manifest,
              :pilot-name "Johanna",
              :id/rocket "SB002-sol",
              :id/employee "22910x2",
              :badges ["SETUP" "PUT" "DATALOG-QUERIES" "BITEMP" "MATCH"],
              :cargo ["stereo" "gold fish" "slippers" "secret note"]}
             (xt/entity (xt/db *api*) :manifest)))))

(t/deftest jupiter-tests
  (let [doc1 {:xt/id :kaarlang/clients, :clients [:encompass-trade]}
        doc2 {:xt/id :kaarlang/clients, :clients [:encompass-trade :blue-energy]}
        doc3 {:xt/id :kaarlang/clients, :clients [:blue-energy]}
        doc4 {:xt/id :kaarlang/clients, :clients [:blue-energy :gold-harmony :tombaugh-resources]}]
    (fix/submit+await-tx [[:xt/put doc1 #inst "2110-01-01T09" #inst "2111-01-01T09"]
                          [:xt/put doc2 #inst "2111-01-01T09" #inst "2113-01-01T09"]
                          [:xt/put doc3 #inst "2113-01-01T09" #inst "2114-01-01T09"]
                          [:xt/put doc4 #inst "2114-01-01T09" #inst "2115-01-01T09"]])

    (t/is (= doc4 (xt/entity (xt/db *api* #inst "2114-01-01T09") :kaarlang/clients)))

    (t/is (= [{::xt/tx-id 0, ::xt/valid-time #inst "2110-01-01T09", ::xt/content-hash (c/hash-doc doc1), ::xt/doc doc1}
              {::xt/tx-id 0, ::xt/valid-time #inst "2111-01-01T09", ::xt/content-hash (c/hash-doc doc2), ::xt/doc doc2}
              {::xt/tx-id 0, ::xt/valid-time #inst "2113-01-01T09", ::xt/content-hash (c/hash-doc doc3), ::xt/doc doc3}
              {::xt/tx-id 0, ::xt/valid-time #inst "2114-01-01T09", ::xt/content-hash (c/hash-doc doc4), ::xt/doc doc4}
              {::xt/tx-id 0, ::xt/valid-time #inst "2115-01-01T09", ::xt/content-hash (c/new-id c/nil-id-buffer), ::xt/doc nil}]

             (->> (xt/entity-history (xt/db *api* #inst "2116-01-01T09") :kaarlang/clients :asc {:with-docs? true})
                  (mapv #(dissoc % ::xt/tx-time)))))

    (fix/submit+await-tx [[:xt/delete :kaarlang/clients #inst "2110-01-01" #inst "2116-01-01"]])

    (t/is nil? (xt/entity (xt/db *api* #inst "2114-01-01T09") :kaarlang/clients))

    (t/is (= [{::xt/tx-id 1, ::xt/valid-time #inst "2110-01-01T00", ::xt/content-hash (c/new-id c/nil-id-buffer)}
              {::xt/tx-id 1, ::xt/valid-time #inst "2110-01-01T09", ::xt/content-hash (c/new-id c/nil-id-buffer)}
              {::xt/tx-id 1, ::xt/valid-time #inst "2111-01-01T09", ::xt/content-hash (c/new-id c/nil-id-buffer)}
              {::xt/tx-id 1, ::xt/valid-time #inst "2113-01-01T09", ::xt/content-hash (c/new-id c/nil-id-buffer)}
              {::xt/tx-id 1, ::xt/valid-time #inst "2114-01-01T09", ::xt/content-hash (c/new-id c/nil-id-buffer)}
              {::xt/tx-id 1, ::xt/valid-time #inst "2115-01-01T09", ::xt/content-hash (c/new-id c/nil-id-buffer)}
              {::xt/tx-id 0, ::xt/valid-time #inst "2116-01-01T00", ::xt/content-hash (c/new-id c/nil-id-buffer)}]

             (->> (xt/entity-history (xt/db *api* #inst "2116-01-01T09") :kaarlang/clients :asc {:with-docs? false})
                  (mapv #(dissoc % ::xt/tx-time)))))))

(t/deftest Oumuamua-test
  (fix/submit+await-tx [[:xt/put {:xt/id :person/kaarlang
                                  :full-name "Kaarlang"
                                  :origin-planet "Mars"
                                  :identity-tag :KA01299242093
                                  :DOB #inst "2040-11-23"}]

                        [:xt/put {:xt/id :person/ilex
                                  :full-name "Ilex Jefferson"
                                  :origin-planet "Venus"
                                  :identity-tag :IJ01222212454
                                  :DOB #inst "2061-02-17"}]

                        [:xt/put {:xt/id :person/thadd
                                  :full-name "Thad Christover"
                                  :origin-moon "Titan"
                                  :identity-tag :IJ01222212454
                                  :DOB #inst "2101-01-01"}]

                        [:xt/put {:xt/id :person/johanna
                                  :full-name "Johanna"
                                  :origin-planet "Earth"
                                  :identity-tag :JA012992129120
                                  :DOB #inst "2090-12-07"}]])

  (t/is (= #{[{:xt/id :person/ilex,
               :full-name "Ilex Jefferson",
               :origin-planet "Venus",
               :identity-tag :IJ01222212454,
               :DOB #inst "2061-02-17T00:00:00.000-00:00"}]
             [{:xt/id :person/thadd,
               :full-name "Thad Christover",
               :origin-moon "Titan",
               :identity-tag :IJ01222212454,
               :DOB #inst "2101-01-01T00:00:00.000-00:00"}]
             [{:xt/id :person/kaarlang,
               :full-name "Kaarlang",
               :origin-planet "Mars",
               :identity-tag :KA01299242093,
               :DOB #inst "2040-11-23T00:00:00.000-00:00"}]
             [{:xt/id :person/johanna,
               :full-name "Johanna",
               :origin-planet "Earth",
               :identity-tag :JA012992129120,
               :DOB #inst "2090-12-07T00:00:00.000-00:00"}]}
           (full-query *api*)))

  (fix/submit+await-tx [[:xt/evict :person/kaarlang]]) ;; <1>
  (t/is empty? (full-query *api*))

  ;; Check not nil, history constantly changing so it is hard to check otherwise
  (with-open [history (xt/open-entity-history (xt/db *api*) :person/kaarlang :desc)]
    (t/is history))

  (with-open [history (xt/open-entity-history (xt/db *api*) :person/ilex :desc)]
    (t/is history))

  (with-open [history (xt/open-entity-history (xt/db *api*) :person/thadd :desc)]
    (t/is history))

  (with-open [history (xt/open-entity-history (xt/db *api*) :person/johanna :desc)]
    (t/is history)))
