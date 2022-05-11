(ns xtdb.http-server.json-test
  (:require [juxt.clojars-mirrors.clj-http.v3v12v2.clj-http.client :as http]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.fixtures :as fix]
            [xtdb.fixtures.http-server :as fh :refer [*api-url*]]
            [juxt.clojars-mirrors.jsonista.v0v3v5.jsonista.core :as json]))

(t/use-fixtures :each fh/with-http-server fix/with-node)

(defn json-get [{:keys [url qps http-opts]}]
  (-> (http/get (str *api-url* url)
                (merge
                 {:accept "application/json"
                  :content-type "application/json"
                  :query-params qps
                  :throw-exceptions false}
                 http-opts))
      :body
      (json/read-value)))

(defn submit-tx [body]
  (-> (http/post (str *api-url* "/_xtdb/submit-tx")
                 {:accept "application/json"
                  :content-type "application/json"
                  :as :stream
                  :body (json/write-value-as-string {"tx-ops" body})
                  :throw-exceptions true})
      :body
      (json/read-value)))

(t/deftest test-status
  (t/is (= "xtdb.mem_kv.MemKv" (get (json-get {:url "/_xtdb/status"}) "kvStore"))))

(t/deftest test-submit
  (let [{:strs [txId txTime] :as tx} (submit-tx [["put" {"xt/id" "test-person", "first-name" "George"}]])]
    (t/is (= 0 txId) "initial submit")
    (t/is (= tx
             (json-get {:url "/_xtdb/await-tx"
                        :qps {"txId" txId}})))
    (t/is (= {"txCommitted?" true}
             (json-get {:url "/_xtdb/tx-committed?"
                        :qps {"txId" txId}})))
    (t/is (= {"txTime" txTime}
             (json-get {:url "/_xtdb/await-tx-time"
                        :qps {"txTime" txTime}})))
    (t/is (= {"txTime" txTime}
             (json-get {:url "/_xtdb/sync"})))
    (t/is (= tx
             (json-get {:url "/_xtdb/latest-completed-tx"})))
    (t/is (= {"txId" txId}
             (json-get {:url "/_xtdb/latest-submitted-tx"})))

    (t/testing "/_xtdb/entity"
      (t/is (= {"xt/id" "test-person", "first-name" "George"}
               (json-get {:url "/_xtdb/entity"
                          :qps {"eid" "test-person"}})
               (json-get {:url "/_xtdb/entity"
                          :qps {"eidJson" (pr-str "test-person")}}))))))

(t/deftest test-query
  (let [{:strs [txId] :as tx} (submit-tx [["put" {"xt/id" "sal", "firstName" "Sally", "lastName" "Example"}]
                                          ["put" {"xt/id" "jed", "firstName" "Jed", "lastName" "Test"}]
                                          ["put" {"xt/id" "colin", "firstName" "Colin", "lastName" "Example"}]])]
    (t/is (= tx
             (json-get {:url "/_xtdb/await-tx"
                        :qps {"txId" txId}})))

    (t/is (= #{["sal"] ["jed"] ["colin"]}
             (set (json-get {:url "/_xtdb/query"
                             :qps {"queryEdn" (pr-str '{:find [e]
                                                        :where [[e :xt/id]]})}}))))
    (Thread/sleep 5) ;; wait for asynchronous bus event to update `!running-queries` in `xtdb.node`
    (t/is (= (pr-str '{:find [e]
                       :where [[e :xt/id]]})
             (-> (json-get {:url "/_xtdb/recent-queries"})
                 (get-in [0 "query"]))))
    (t/is (json-get
           {:url "/_xtdb/query"
            :qps {"queryEdn" (pr-str '{:find [e]
                                       :where [[e :xt/id]]})}}))
    (t/is (= #{["Sally"] ["Colin"]}
             (set
              (json-get
               {:url "/_xtdb/query"
                :qps {"queryEdn" (pr-str '{:find [first-name]
                                           :where [[e :firstName first-name]
                                                   [e :lastName "Example"]]})}}))))
    (t/is (= [[{"xt/id" "sal", "firstName" "Sally", "lastName" "Example"}]]
             (json-get
              {:url "/_xtdb/query"
               :qps {"queryEdn" (pr-str '{:find [(pull e [*])]
                                          :where [[e :firstName "Sally"]]})}})))

    (t/is (= [[{"xt/id" "sal", "firstName" "Sally", "lastName" "Example"}]
              [{"xt/id" "sal", "firstName" "Sally", "lastName" "Example"}]
              [{"xt/id" "sal", "firstName" "Sally", "lastName" "Example"}]]
             (json-get
              {:url "/_xtdb/query"
               :qps {"queryEdn" (pr-str '{:find [(pull e [*])]
                                          :in [n c k s [i ...] m]
                                          ;; :limit 1 ;; can use this to mitigate streaming open-q bag effect
                                          :where [[e :firstName n]
                                                  [(= c 123)]
                                                  [(= k :firstName)]
                                                  [(contains? s i)]
                                                  [(= m {:a {:b {:c :d :e "f"}}})]]})
                     "inArgsEdn" (pr-str ["Sally" 123 :firstName [0 1 2] [0 1 2] {:a {:b {:c :d :e "f"}}}])}})))

    (t/is (= [[{"xt/id" "sal", "firstName" "Sally", "lastName" "Example"}]
              [{"xt/id" "sal", "firstName" "Sally", "lastName" "Example"}]
              [{"xt/id" "sal", "firstName" "Sally", "lastName" "Example"}]]
             (json-get
              {:url "/_xtdb/query"
               :qps {"queryEdn" (pr-str '{:find [(pull e [*])]
                                          :in [n c k s [i ...] m]
                                          :where [[e :firstName n]
                                                  [(= c 123)]
                                                  [(= k "firstName")]
                                                  [(contains? s i)]
                                                  [(= m {:a {:b {:c "d" :e "f"}}})]]})
                     "inArgsJson" (json/write-value-as-string ["Sally" 123 "firstName" [0 1 2] [0 1 2] {"a" {"b" {"c" "d" "e" "f"}}}])}})))

    (t/testing "pull"
      (let [{:strs [txId] :as tx} (submit-tx [["put" {"xt/id" "link", "linking" "jed"}]])]
        (t/is (= tx
                 (json-get {:url "/_xtdb/await-tx"
                            :qps {"txId" txId}})))
        (t/is (= [[{"xt/id" "sal" "firstName" "Sally", "lastName" "Example"}]]
                 (json-get {:url "/_xtdb/query"
                            :qps {"queryEdn" (pr-str '{:find [(pull e [*])]
                                                       :where [[e :firstName "Sally"]]})}})))
        (t/is (= [[{"firstName" "Jed", "lastName" "Test"}]]
                 (json-get {:url "/_xtdb/query"
                            :qps {"queryEdn" (pr-str '{:find [(pull e [:firstName :lastName])]
                                                       :where [[e :firstName "Jed"]]})}})))
        (t/is (= [[{"linking" {"firstName" "Jed", "lastName" "Test"}}]]
                 (json-get {:url "/_xtdb/query"
                            :qps {"queryEdn" (pr-str '{:find [(pull e [{:linking [:firstName :lastName]}])]
                                                       :where [[e :linking linking]]})}})))))))

(t/deftest test-history
  (let [{:strs [txTime] :as tx} (submit-tx [["put" {"xt/id" "test-person", "first-name" "George"}]])
        tx2 (submit-tx [["put" {"xt/id" "test-person", "first-name" "george"} "2020-09-30T20:05:50Z"]])
        {:strs [txId] :as tx3} (submit-tx [["put" {"xt/id" "test-person", "firstName" "George"}]])]
    (t/is (= 1 (get tx2 "txId")) "put with valid-time")
    (t/is (= 2 (get tx3 "txId")))
    (t/is (= tx3
             (json-get {:url "/_xtdb/await-tx", :qps {"txId" txId}})))
    (t/testing "/_xtdb/entity?history=true"
      (let [entity-history (json-get {:url "/_xtdb/entity"
                                      :qps {"eidJson" (pr-str "test-person")
                                            "history" true
                                            "sortOrder" "asc"
                                            "withDocs" true}})]
        (t/is (= 3 (count entity-history)))
        (t/is (= txTime (get-in entity-history [1 "txTime"])))
        (t/is (= {"xt/id" "test-person", "first-name" "george"} (get-in entity-history [0 "doc"])))
        (t/is (= {"xt/id" "test-person", "first-name" "George"} (get-in entity-history [1 "doc"])))
        (t/is (= {"xt/id" "test-person", "firstName" "George"} (get-in entity-history [2 "doc"])))))

    (t/testing "/_xtdb/tx-log"
      (let [tx-log (json-get {:url "/_xtdb/tx-log"
                              :qps {"with-ops?" true}})]
        (t/is (= 3 (count tx-log)))
        (t/is (= (assoc tx "txOps" [["put" {"xt/id" "test-person", "first-name" "George"}]]) (get tx-log 0)))
        (t/is (= (assoc tx2 "txOps" [["put" {"xt/id" "test-person", "first-name" "george"} "2020-09-30T20:05:50Z"]]) (get tx-log 1)))
        (t/is (= (assoc tx3 "txOps" [["put" {"xt/id" "test-person", "firstName" "George"}]]) (get tx-log 2)))))))

(t/deftest test-delete
  (submit-tx [["put" {"xt/id" "test-person", "firstName" "George"}]])
  (let [{:strs [txId] :as tx} (submit-tx [["delete" "test-person"]])]
    (t/is (= tx
             (json-get {:url "/_xtdb/await-tx"
                        :qps {"txId" txId}})))
    (t/is (= {"error" "test-person entity not found"}
             (json-get {:url "/_xtdb/entity"
                        :qps {"eid" "test-person"}})))))

(t/deftest test-match
  (submit-tx [["put" {"xt/id" "test-person", "firstName" "George"}]])
  (let [{:strs [txId txTime] :as tx} (submit-tx [["match" "test-person" {"xt/id" "test-person", "firstName" "George"}]
                                                 ["put" {"xt/id" "test-person", "firstName" "George2"}]])]
    (t/is (= tx
             (json-get {:url "/_xtdb/await-tx?timeout=1000"
                        :qps {"txId" txId}})))
    (t/is (= {"xt/id" "test-person", "firstName" "George2"}
             (json-get {:url "/_xtdb/entity"
                        :qps {"eid" "test-person"}})))))

(t/deftest test-evict
  (submit-tx [["put" {"xt/id" "test-person", "firstName" "George"}]])
  (let [{:strs [txId] :as tx} (submit-tx [["evict" "test-person"]])]
    (t/is (= tx
             (json-get {:url "/_xtdb/await-tx"
                        :qps {"txId" txId}})))
    (t/is (= []
             (json-get {:url "/_xtdb/entity"
                        :qps {"eidJson" (pr-str "test-person")
                              "history" true
                              "sortOrder" "asc"
                              "withDocs" true}})))))

(t/deftest test-transaction-functions
  (let [tx-fn (pr-str '(fn [ctx eid]
                         (let [db (xtdb.api/db ctx)
                               entity (xtdb.api/entity db eid)]
                           [[::xt/put (update entity :age inc)]])))
        {:strs [txId] :as tx} (submit-tx [["put" {"xt/id" "increment-age", "xt/fn" tx-fn}]])]
    (t/is (= tx
             (json-get {:url "/_xtdb/await-tx"
                        :qps {"txId" txId}})))
    (t/is (= {"xt/id" "increment-age", "xt/fn" tx-fn}
             (json-get {:url "/_xtdb/entity"
                        :qps {"eid" "increment-age"}}))))

  (let [{:strs [txId] :as tx} (submit-tx [["put" {"xt/id" "ivan", "age" 21}]])]
    (t/is (= tx
             (json-get {:url "/_xtdb/await-tx"
                        :qps {"txId" txId}})))
    (t/is (= {"xt/id" "ivan" "age" 21}
             (json-get {:url "/_xtdb/entity"
                        :qps {"eid" "ivan"}}))))

  (let [{:strs [txId] :as tx} (submit-tx [["fn" "increment-age" "ivan"]])]
    (t/is (= tx
             (json-get {:url "/_xtdb/await-tx"
                        :qps {"txId" txId}})))
    (t/is (= {"txCommitted?" true}
             (json-get {:url "/_xtdb/tx-committed?"
                        :qps {"txId" txId}})))
    (t/is (= {"xt/id" "ivan" "age" 22}
             (json-get {:url "/_xtdb/entity"
                        :qps {"eidJson" (pr-str "ivan")}})))))

(t/deftest test-object-mapping
  (fix/submit+await-tx [[::xt/put {:xt/id "foo"
                                  :bytes (byte-array [1 2 3])}]])
  (t/is (= {"xt/id" "foo"
            "bytes" "AQID"}
           (json-get {:url "/_xtdb/entity"
                      :qps {"eid" "foo"}}))))
