(ns xtdb.secondary-index-test
  (:require [clojure.string :as str]
            [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.io DataInputStream DataOutputStream)
           (java.net InetAddress Socket)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (org.postgresql.util PSQLException)
           (xtdb.api TableRef Xtdb Xtdb$Connection)
           (xtdb.api.storage ObjectStore$StoredObject)
           (xtdb.database Database Database$Config)
           (xtdb.index IndexState$Building IndexState$Dropped IndexState$Ready IndexTrie SecondaryIndex)))

(t/use-fixtures :each tu/with-node)

(defn- indices [^Database db]
  (->> (.getIndices (.getIndexCatalog (.getQueryState db)))
       (map (fn [^SecondaryIndex index]
              (let [index-def (.getDef index)
                    table (.getTable index-def)
                    state (.getState index)]
                {:name (.getName index-def)
                 :table [(.getSchemaName table) (.getTableName table)]
                 :column (.getColumn index-def)
                 :state (condp instance? state
                          IndexState$Building :building
                          IndexState$Ready :ready
                          IndexState$Dropped :dropped)})))
       (sort-by (juxt :name :state))))

(defn- await-indices
  "The database's indices, once they equal `expected` or after 5s: Ready reaches a node's catalog behind the declaring transaction, not with it."
  [db expected]
  (let [deadline (+ (System/currentTimeMillis) 5000)]
    (loop []
      (let [actual (indices db)]
        (if (or (= expected actual) (> (System/currentTimeMillis) deadline))
          actual
          (do (Thread/sleep 10) (recur)))))))

(t/deftest create-and-drop-index
  (jdbc/execute! tu/*node* ["CREATE INDEX by_email ON users (email)"])

  (let [expected [{:name "by_email", :table ["public" "users"], :column "email", :state :ready}]]
    (t/is (= expected (await-indices (db/primary-db tu/*node*) expected))
          "an absent table and column are accepted, and the index, owing no blocks, becomes Ready"))

  (t/is (thrown-with-msg? PSQLException #"Index already exists"
                          (jdbc/execute! tu/*node* ["CREATE INDEX by_email ON orders (email)"])))

  (jdbc/execute! tu/*node* ["DROP INDEX by_email"])

  (t/is (thrown-with-msg? PSQLException #"Index does not exist"
                          (jdbc/execute! tu/*node* ["DROP INDEX by_email"])))

  (jdbc/execute! tu/*node* ["CREATE INDEX by_email ON crm.users (email)"])

  (let [expected [{:name "by_email", :table ["public" "users"], :column "email", :state :dropped}
                  {:name "by_email", :table ["crm" "users"], :column "email", :state :ready}]]
    (t/is (= expected (await-indices (db/primary-db tu/*node*) expected))
          "the dropped index stays in the catalog, and its name is free to declare again")))

(t/deftest an-index-on-a-column-no-document-holds-is-rejected
  (with-open [conn (.connect ^Xtdb tu/*node*)]
    (doseq [col ["_iid" "_valid_from" "_valid_to" "_system_from" "_system_to"]]
      (t/is (anomalous? [:incorrect :xtdb/index-on-system-column nil]
                        (.createIndex conn "by_col" (TableRef. "public" "users") col))
            col)))

  (t/is (thrown-with-msg? PSQLException #"Cannot index column '_valid_from'"
                          (jdbc/execute! tu/*node* ["CREATE INDEX by_vf ON users (_valid_from)"])))

  (jdbc/execute! tu/*node* ["CREATE INDEX by_id ON users (_id)"])

  (t/is (= ["by_id"] (map :name (indices (db/primary-db tu/*node*))))
        "`_id` is a document's column, so it takes an index"))

(t/deftest index-belongs-to-the-connections-database
  (jdbc/execute! tu/*node* ["ATTACH DATABASE new_db"])

  (with-open [new-db-conn (.build (-> (.createConnectionBuilder tu/*node*) (.database "new_db")))]
    (jdbc/execute! new-db-conn ["CREATE INDEX by_email ON users (email)"]))

  (t/is (= [] (indices (db/primary-db tu/*node*))))
  (t/is (= ["by_email"] (map :name (indices (.databaseOrNull (db/<-node tu/*node*) "new_db"))))))

(defn- simple-query
  [^Xtdb node ^String sql]
  (letfn [(write-msg [^DataOutputStream out type ^bytes body]
            (when type (.writeByte out (int type)))
            (.writeInt out (+ 4 (alength body)))
            (.write out body)
            (.flush out))
          (cstring [^String s]
            (.getBytes (str s "\u0000") StandardCharsets/UTF_8))
          (read-until-ready [^DataInputStream in]
            (loop [errors []]
              (let [type (char (.readByte in))
                    body (byte-array (- (.readInt in) 4))]
                (.readFully in body)
                (case type
                  \Z errors
                  \E (recur (conj errors (->> (str/split (String. body StandardCharsets/UTF_8) #"\u0000")
                                              (some #(when (str/starts-with? % "M") (subs % 1))))))
                  (recur errors)))))]
    (with-open [socket (Socket. (InetAddress/getLoopbackAddress) (int (.getServerPort node)))]
      (let [out (DataOutputStream. (.getOutputStream socket))
            in (DataInputStream. (.getInputStream socket))]
        (write-msg out nil (let [^bytes params (cstring (str "user\u0000xtdb\u0000database\u0000xtdb\u0000"))]
                             (-> (ByteBuffer/allocate (+ 4 (alength params))) (.putInt 196608) (.put params) (.array))))
        (read-until-ready in)
        (write-msg out \Q (cstring sql))
        (let [errors (read-until-ready in)]
          (write-msg out \X (byte-array 0))
          errors)))))

(def ^:private ddl-kinds
  {:attach {:sql #(str "ATTACH DATABASE " %)
            :conn-op (fn [^Xtdb$Connection conn n] (.attachDb conn n (Database$Config.)))
            :applied? #(some? (.databaseOrNull (db/<-node tu/*node*) ^String %))}

   :create-index {:sql #(str "CREATE INDEX " % " ON docs (email)")
                  :conn-op (fn [^Xtdb$Connection conn n] (.createIndex conn n (TableRef. "public" "docs") "email"))
                  :applied? (fn [n] (boolean (some #{n} (map :name (indices (db/primary-db tu/*node*))))))}

   :detach {:setup #(jdbc/execute! tu/*node* [(str "ATTACH DATABASE " %)])
            :sql #(str "DETACH DATABASE " %)
            :conn-op (fn [^Xtdb$Connection conn n] (.detachDb conn n))
            :applied? #(nil? (.databaseOrNull (db/<-node tu/*node*) ^String %))}

   :drop-index {:setup #(jdbc/execute! tu/*node* [(str "CREATE INDEX " % " ON docs (email)")])
                :sql #(str "DROP INDEX " %)
                :conn-op (fn [^Xtdb$Connection conn n] (.dropIndex conn n))
                :applied? (fn [n] (not-any? (fn [{index-name :name, :keys [state]}]
                                              (and (= n index-name) (not= :dropped state)))
                                            (indices (db/primary-db tu/*node*))))}})

(defn- ddl-outcome
  [ddl-kind scenario insert-id]
  (let [{:keys [setup sql conn-op applied?]} (ddl-kinds ddl-kind)
        n (str/replace (str (name ddl-kind) "_" (name scenario)) "-" "_")
        _ (when setup (setup n))
        insert (str "INSERT INTO docs RECORDS {_id: " insert-id "}")
        ddl (sql n)
        error (try
                (case scenario
                  :simple-ddl-alone (first (simple-query tu/*node* ddl))
                  :simple-insert-then-ddl (first (simple-query tu/*node* (str insert "; " ddl)))
                  :simple-ddl-then-insert (first (simple-query tu/*node* (str ddl "; " insert)))
                  :simple-explicit-tx (first (simple-query tu/*node* (str "BEGIN READ WRITE; " insert "; " ddl "; COMMIT")))
                  :extended-ddl-alone (do (jdbc/execute! tu/*node* [ddl]) nil)
                  :extended-insert-then-ddl-before-sync (do (jdbc/execute! tu/*node* [(str insert "; " ddl)]) nil)
                  :extended-explicit-tx (with-open [conn (jdbc/get-connection tu/*node*)]
                                          (jdbc/execute! conn ["BEGIN READ WRITE"])
                                          (try
                                            (jdbc/execute! conn [insert])
                                            (jdbc/execute! conn [ddl])
                                            (jdbc/execute! conn ["COMMIT"])
                                            nil
                                            (finally
                                              (jdbc/execute! conn ["ROLLBACK"]))))
                  :connection-open-tx (with-open [conn (.connect ^Xtdb tu/*node*)]
                                        (.begin conn)
                                        (try
                                          (with-open [stmt (.createStatement conn ^String insert)]
                                            (.executeUpdate stmt))
                                          (conn-op conn n)
                                          (.commitSync conn)
                                          nil
                                          (finally
                                            (when (.isTxOpen conn) (.rollbackTx conn))))))
                (catch Exception e (ex-message e)))]
    (xt-log/sync-node tu/*node* #xt/duration "PT5S")
    {:error (some-> error
                    (str/replace #"^ERROR: " "")
                    str/split-lines first
                    (str/replace #"attach a database|detach a database|create an index|drop an index" "<ddl>"))
     :applied? (applied? n)
     :inserted? (= [{:xt/id insert-id}] (xt/q tu/*node* ["SELECT _id FROM docs WHERE _id = ?" insert-id]))}))

(t/deftest index-ddl-behaves-as-attach-and-detach-do-in-each-transaction-scenario
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 0, :email "ada@example.com"}]])

  (let [!insert-id (atom 0)
        scenarios [:simple-ddl-alone :simple-insert-then-ddl :simple-ddl-then-insert :simple-explicit-tx
                   :extended-ddl-alone :extended-insert-then-ddl-before-sync :extended-explicit-tx
                   :connection-open-tx]
        outcomes (fn [ddl-kind]
                   (into {} (for [scenario scenarios]
                              [scenario (ddl-outcome ddl-kind scenario (swap! !insert-id inc))])))
        attach (outcomes :attach)
        detach (outcomes :detach)]

    (t/is (= {:simple-ddl-alone {:error nil, :applied? true, :inserted? false}
              :simple-insert-then-ddl {:error nil, :applied? true, :inserted? true}
              :simple-ddl-then-insert {:error nil, :applied? true, :inserted? true}
              :simple-explicit-tx {:error "Cannot <ddl> in a transaction.", :applied? false, :inserted? false}
              :extended-ddl-alone {:error nil, :applied? true, :inserted? false}
              :extended-insert-then-ddl-before-sync {:error nil, :applied? true, :inserted? true}
              :extended-explicit-tx {:error "Cannot <ddl> in a transaction.", :applied? false, :inserted? false}
              :connection-open-tx {:error nil, :applied? true, :inserted? true}}
             attach)
          "ATTACH runs in an implicit transaction and beside an open `Connection` transaction, and is refused only in an explicit one")

    (t/is (= detach attach))
    (t/is (= attach (outcomes :create-index)))
    (t/is (= detach (outcomes :drop-index)))))

(t/deftest restarted-node-holds-the-indices-its-peers-hold
  (util/with-tmp-dirs #{path}
    (let [node-opts {:log [:local {:path (.resolve path "log")}]
                     :storage [:local {:path (.resolve path "objects")}]
                     :compactor {:threads 0}}]
      (with-open [node (xtn/start-node node-opts)]
        (jdbc/execute! node ["INSERT INTO users RECORDS {_id: 1, email: 'ada@example.com'}"])
        (jdbc/execute! node ["CREATE INDEX by_email ON users (email)"])
        (jdbc/execute! node ["CREATE INDEX by_sku ON orders (sku)"])
        (jdbc/execute! node ["CREATE INDEX by_name ON users (name)"])
        (jdbc/execute! node ["DROP INDEX by_name"])
        (jdbc/execute! node ["CREATE INDEX by_total ON orders (total)"])
        (tu/flush-block! node)

        (t/is (nil? (.rowCount (.snap (.getTableCatalog (db/primary-db node))) (TableRef. "public" "orders")))
              "a table only indices name stays out of the table catalog's tables")

        (jdbc/execute! node ["DROP INDEX by_total"]))

      (with-open [node (xtn/start-node node-opts)]
        (xt-log/sync-node node #xt/duration "PT5S")

        (t/is (nil? (.rowCount (.snap (.getTableCatalog (db/primary-db node))) (TableRef. "public" "orders")))
              "and stays out of them across a restart, though its block file holds its indices")

        (let [expected [{:name "by_email", :table ["public" "users"], :column "email", :state :ready}
                        {:name "by_name", :table ["public" "users"], :column "name", :state :dropped}
                        {:name "by_sku", :table ["public" "orders"], :column "sku", :state :ready}
                        {:name "by_total", :table ["public" "orders"], :column "total", :state :dropped}]]
          (t/is (= expected (await-indices (db/primary-db node) expected))
                "the block's indices, including one on a table no block has written, with the drop after it replayed onto them"))

        (t/is (thrown-with-msg? PSQLException #"Index already exists"
                                (jdbc/execute! node ["CREATE INDEX by_email ON users (email)"])))

        (t/is (thrown-with-msg? PSQLException #"Index already exists"
                                (jdbc/execute! node ["CREATE INDEX by_sku ON orders (sku)"])))

        (jdbc/execute! node ["DROP INDEX by_email"])

        (t/is (= :dropped (->> (indices (db/primary-db node))
                               (filter (comp #{"by_email"} :name))
                               first :state)))))))

(t/deftest only-an-index-owing-no-blocks-becomes-ready
  (util/with-tmp-dirs #{path}
    (let [node-opts {:log [:local {:path (.resolve path "log")}]
                     :storage [:local {:path (.resolve path "objects")}]
                     :compactor {:threads 0}}
          expected [{:name "by_email", :table ["public" "users"], :column "email", :state :building}
                    {:name "by_name", :table ["public" "people"], :column "name", :state :ready}
                    {:name "by_sku", :table ["public" "orders"], :column "sku", :state :ready}]]

      (with-open [node (xtn/start-node node-opts)
                  follower (-> (xtn/->config node-opts) (.readOnlyDatabases true) (.open))]
        (jdbc/execute! node ["INSERT INTO users RECORDS {_id: 1, email: 'ada@example.com'}"])
        (tu/flush-block! node)
        (jdbc/execute! node ["INSERT INTO orders RECORDS {_id: 1, sku: 'X-1'}"])

        (jdbc/execute! node ["CREATE INDEX by_email ON users (email)"])
        (jdbc/execute! node ["CREATE INDEX by_sku ON orders (sku)"])
        (jdbc/execute! node ["CREATE INDEX by_name ON people (name)"])

        (t/is (= expected (await-indices (db/primary-db node) expected))
              "Ready on a table first written in the open block and on one never written; Building on one with a flushed block")

        (tu/flush-block! node)

        (xt-log/sync-node follower #xt/duration "PT5S")
        (t/is (= expected (await-indices (db/primary-db follower) expected))
              "a follower holds the leader's decision"))

      (with-open [node (xtn/start-node node-opts)]
        (xt-log/sync-node node #xt/duration "PT5S")
        (t/is (= expected (await-indices (db/primary-db node) expected))
              "a restarted node holds the same states")))))

(defn- index-tries
  "Each index's tries, by name, with the files under its directory."
  [^Database db]
  (let [files (->> (.listAllObjects (.getBufferPool db) (util/->path "tables"))
                   (map (comp str #(.getKey ^ObjectStore$StoredObject %)))
                   (filter #(str/includes? % "/indices/")))]
    (->> (.getIndices (.getIndexCatalog (.getQueryState db)))
         (into {} (map (fn [^SecondaryIndex index]
                         (let [index-id (str (.getId (.getDef index)))]
                           [(.getName (.getDef index))
                            {:tries (mapv #(str (.getKey ^IndexTrie %)) (.getTries index))
                             :files (->> files
                                         (filter #(str/includes? % index-id))
                                         (map #(str/replace % index-id "<id>"))
                                         sort vec)}])))))))

(t/deftest flushed-blocks-write-index-l0s
  (util/with-tmp-dirs #{path}
    (let [node-opts {:log [:local {:path (.resolve path "log")}]
                     :storage [:local {:path (.resolve path "objects")}]
                     :compactor {:threads 0}}
          expected {"by_email" {:tries ["l00-rc-b00"]
                                :files ["tables/public$users/indices/<id>/data/l00-rc-b00.arrow"
                                        "tables/public$users/indices/<id>/meta/l00-rc-b00.arrow"]}
                    "by_name" {:tries [], :files []}
                    "by_sku" {:tries ["l00-rc-b01"]
                              :files ["tables/public$orders/indices/<id>/data/l00-rc-b01.arrow"
                                      "tables/public$orders/indices/<id>/meta/l00-rc-b01.arrow"]}}]

      (with-open [node (xtn/start-node node-opts)
                  follower (-> (xtn/->config node-opts) (.readOnlyDatabases true) (.open))]
        (jdbc/execute! node ["CREATE INDEX by_email ON users (email)"])
        (jdbc/execute! node ["CREATE INDEX by_name ON users (name)"])
        (jdbc/execute! node ["DROP INDEX by_name"])
        (jdbc/execute! node ["CREATE INDEX by_sku ON orders (sku)"])

        (jdbc/execute! node ["INSERT INTO users RECORDS {_id: 1, email: 'ada@example.com', name: 'Ada'}, {_id: 2, email: NULL}"])
        (jdbc/execute! node ["INSERT INTO orders RECORDS {_id: 1, total: 3}"])
        (tu/flush-block! node)

        (t/is (= {"by_email" (get expected "by_email")
                  "by_name" {:tries [], :files []}
                  "by_sku" {:tries [], :files []}}
                 (index-tries (db/primary-db node)))
              "an L0 for the Building index whose column a put gave a value, and none for a dropped index or for a block with no such put")

        (jdbc/execute! node ["INSERT INTO users RECORDS {_id: 3, name: 'Carol'}"])
        (jdbc/execute! node ["INSERT INTO orders RECORDS {_id: 2, sku: 'X-1'}"])
        (tu/flush-block! node)

        (t/is (= expected (index-tries (db/primary-db node))))

        (xt-log/sync-node follower #xt/duration "PT5S")
        (t/is (= expected (index-tries (db/primary-db follower)))
              "a follower adopts the same index tries off the replica log"))

      (with-open [node (xtn/start-node node-opts)]
        (xt-log/sync-node node #xt/duration "PT5S")
        (t/is (= expected (index-tries (db/primary-db node)))
              "a restarted node reloads the index tries from the latest block")))))
