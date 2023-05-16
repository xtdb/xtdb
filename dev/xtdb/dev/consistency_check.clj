(ns xtdb.dev.consistency-check
  "Tool designed to provoke non-deterministic transaction processing (and eventual index inconsistency)
   by simulating different environmental conditions (such as flaking storage, concurrency and node restarts).

   Try (print-test {}) for a quick test."
  (:require [clojure.java.shell :as sh]
            [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.cache :as cache]
            [xtdb.db :as db]
            [xtdb.io :as xio]
            [xtdb.kv.document-store]
            [xtdb.kv.tx-log]
            [xtdb.mem-kv]
            [xtdb.tx.subscribe :as tx-sub])
  (:refer-clojure :exclude [rand-int])
  (:import (com.google.common.collect MinMaxPriorityQueue)
           (java.io Closeable)
           (java.time Duration)
           (java.util Comparator Random)
           (java.util.concurrent Executors TimeUnit)
           (xtdb.api ICursor)))

;; Definitions:

;; st
;; a map of id (:xt/id) to doc, the contents of the database at a basis time point.

;; tx
;; an XTDB transaction

;; transition
;; a transaction, and its expected outcome when applied to some state.
;; represented with a map, with the keys: :prev-st, :tx, :new-st

;; model
;; data structure with a :transition-fn from (st, rng) -> transition
;; this allows one to generate infinite sequences of transactions against a state model, and
;; the document state at each transaction if the log is applied sequentially.

(defn- rand-get
  "Like rand-nth but uses *rng* and accepts empty colls (returning nil)."
  [^Random rng coll]
  (when (seq coll)
    (let [i (.nextInt rng (count coll))]
      (nth coll i))))

(defn- rand-int
  ([^Random rng] (.nextInt rng))
  ([^Random rng n] (.nextInt rng n)))

(defn- weighted-sample-fn
  "Aliased random sampler:

  https://www.peterstefek.me/alias-method.html

  Given a seq of [item weight] pairs, return a function who when given a Random will return an item according to the weight distribution."
  [weighted-items]
  (case (count weighted-items)
    0 (constantly nil)
    1 (constantly (ffirst weighted-items))
    (let [total (reduce + (map second weighted-items))
          normalized-items (mapv (fn [[item weight]] [item (double (/ weight total))]) weighted-items)
          len (count normalized-items)
          pq (doto (.create (MinMaxPriorityQueue/orderedBy ^Comparator (fn [[_ w] [_ w2]] (compare w w2)))) (.addAll normalized-items))
          avg (/ 1.0 len)
          parts (object-array len)
          epsilon 0.00001]
      (dotimes [i len]
        (let [[smallest small-weight] (.pollFirst pq)
              overfill (- avg small-weight)]
          (if (< epsilon overfill)
            (let [[largest large-weight] (.pollLast pq)
                  new-weight (- large-weight overfill)]
              (when (< epsilon new-weight)
                (.add pq [largest new-weight]))
              (aset parts i [small-weight smallest largest]))
            (aset parts i [small-weight smallest smallest]))))
      ^{:table parts}
      (fn sample-weighting
        ([^Random random]
         (let [i (.nextInt random len)
               [split small large] (aget parts i)]
           (if (<= (/ (.nextDouble random) len) (double split)) small large)))))))

(defn put-transition [st & docs]
  {:prev-st st
   :tx (mapv (fn [doc] [::xt/put doc]) docs)
   :new-st (reduce #(assoc %1 (:xt/id %2) %2) st docs)})

(defn counter-model
  "Returns a model (for use in test-model or print-test) that describes
  transactions over some set of counters.

  Includes nesting dependent read sequences including matches and conditional tx-fns.

  Options:

  :counters default 4
  The number of distinct counters we should transact against

  :non-determinism
  a weighting that increases the likely-hood of an intentionally non-deterministic tx-fn being introduced.
  At 0.0, the generated transactions will be entirely deterministic.

  :seed (long)
  the rng seed used for new transactions via transition-seq."
  [{:keys [counters
           non-determinism
           seed]
    :or {counters 4
         non-determinism 0.0
         seed 8154811973970406745}}]
  (let [tx-fns [{:xt/id :incr
                 :xt/fn '(fn [ctx ctr]
                           (let [doc (xtdb.api/entity (xtdb.api/db ctx) ctr)]
                             [[::xt/put {:xt/id ctr, :n (inc (:n doc 0))}]]))}
                {:xt/id :set
                 :xt/fn '(fn [ctx ctr n]
                           [[::xt/put {:xt/id ctr, :n n}]])}
                {:xt/id :cas
                 :xt/fn '(fn [ctx ctr n tx]
                           (let [doc (xtdb.api/entity (xtdb.api/db ctx) ctr)]
                             (if (= n (:n doc))
                               tx
                               [])))}
                {:xt/id :flip
                 :xt/fn '(fn [ctx tx]
                           (if (< 0.5 (rand))
                             tx
                             []))}]
        depth-sampler
        (memoize
          (fn [depth]
            (weighted-sample-fn
              (remove (fn [[_ weight]] (<= weight 0.0))
                      {:match-hit 0.2
                       :match-miss (max 0.0 (- 0.01 (* 0.005 depth)))
                       :conditional-hit 0.1
                       :conditional-miss (max 0.0 (- 0.01 (* 0.005 depth)))
                       :delete 0.01
                       :put 0.01
                       :set 0.02
                       :incr 0.5
                       :maybe-non-deterministic (max non-determinism 0.0)}))))
        sample-miss-n-or-nil (weighted-sample-fn {-1 1.0, 1 0.5, nil 0.1})]
    {:short-desc (format "counters %s (non-determinism %s)" counters non-determinism)
     :init (apply put-transition {} tx-fns)
     :transition-fn
     (fn [st ^Random rng]
       (letfn [(sample-op [depth] ((depth-sampler depth) rng))
               (sample-miss-n [] (or (sample-miss-n-or-nil rng) (rand-int rng)))
               (sample-ctr [] (rand-int rng counters))
               (get-doc [st ctr] (or (st ctr) {:xt/id ctr, :n 0}))
               (sample-miss [st ctr] (update (get-doc st ctr) :n + (sample-miss-n)))
               (next-tx
                 ([st depth] (next-tx st depth (max 0 (- (inc (rand-int rng 10)) (* 2 depth)))))
                 ([st depth n]
                  (assert (nat-int? n))
                  (let [o-st st]
                    (loop [st st
                           aborts false
                           ops []
                           i 0]
                      (if (= i n)
                        [(if aborts o-st st) ops aborts]
                        (let [[new-st op now-aborts] (next-op st depth)
                              aborts (or now-aborts aborts)]
                          (recur (if aborts st new-st) aborts (conj ops op) (inc i))))))))
               (next-op [st depth]
                 (let [op (sample-op depth)]
                   (case op
                     :maybe-non-deterministic
                     (if (pos? non-determinism)
                       [st [::xt/fn :flip (next-tx st (inc depth))]]
                       (recur st depth))

                     :match-hit
                     (let [ctr (sample-ctr)
                           doc (st ctr)]
                       (if doc
                         [st ^{:hit true} [::xt/match ctr doc] false]
                         (next-op st depth)))

                     :match-miss
                     (let [ctr (sample-ctr)
                           miss-doc (sample-miss st ctr)]
                       [st ^{:hit false} [::xt/match ctr miss-doc] true])

                     :conditional-hit
                     (let [ctr (sample-ctr)
                           doc (st ctr)]
                       (if doc
                         (let [[new-st tx aborts] (next-tx st (inc depth))]
                           [(if aborts st new-st) ^{:hit true} [::xt/fn :cas ctr (:n doc) tx] aborts])
                         (recur st depth)))

                     :conditional-miss
                     (let [ctr (sample-ctr)
                           doc (sample-miss st ctr)
                           [_ tx] (next-tx st (inc depth))]
                       [st ^{:hit false} [::xt/fn :cas ctr (:n doc) tx] false])

                     :delete
                     (let [ctr (sample-ctr)]
                       [(dissoc st ctr) [::xt/delete ctr] false])

                     :put
                     (let [ctr (sample-ctr)
                           n (rand-int rng)
                           doc {:xt/id ctr, :n n}]
                       [(assoc st ctr doc) [::xt/put doc] false])

                     :set
                     (let [ctr (sample-ctr)
                           n (rand-int rng)
                           doc {:xt/id ctr, :n n}]
                       [(assoc st ctr doc) [::xt/fn :set ctr n] false])

                     :incr
                     (let [ctr (sample-ctr)
                           doc (get-doc st ctr)
                           new-doc (update doc :n inc)]
                       [(assoc st ctr new-doc) [::xt/fn :incr ctr] false]))))]
         (let [[new-st tx] (next-tx st 0)]
           {:prev-st st
            :tx tx
            :new-st new-st})))
     :seed seed}))

(defn transition-seq [model]
  (let [{:keys [init, transition-fn, seed]} model
        rng (Random. seed)]
    (iterate (fn [{:keys [new-st]}] (transition-fn new-st rng)) init)))

(defn tx-seq [model] (map :tx (transition-seq model)))

(defn state-seq [model] (map :prev-st (transition-seq model)))

(defn state-at [model index] (nth (state-seq model) index))

(defn- update-vals-backport [m f] (persistent! (reduce-kv #(assoc! %1 %2 (f %3)) (transient {}) m)))

(defn- present-state
  "Sorts maps in docs for easier visual consumption."
  [x]
  (cond
    (map? x)
    (update-vals-backport
      (into (sorted-map-by
              (let [sort-key (juxt #(.getName (class %)) identity)]
                (fn [a b] (compare (sort-key a) (sort-key b)))))
            x)
      present-state)
    (coll? x) (into (empty x) (map present-state) x)
    :else x))

(defn query-state [node]
  (let [db (xt/db node)
        tuples (xt/q db '{:find [e] :where [[e :xt/id]]})]
    (->> (map (comp (partial xt/entity db) first) tuples)
         (reduce (fn [m {id :xt/id :as doc}] (assoc m id doc)) {}))))

(def noop-model
  {:short-desc "noop"
   :init (put-transition {})
   :transition-fn (fn [st _rng] {:prev-st st, :tx [], :new-st st})
   :seed 0})

(defn const-model [doc]
  {:short-desc (str "const " (:xt/id doc))
   :init (put-transition {} doc)
   :transition-fn (fn [st _rng] (put-transition st doc))
   :seed 0})

(defn- diff-state [a-label a, b-label b]
  (->> (for [k (set (concat (keys a) (keys b)))
             :let [ov (a k)
                   nv (b k)]
             :when (not= ov nv)]
         [k (array-map a-label ov b-label nv)])
       (into {})))

(defn annotate-hits [tx]
  (walk/postwalk (fn [x] (if-some [hit (:hit (meta x))]
                           (if hit
                             (list 'hit x)
                             (list 'miss x))
                           x))
                 tx))

(defn quick-states [{:keys [model,
                            node-cfg,
                            tx-count]
                     :or {model (counter-model {})
                          node-cfg {}
                          tx-count 100}}]
  (with-open [node (xt/start-node node-cfg)]
    (vec (for [{:keys [prev-st, tx, new-st]} (take tx-count (transition-seq model))
               :let [tx-res (xt/submit-tx node tx)
                     _ (xt/sync node)
                     observed (query-state node)
                     diff (diff-state :expected new-st :observed observed)]]
           {:tx-id (::xt/tx-id tx-res)
            :observed observed
            :expected new-st
            :prev prev-st
            :drift (pos? (count diff))
            :diff diff
            :expected-delta (diff-state :prev prev-st :new-st new-st)
            :tx (annotate-hits tx)}))))

(defn quick-run [{:keys [model,
                         tx-count]
                  :or {model (counter-model {})
                       tx-count 100}}]
  (with-open [node (xt/start-node {})]
    (doseq [tx (take tx-count (tx-seq model))]
      (xt/submit-tx node tx))
    (xt/sync node)
    (let [expected (state-at model tx-count)
          observed (query-state node)
          present-map present-state]
      {:node-status (xt/status node)
       :latest-submitted (xt/latest-submitted-tx node)
       :latest-completed (xt/latest-completed-tx node)
       :expected (present-map expected)
       :observed (present-map observed)
       :diff (present-map (diff-state :expected expected, :observed observed))
       :tx-log (take tx-count (tx-seq model))})))

(defn pg-cfg
  [{:keys [tx-port, doc-port]
    :or {tx-port 5432
         doc-port 5432}}]
  {:xtdb/tx-log {:xtdb/module 'xtdb.jdbc/->tx-log
                 :connection-pool {:dialect {:xtdb/module 'xtdb.jdbc.psql/->dialect}
                                   :pool-opts {}
                                   :db-spec {:port tx-port
                                             :dbtype "postgresql"
                                             :dbname "tx"
                                             :user "postgres"
                                             :password "postgres"}}}
   :xtdb/document-store {:xtdb/module 'xtdb.jdbc/->document-store
                         :connection-pool {:dialect {:xtdb/module 'xtdb.jdbc.psql/->dialect}
                                           :pool-opts {}
                                           :db-spec {:port doc-port
                                                     :dbtype "postgresql"
                                                     :dbname "docs"
                                                     :user "postgres"
                                                     :password "postgres"}}}})

(defn pg-reset [{:xtdb/keys [tx-log, document-store]}]
  (when-some [db-spec (:db-spec (:connection-pool tx-log))]
    (with-open [conn (jdbc/get-connection (dissoc db-spec :dbname))]
      (jdbc/execute! conn ["DROP DATABASE IF EXISTS tx"])
      (jdbc/execute! conn ["CREATE DATABASE tx"])))
  (when-some [db-spec (:db-spec (:connection-pool document-store))]
    (with-open [conn (jdbc/get-connection (dissoc db-spec :dbname))]
      (jdbc/execute! conn ["DROP DATABASE IF EXISTS docs"])
      (jdbc/execute! conn ["CREATE DATABASE docs"]))))

(defn pg-up []
  (sh/sh "docker-compose" "up" "-d" "postgres" :dir "modules/jdbc"))

(defn pg-down []
  (sh/sh "docker-compose" "down" :dir "modules/jdbc"))

(defn mysql-cfg
  [{:keys [tx-port, doc-port]
    :or {tx-port 3306
         doc-port 3306}}]
  {:xtdb/tx-log {:xtdb/module 'xtdb.jdbc/->tx-log
                 :connection-pool {:dialect {:xtdb/module 'xtdb.jdbc.mysql/->dialect}
                                   :pool-opts {}
                                   :db-spec {:port tx-port
                                             :dbname "tx",
                                             :user "root",
                                             :password "my-secret-pw"
                                             :dbtype "mysql"}}}
   :xtdb/document-store {:xtdb/module 'xtdb.jdbc/->document-store
                         :connection-pool {:dialect {:xtdb/module 'xtdb.jdbc.mysql/->dialect}
                                           :pool-opts {}
                                           :db-spec {:port doc-port
                                                     :dbname "docs",
                                                     :user "root",
                                                     :password "my-secret-pw"
                                                     :dbtype "mysql"}}}})

(defn mysql-reset [{:xtdb/keys [tx-log, document-store]}]
  (when-some [db-spec (:db-spec (:connection-pool tx-log))]
    (with-open [conn (jdbc/get-connection (dissoc db-spec :dbname))]
      (jdbc/execute! conn ["DROP DATABASE IF EXISTS tx"])
      (jdbc/execute! conn ["CREATE DATABASE tx"])))
  (when-some [db-spec (:db-spec (:connection-pool document-store))]
    (with-open [conn (jdbc/get-connection (dissoc db-spec :dbname))]
      (jdbc/execute! conn ["DROP DATABASE IF EXISTS docs"])
      (jdbc/execute! conn ["CREATE DATABASE docs"]))))

(defn mysql-up []
  (sh/sh "docker-compose" "up" "-d" "mysql" :dir "modules/jdbc"))

(defn mysql-down []
  (sh/sh "docker-compose" "down" :dir "modules/jdbc"))

(defn memory-doc-store []
  (let [kv-store (xtdb.mem-kv/->kv-store)]
    (xtdb.kv.document-store/->document-store
      {:kv-store kv-store,
       :document-cache (cache/->cache {:cache-size (* 128 1024)})
       :fsync? false})))

(defn memory-tx-log []
  (let [kv-store (xtdb.mem-kv/->kv-store)]
    (xtdb.kv.tx-log/->tx-log {:kv-store kv-store})))

(defprotocol FlakeProxy
  (pause-proxy-flake [_])
  (resume-proxy-flake [_]))

(extend-protocol FlakeProxy
  Object
  (pause-proxy-flake [_])
  (resume-proxy-flake [_]))

(defn pause-flake
  ([node]
   (doto node
     (pause-flake :xtdb/tx-log)
     (pause-flake :xtdb/document-store)))
  ([node k] (-> node :!system deref k pause-proxy-flake)))

(defn resume-flake
  ([node]
   (doto node
     (resume-flake :xtdb/tx-log)
     (resume-flake :xtdb/document-store)))
  ([node k] (-> node :!system deref k resume-proxy-flake)))

(defn- flake-fn
  "Returns a function that takes a Random and throws if sampling weighting-table yields
  an Exception or function (that throws or returns an Exception)."
  [weighting-table]
  (let [sample-fn (weighted-sample-fn weighting-table)]
    (fn [rng]
      (when-some [entry (sample-fn rng)]
        (if (fn? entry) (throw (entry)) (throw entry))))))

(defn doc-store-proxy
  "Applies optional flake to a shared :doc-store.

  You can configure flake by providing weighting tables exception (or fn yielding an exception) to weight.

  An example weighting table:

  {nil 1.0, ; nil means 'no exception'.
   (Exception. \"boom\") 0.1,
   (OutOfMemoryError) 0.05}

  ---
  Options:

  :rng - Random, (default new Random)


  Flake weighting options - default {}:

  - :fetch-flake
  - :submit-flake
  - :close-flake"
  [{:keys [doc-store
           rng
           fetch-flake
           submit-flake
           close-flake]
    :or {rng (Random.)
         fetch-flake {}
         submit-flake {}
         close-flake {}}}]
  (let [flaking (volatile! false)
        roll-fn (fn [weightings] (let [f (flake-fn weightings)] (fn [] (when @flaking (f rng)))))
        roll-fetch (roll-fn fetch-flake)
        roll-submit (roll-fn submit-flake)
        roll-close (roll-fn close-flake)]
    (reify db/DocumentStore
      (fetch-docs [_ ids]
        (roll-fetch)
        (db/fetch-docs doc-store ids))
      (submit-docs [_ doc-map]
        (roll-submit)
        (db/submit-docs doc-store doc-map))
      Closeable
      (close [_] (roll-close))
      FlakeProxy
      (pause-proxy-flake [_] (vreset! flaking false))
      (resume-proxy-flake [_] (vreset! flaking true)))))

(defn tx-log-proxy
  "Applies optional flake to a shared :tx-log.

  You can configure flake by providing weighting tables exception (or fn yielding an exception) to weight.

  An example weighting table:

  {nil 1.0, ; nil means 'no exception'.
   (Exception. \"boom\") 0.1,
   (OutOfMemoryError) 0.05}

  ---
  Options:

  :rng - Random, default new Random

  :subscribe-behaviour - default :proxy
  If :proxy subscribe will delegate to the :tx-log, if :poll is used - a polling subscriber is used.
  It might be useful to use :poll to ensure the open-tx-log cursor is being used for ingestion.

  Flake weighting options - default {}:

  - :open-flake
  - :cursor-flake
  - :cursor-close-flake
  - :latest-submitted-flake
  - :close-flake"
  [{:keys [tx-log
           rng
           subscribe-behaviour
           open-flake
           cursor-flake
           cursor-close-flake
           latest-submitted-flake
           close-flake]
    :or {rng (Random.)
         subscribe-behaviour :proxy
         open-flake {}
         cursor-flake {}
         cursor-close-flake {}
         latest-submitted-flake {}
         close-flake {}}}]
  (let [flaking (volatile! false)
        roll-fn (fn [weightings] (let [f (flake-fn weightings)] (fn [] (when @flaking (f rng)))))
        roll-open (roll-fn open-flake)
        roll-cursor (roll-fn cursor-flake)
        roll-cursor-close (roll-fn cursor-close-flake)
        roll-latest-submitted (roll-fn latest-submitted-flake)
        roll-close (roll-fn close-flake)]
    (reify db/TxLog
      (submit-tx [this tx-events] (db/submit-tx tx-log tx-events))
      (submit-tx [this tx-events opts] (db/submit-tx tx-log tx-events opts))
      (open-tx-log [this after-tx-id opts]
        (roll-open)
        (let [cursor (db/open-tx-log tx-log after-tx-id opts)]
          (reify ICursor
            (hasNext [_]
              (roll-cursor)
              (.hasNext cursor))
            (next [_]
              (roll-cursor)
              (.next cursor))
            (close [_]
              ;; do not want to actually leak, so .close first
              (.close cursor)
              (roll-cursor-close)))))
      (latest-submitted-tx [_]
        (roll-latest-submitted)
        (db/latest-submitted-tx tx-log))
      (subscribe [this after-tx-id f]
        (case subscribe-behaviour
          :proxy (db/subscribe tx-log after-tx-id f)
          :poll (tx-sub/handle-polling-subscription this after-tx-id {:poll-sleep-duration (Duration/ofMillis 1)} f)))
      Closeable
      (close [_] (roll-close))
      FlakeProxy
      (pause-proxy-flake [_] (vreset! flaking false))
      (resume-proxy-flake [_] (vreset! flaking true)))))

(defn- run-ctx* [f {:keys [sleep, logging]}]
  (with-redefs [xio/sleep (if sleep xio/sleep (fn [& _]))
                log/log* (if logging log/log* (fn [& _]))]
    (f)))

(defmacro ^:private run-ctx [opts & body] `(run-ctx* (fn [] ~@body) ~opts))

(def ^:dynamic *print-opts* false)

(defn test-model
  "Runs a concurrent test to try and provoke non-determinism, returning a report of the test.

  ---
  Options:

  :model - default (counter-model {}).
  The model that defines the transactions that make up the test.

  :logging - default false
  Set to true if you want logs but why would you do that to yourself.

  :index - default :rocks
  The kv store to use, :lmdb or :rocks. Dirs will be managed by the test, forget-about-it.

  :storage - default in-memory healthy storage.
  Map describing the shared storage to use, and its flake-ness. See tx-log-proxy, doc-store-proxy to configure failures.
  - Use :pg to configure postgres (pg-cfg arg)
  - Use :tx-log, :doc-store to pass options to tx-log-proxy and doc-store-proxy, see their docs for more info.

  :submit-threads - default 1
  The number of submitting threads to use.
  If above 1, the report will be diff against the first nodes state, instead of the expected state derived from the transition model.

  :node-count - default 2
  Run as many as your machine can handle.

  :tx-count - default 100
  The number of transactions to submit, generated from the model.

  :sync-duration - java.time.Duration, default 1 second
  The amount of time to wait before checking if a node is done, still progressing or failed.
  This bounds how often restarts might happen, so you might want to set it lower.
  However, if an individual transactions might take longer than this time to process - then you might exit the test early.

  ---
  Report:
  Returns a report as a map, summarized:

  {:submit {:tx-time-ordering-violations (count of ordering violations, where tx-time is > the previous transaction)} ,
   :consistent true|false,
   :nodes [{:node {:status (result of xt/status call),
                   :restarts (a vector of restart indicators, :s for a stuck restart, :i for ingester fail),
                   :latest-submitted (result of xt/last-submitted-tx)
                   :latest-completed (result of xt/last-completed-tx),
           :expected (the expected st map),
           :observed (the observed st map),
           :diff (any difference between the maps)}
           ...]}

  See also print-report, print-test."
  [{:keys [model,
           logging,
           index
           storage
           submit-threads
           node-count
           tx-count
           sync-duration]
    :or {model (counter-model {})
         index :rocks
         submit-threads 1
         node-count 2
         tx-count 100
         sync-duration (Duration/ofSeconds 1)}}]
  (when *print-opts*
    (println "- :model" (:short-desc model "anonymous") (format "(seed %s)" (:seed model)))
    (println "- :node-count" node-count)
    (println "- :submit-threads" submit-threads)
    (println "- :tx-count" tx-count))
  (run-ctx {:logging logging}
    (let [kv-data-dirs (vec (repeatedly node-count #(xio/create-tmpdir "kv")))

          get-kv-cfg
          (fn [node-idx]
            {:xtdb/module (case index :rocks 'xtdb.rocksdb/->kv-store :lmdb 'xtdb.lmdb/->kv-store)
             :db-dir (str (kv-data-dirs node-idx))})

          get-pg-node-cfg
          (let [pg-cfg (delay (doto (pg-cfg (:pg storage)) (pg-reset)))]
            (fn [node-idx]
              (assoc @pg-cfg :xtdb/index-store {:kv-store (get-kv-cfg node-idx)})))

          get-mysql-node-cfg
          (let [mysql-cfg (delay (doto (mysql-cfg (:pg storage)) (mysql-reset)))]
            (fn [node-idx]
              (assoc @mysql-cfg :xtdb/index-store {:kv-store (get-kv-cfg node-idx)})))

          get-mem-node-cfg
          (let [tx-log (delay (memory-tx-log))
                doc-store (delay (memory-doc-store))]
            (fn [node-idx]
              {:xtdb/index-store {:kv-store (get-kv-cfg node-idx)}
               :xtdb/tx-log {:xtdb/module (fn [& _] (tx-log-proxy (assoc (:tx-log storage) :tx-log @tx-log)))}
               :xtdb/document-store {:xtdb/module (fn [& _] (doc-store-proxy (assoc (:doc-store storage) :doc-store @doc-store)))}}))

          get-node-cfg (cond (:pg storage) get-pg-node-cfg, (:mysql storage) get-mysql-node-cfg, :else get-mem-node-cfg)
          node-refs (vec (repeatedly node-count #(volatile! nil)))
          restarts (vec (repeatedly node-count #(volatile! [])))
          rng (Random. 34234324234)
          tx-results (object-array tx-count)
          tx-nodes (object-array tx-count)
          retry-silent #(xio/exp-backoff % {:retryable (constantly true)
                                            :on-retry (constantly nil)})]
      (try

        ;; start nodes
        (doseq [[node-idx node-ref] (map-indexed vector node-refs)]
          (vreset! node-ref (doto (xt/start-node (get-node-cfg node-idx))
                              (resume-flake))))

        ;; submit all transactions
        (let [tx-executor (Executors/newFixedThreadPool submit-threads)
              all-sent (volatile! false)]
          (try

            (future
              (doseq [[i tx] (map-indexed vector (take tx-count (tx-seq model)))]
                (let [node-idx (rand-int rng (count node-refs))
                      node-ref (nth node-refs node-idx)
                      tx-fut (.submit tx-executor ^Callable (fn [] (retry-silent #(xt/submit-tx @node-ref tx))))]
                  (aset tx-results i tx-fut)
                  (aset tx-nodes i node-idx)))
              (vreset! all-sent true))

            @(future
               (doseq [[node-idx node-ref] (map-indexed vector node-refs)
                       :let [max-tx-id (fn [a b] (if (and a b) (max a b) (or a b)))]]
                 (loop [completed-tx-id nil
                        max-completed-tx-id nil
                        progressing true]
                   (try (xt/sync @node-ref sync-duration) (catch Throwable _))
                   (let [node @node-ref
                         now-completed-tx-id (::xt/tx-id (retry-silent #(xt/latest-completed-tx node)))
                         max-completed-tx-id (max-tx-id max-completed-tx-id now-completed-tx-id)
                         now-submitted-tx-id (::xt/tx-id (retry-silent #(xt/latest-submitted-tx node)))]
                     (cond
                       (Thread/interrupted) (throw (InterruptedException.))

                       (and now-completed-tx-id (< (or completed-tx-id -1) now-completed-tx-id))
                       (do
                         (doto node (resume-flake))
                         (recur now-completed-tx-id max-completed-tx-id true))

                       (= completed-tx-id now-completed-tx-id)
                       (cond
                         (and progressing (:ingester-failed? (xt/status node)))
                         (do (xio/try-close node)
                             (vswap! (restarts node-idx) conj [:i now-completed-tx-id])
                             (vreset! node-ref (xt/start-node (get-node-cfg node-idx)))
                             (recur now-completed-tx-id max-completed-tx-id false))


                         (= now-completed-tx-id now-submitted-tx-id)
                         (cond
                           (not @all-sent)
                           (recur now-completed-tx-id max-completed-tx-id true)

                           (not= now-submitted-tx-id (apply max (map (comp ::xt/tx-id deref) tx-results)))
                           (recur now-completed-tx-id max-completed-tx-id true)

                           :else nil)

                         progressing
                         (do (xio/try-close node)
                             (vswap! (restarts node-idx) conj [:i now-completed-tx-id])
                             (vreset! node-ref (xt/start-node (get-node-cfg node-idx)))
                             (recur now-completed-tx-id max-completed-tx-id false))

                         :else
                         (do (println (format "%s stuck at:%s, max:%s, submitted:%s, restarts:%s"
                                              node-idx now-completed-tx-id max-completed-tx-id now-submitted-tx-id
                                              @(restarts node-idx)))
                             nil))

                       :else (recur now-completed-tx-id max-completed-tx-id true))))))

            (finally
              (.shutdown tx-executor)
              (.awaitTermination tx-executor 10 TimeUnit/MINUTES))))

        (run! pause-flake (map deref node-refs))

        ;; reports
        (let [expected (if (= 1 submit-threads)
                         (state-at model tx-count)
                         (query-state @(first node-refs)))

              submit-report
              {:tx-time-ordering-violations
               (letfn [(ordering-violation? [tx1 tx2]
                         (when (and tx1 tx2)
                           (let [comparison (compare (::xt/tx-time tx2) (::xt/tx-time tx1))]
                             (neg? comparison))))]
                 (->> tx-results
                      (sort-by #(some-> % deref ::xt/tx-id))
                      (partition-all 2)
                      (filter (fn [[a b]] (ordering-violation? (some-> a deref) (some-> b deref))))
                      count))}

              node-reports
              (vec (for [[node-idx node-ref] (map-indexed vector node-refs)
                         :let [node @node-ref
                               observed (query-state node)]]
                     {:node {:status (xt/status node)
                             :restarts @(restarts node-idx)
                             :latest-submitted (xt/latest-submitted-tx node)
                             :latest-completed (xt/latest-completed-tx node)}
                      :expected (present-state expected)
                      :observed (present-state observed)
                      :diff (present-state (diff-state :expected expected, :observed observed))}))]
          {:submit submit-report
           :consistent (every? empty? (map :diff node-reports))
           :nodes node-reports})
        (finally
          (run! xio/try-close (keep deref node-refs))
          (run! xio/delete-dir kv-data-dirs))))))

(def ^:redef last-printed-report)

(defn print-report [report]
  (let [{:keys [consistent, nodes]} report
        outcome (format "[%s/%s]" (count (filter (comp empty? :diff) nodes)) (count nodes))]
    (println "")
    (if consistent
      (println "✅ SUCCESS" outcome)
      (do (println "❌ FAIL" outcome)
          (println)
          (doseq [[i {node-report :node
                      :keys [diff]}] (map-indexed vector nodes)]
            (println "node" i (if (seq diff) "❌" "✅"))
            (let [{:keys [latest-submitted, latest-completed]} node-report
                  submitted-tx-id (::xt/tx-id latest-submitted)
                  completed-tx-id (::xt/tx-id latest-completed)]
              (when (not= submitted-tx-id completed-tx-id)
                (println "Did not finish indexing at" completed-tx-id "of" submitted-tx-id)))
            (println)
            (when (seq diff)
              (pp/pprint diff))
            (println))))

    (when (some (comp seq :restarts :node) nodes)
      (println "Restarts")
      (println ":s = stuck, :i = ingester fail")
      (doseq [[i {node-report :node}] (map-indexed vector nodes)
              :let [{:keys [restarts]} node-report]
              :when (seq restarts)]
        (print i " " (str/join ", " restarts))
        (flush)
        (println)))

    (alter-var-root #'last-printed-report (constantly report))
    (println "For full report see the value of last-printed-report")))

(defn print-test
  "Runs a test and prints it. See test-model for opts, or if you want data back."
  [opts]
  (println "Starting test 🕐")
  (binding [*print-opts* true]
    (print-report (test-model opts))))

(comment

  (pg-up)
  (pg-down)
  (pg-reset (pg-cfg {}))

  (mysql-up)
  (mysql-down)
  (mysql-reset (mysql-cfg {}))

  (print-test {:storage {:mysql {}}})

  (print-test {})
  (print-test {:model (counter-model {})})
  (print-test {:model noop-model})
  (print-test {:model (const-model {:xt/id :foo})})
  (print-test {:model (counter-model {:non-determinism 0.5})})

  (print-test {:model (counter-model {:non-determinism 0.0})
               :storage {:doc-store {:fetch-flake {nil 1.0, (Exception. "boom") 0.1}}}
               :tx-count 100})

  (print-test {:model (counter-model {:non-determinism 0.0})
               :storage {:doc-store {:fetch-flake {nil 1.0, (OutOfMemoryError. "boom") 0.1}}}
               :tx-count 100})

  (print-test {:model (counter-model {:counters 1, :non-determinism 0.0})
               :storage {:doc-store {:submit-flake {nil 1.0, (OutOfMemoryError. "boom") 0.1}}}
               :node-count 1
               :tx-count 4})

  (print-test {:model (counter-model {:counters 1, :non-determinism 0.0})
               :storage {:doc-store {:submit-flake {nil 1.0, (OutOfMemoryError. "boom") 0.5}}}
               :node-count 1
               :tx-count 3})

  (print-test {:model (counter-model
                        {:counters 16,
                         :non-determinism 0.0})
               :node-count 4
               :submit-threads 4
               :storage {:doc-store {:fetch-flake {nil 1.0, (Exception. "boom") 0.01}
                                     :submit-flake {nil 1.0, (Exception. "boom") 0.01}
                                     :close-flake {nil 1.0, (Exception. "boom") 0.1}}
                         :tx-log {:subscribe-behaviour :poll
                                  :open-flake {nil 1.0, (Exception. "boom") 0.1}
                                  :cursor-flake {nil 1.0, (Exception. "boom") 0.0001}
                                  :cursor-close-flake {nil 1.0, (Exception. "boom") 0.01}
                                  :close-flake {nil 1.0, (Exception. "boom") 0.1}}}
               :tx-count 10000})

  )

(comment


  (take 2 (transition-seq (counter-model {:counters 1})))
  (take 2 (tx-seq (counter-model {:counters 1})))
  (state-at (counter-model {:counters 1}) 2)

  )
