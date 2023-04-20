(ns xtdb.current-row-ids-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.datalog :as xt]
            [xtdb.operator.scan :as scan]
            [xtdb.temporal :as temporal]
            [xtdb.temporal.kd-tree-test :refer [as-micros ->coordinates]]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import org.roaringbitmap.longlong.Roaring64Bitmap
           java.io.Closeable
           org.apache.arrow.memory.RootAllocator
           java.time.Duration))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(def
  tx1
  '[[:put :xt_docs {:xt/id :ivan, :first-name "Ivan"}]
    [:put :xt_docs {:xt/id :petr, :first-name "Petr"}
     {:for-app-time [:in #inst "2020-01-02T12:00:00Z"]}]
    [:put :xt_docs {:xt/id :susie, :first-name "Susie"}
     {:for-app-time [:in nil #inst "2020-01-02T13:00:00Z"]}]
    [:put :xt_docs {:xt/id :sam, :first-name "Sam"}]
    [:put :xt_docs {:xt/id :petr, :first-name "Petr"}
     {:for-app-time [:in #inst "2020-01-04T12:00:00Z"]}]
    [:put :xt_docs {:xt/id :jen, :first-name "Jen"}
     {:for-app-time [:in nil #inst "2020-01-04T13:00:00Z"]}]
    [:put :xt_docs {:xt/id :james, :first-name "James"}
     {:for-app-time [:in #inst "2020-01-01T12:00:00Z"]}]
    [:put :xt_docs {:xt/id :jon, :first-name "Jon"}
     {:for-app-time [:in nil #inst "2020-01-01T12:00:00Z"]}]
    [:put :xt_docs {:xt/id :lucy :first-name "Lucy"}]])

(deftest test-current-row-ids
  (xt/submit-tx
   tu/*node*
   tx1)

  (xt/submit-tx
   tu/*node*
   '[[:put :xt_docs {:xt/id :ivan, :first-name "Ivan-2"}
      {:for-app-time [:in #inst "2020-01-02T14:00:00Z"]}]
     [:put :xt_docs {:xt/id :ben, :first-name "Ben"}
      {:for-app-time [:in #inst "2020-01-02T14:00:00Z" #inst "2020-01-02T15:00:00Z"]}]
     [:evict :xt_docs :lucy]])

  (t/is (= [{:name "Ivan-2"}
            {:name "James"}
            {:name "Jen"}
            {:name "Petr"}
            {:name "Sam"}]
           (xt/q
            tu/*node*
            (-> '{:find [name]
                  :where [(match :xt_docs {:first-name name})]
                  :order-by [[name :asc]]}
                (assoc :basis {:current-time #time/instant "2020-01-03T00:00:00Z"})))))) ;; timing

(defn valid-ids-at [current-time]
  (xt/q
    tu/*node*
    (-> '{:find [id]
          :where [(match :xt_docs {:xt/id id})]}
        (assoc :basis {:current-time current-time}))))

(deftest test-current-row-ids-app-time-start-inclusivity
  (t/testing "app-time-start"
    (xt/submit-tx
     tu/*node*
     '[[:put :xt_docs {:xt/id 1}
        {:for-app-time [:in #inst "2020-01-01T00:00:02Z"]}]])

    (t/is (= []
             (valid-ids-at #time/instant "2020-01-01T00:00:01Z")))
    (t/is (= [{:id 1}]
             (valid-ids-at #time/instant "2020-01-01T00:00:02Z")))
    (t/is (= [{:id 1}]
             (valid-ids-at #time/instant "2020-01-01T00:00:03Z")))))

(deftest test-current-row-ids-app-time-end-inclusivity
  (t/testing "app-time-start"
    (xt/submit-tx
     tu/*node*
     '[[:put :xt_docs {:xt/id 1}
        {:for-app-time [:in nil #inst "2020-01-01T00:00:02Z"]}]])

    (t/is (= [{:id 1}]
             (valid-ids-at #time/instant "2020-01-01T00:00:01Z")))
    (t/is (= []
             (valid-ids-at #time/instant "2020-01-01T00:00:02Z")))
    (t/is (= []
             (valid-ids-at #time/instant "2020-01-01T00:00:03Z")))))


(deftest remove-evicted-row-ids-test
  (t/is
    (= #{1 3}
       (temporal/remove-evicted-row-ids
         #{1 2 3}
         (doto
           (Roaring64Bitmap.)
           (.addLong (long 2)))))))

(deftest current-row-ids-can-be-built-at-startup
  (let [node-dir (util/->path "target/can-build-current-row-ids-at-startup")
        expectation [{:name "Ivan"}
                     {:name "James"}
                     {:name "Jen"}
                     {:name "Lucy"}
                     {:name "Petr"}
                     {:name "Sam"}]]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]

      (-> (xt/submit-tx node tx1)
          (tu/then-await-tx* node (Duration/ofMillis 2000)))

      (tu/finish-chunk! node)

      (t/is (= expectation
               (xt/q
                 node
                 (-> '{:find [name]
                       :where [(match :xt_docs {:first-name name})]
                       :order-by [[name :asc]]}
                     (assoc :basis {:current-time #time/instant "2020-01-03T00:00:00Z"}))))))

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (t/is (= expectation
               (xt/q
                 node
                 (-> '{:find [name]
                       :where [(match :xt_docs {:first-name name})]
                       :order-by [[name :asc]]}
                     (assoc :basis {:current-time #time/instant "2020-01-03T00:00:00Z"}))))))))

(deftest test-queries-that-can-use-current-row-id-cache
  (with-redefs [scan/get-current-row-ids
                (fn [_ _]
                  (throw (Exception. "Scan tried to use current-row-id cache")))]

    (t/is
     (xt/q
      tu/*node*
      '{:find [id]
        :where [(match :xt_docs {:xt/id id})]})
     "query against empty db should not use current-row-id")

    (let [tx1 (xt/submit-tx
               tu/*node*
               '[[:put :xt_docs {:xt/id 1}]])
          tx2 (xt/submit-tx
               tu/*node*
               '[[:put :xt_docs {:xt/id 2}]])]

      (t/testing "queries that can use current-row-ids cache"

        (t/is
         (thrown-with-msg?
          Exception
          #"Scan tried to use current-row-id cache"
          (xt/q
           tu/*node*
           (-> '{:find [id]
                 :where [(match :xt_docs {:xt/id id})]})))
         "query with no temporal constraints")

        (t/is
         (thrown-with-msg?
          Exception
          #"Scan tried to use current-row-id cache"
          (xt/q
           tu/*node*
           (-> '{:find [id]
                 :where [(match :xt_docs {:xt/id id})]}
               (assoc :basis {:tx tx2}))))
         "query at latest tx")

        (t/is
         (thrown-with-msg?
          Exception
          #"Scan tried to use current-row-id cache"
          (xt/q
           tu/*node*
           (-> '{:find [id]
                 :where [(match :xt_docs {:xt/id id})]}
               (assoc :basis {:current-time #time/instant "2020-01-03T00:00:00Z"}))))
         "query with current-time now or in future")

        (t/is
         (thrown-with-msg?
          Exception
          #"Scan tried to use current-row-id cache"
          (xt/q
           tu/*node*
           (-> '{:find [id]
                 :where [(match :xt_docs {:xt/id id}
                                {:for-app-time [:at :now]
                                 :for-sys-time [:at :now]})]})))
         "query where all table temporal constaints are now"))

      (t/testing "queries that cannot use current-row-ids cache"

        (t/is
         (xt/q
          tu/*node*
          (-> '{:find [id]
                :where [(match :xt_docs {:xt/id id})]}
              (assoc :basis {:tx tx1})))
         "query at previous tx")

        (t/is
         (xt/q
          tu/*node*
          (-> '{:find [id]
                :where [(match :xt_docs {:xt/id id})]}
              (assoc :basis {:current-time #time/instant "2020-01-01T00:00:00Z"})))
         "query with current-time in past")

        (t/is
         (xt/q
          tu/*node*
          (-> '{:find [id]
                :where [(match :xt_docs {:xt/id id}
                               {:for-app-time [:at :now]
                                :for-sys-time [:at #inst "2020-01-01"]})]}))
         "query where all any table temporal constaints aside from now are set")

        (t/is
         (xt/q
          tu/*node*
          (-> '{:find [id]
                :where [(match :xt_docs {:xt/id id}
                               {:for-app-time :all-time})]}))
         "query where all any table temporal constaints aside from now are set")

        (t/is
         (xt/q
          tu/*node*
          (-> '{:find [id]
                :where [(match
                            :xt_docs
                          {:xt/id id
                           :xt/valid-from xt/valid-from}
                          {:for-app-time :all-time})]}))
         "query where all any temporal cols are projected out")))))

(defn current-rows-for [sys-time inserts]
  (let [kd-tree nil
        !current-row-ids (volatile! #{})]
    (with-open [allocator (RootAllocator.)
                ^Closeable kd-tree (reduce
                                    (fn [cur-kd-tree coords]
                                      (temporal/insert-coordinates cur-kd-tree
                                                                   allocator
                                                                   coords
                                                                   !current-row-ids
                                                                   (as-micros sys-time)))
                                    kd-tree
                                    inserts)]
      @!current-row-ids)))

(deftest current-row-ids-inserts
  (let [sys-time #inst "2020-01-02"]
    (t/is (=
           #{1}
           (current-rows-for
            sys-time
            [(->coordinates {:id 101
                             :row-id 1
                             :sys-time-start sys-time
                             :app-time-start #inst "2020-01-02"
                             :new-entity? true})]))
          "app-time-start equal to sys-time"))

  (let [sys-time #inst "2020-01-02"]
    (t/is (=
           #{1}
           (current-rows-for
            sys-time
            [(->coordinates {:id 101
                             :row-id 1
                             :sys-time-start sys-time
                             :app-time-start #inst "2020-01-01"
                             :new-entity? true})]))
          "app-time-start before sys-time"))

  (let [sys-time #inst "2020-01-02"]
    (t/is (=
           #{}
           (current-rows-for
            sys-time
            [(->coordinates {:id 101
                             :row-id 1
                             :sys-time-start sys-time
                             :app-time-start #inst "2020-01-03"
                             :new-entity? true})]))
          "app-time-start after sys-time"))

  (let [sys-time #inst "2020-01-02"]
    (t/is (=
           #{}
           (current-rows-for
            sys-time
            [(->coordinates {:id 101
                             :row-id 1
                             :sys-time-start sys-time
                             :app-time-start #inst "2020-01-01"
                             :app-time-end #inst "2020-01-02"
                             :new-entity? true})]))
          "app-time-start and end before sys-time"))

  (let [sys-time #inst "2020-01-02"]
    (t/is (=
           #{1 2}
           (current-rows-for
            sys-time
            [(->coordinates {:id 101
                             :row-id 1
                             :sys-time-start sys-time
                             :app-time-start #inst "2020-01-02"
                             :new-entity? true})
             (->coordinates {:id 102
                             :row-id 2
                             :sys-time-start sys-time
                             :app-time-start #inst "2020-01-01"
                             :new-entity? true})]))
          "one row for each entity is added")))

(deftest current-row-ids-overlaps
  (let [sys-time #inst "2020-01-02"]
    (t/is (=
           #{}
           (current-rows-for
            sys-time
            [(->coordinates {:id 101
                             :row-id 1
                             :sys-time-start sys-time
                             :app-time-start #inst "2020-01-02"
                             :new-entity? true})
             (->coordinates {:id 101
                             :row-id 2
                             :sys-time-start sys-time
                             :app-time-start #inst "2020-01-02"
                             :new-entity? false
                             :tombstone? true})]))
          "delete overlapping at current sys-time"))

  (let [sys-time #inst "2020-01-02"]
    (t/is (=
           #{1}
           (current-rows-for
            sys-time
            [(->coordinates {:id 101
                             :row-id 1
                             :sys-time-start sys-time
                             :app-time-start #inst "2020-01-02"
                             :app-time-end #inst "2020-01-10"
                             :new-entity? true})
             (->coordinates {:id 101
                             :row-id 2
                             :sys-time-start sys-time
                             :app-time-start #inst "2020-01-03"
                             :app-time-end #inst "2020-01-20"
                             :new-entity? false
                             :tombstone? true})]))
          "delete overlapping after current sys-time"))

  (let [sys-time #inst "2020-01-02"]
    (t/is (=
           #{}
           (current-rows-for
            sys-time
            [(->coordinates {:id 101
                             :row-id 1
                             :sys-time-start sys-time
                             :app-time-start #inst "2020-01-01"
                             :app-time-end #inst "2020-01-10"
                             :new-entity? true})
             (->coordinates {:id 101
                             :row-id 2
                             :sys-time-start sys-time
                             :app-time-start #inst "2020-01-02"
                             :app-time-end #inst "2020-01-04"
                             :new-entity? false
                             :tombstone? true})]))
          "delete overlapping before current sys-time"))

  (let [sys-time #inst "2020-01-02"]
    (t/is (=
           #{2}
           (current-rows-for
            sys-time
            [(->coordinates {:id 101
                             :row-id 1
                             :sys-time-start sys-time
                             :app-time-start #inst "2020-01-01"
                             :new-entity? true})
             (->coordinates {:id 101
                             :row-id 2
                             :sys-time-start sys-time
                             :app-time-start #inst "2020-01-02"
                             :new-entity? false})]))
          "new put overlapping at current sys-time")))

(deftest advance-current-row-ids-add-app-time-start-upper-bound
  (let [sys-time #time/instant "2020-01-01T00:00:01.000001Z"
        kd-tree nil
        !current-row-ids (volatile! #{})]
    (with-open [allocator (RootAllocator.)
                ^Closeable kd-tree (reduce
                                    (fn [cur-kd-tree coords]
                                      (temporal/insert-coordinates cur-kd-tree
                                                                   allocator
                                                                   coords
                                                                   !current-row-ids
                                                                   (util/instant->micros sys-time)))
                                    kd-tree
                                    [(->coordinates {:id 101
                                                     :row-id 1
                                                     :sys-time-start sys-time
                                                     :app-time-start #time/instant "2020-01-01T00:00:01.000010Z"
                                                     :new-entity? true})])]
      (t/is (= #{}
               @!current-row-ids))

      (t/is (= #{}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000009Z"))))

      (t/is (= #{1}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000010Z"))))

      (t/is (= #{1}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000011Z")))))))

(deftest advance-current-row-ids-add-app-time-start-lower-bound
  (let [sys-time #time/instant "2020-01-01T00:00:01.000001Z"
        kd-tree nil
        !current-row-ids (volatile! #{})]
    (with-open [allocator (RootAllocator.)
                ^Closeable kd-tree (reduce
                                    (fn [cur-kd-tree coords]
                                      (temporal/insert-coordinates cur-kd-tree
                                                                   allocator
                                                                   coords
                                                                   !current-row-ids
                                                                   (util/instant->micros sys-time)))
                                    kd-tree
                                    [(->coordinates {:id 101
                                                     :row-id 1
                                                     :sys-time-start sys-time
                                                     :app-time-start #time/instant "2020-01-01T00:00:01.000002Z"
                                                     :new-entity? true})])]
      (t/is (= #{}
               @!current-row-ids))

      (t/is (= #{}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000001Z"))))

      (t/is (= #{1}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000002Z"))))

      (t/is (= #{1}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000003Z")))))))


(deftest advance-current-row-ids-add-app-time-end-lower-bound
  (let [sys-time #time/instant "2020-01-01T00:00:01.000001Z"
        kd-tree nil
        !current-row-ids (volatile! #{})]
    (with-open [allocator (RootAllocator.)
                ^Closeable kd-tree (reduce
                                    (fn [cur-kd-tree coords]
                                      (temporal/insert-coordinates cur-kd-tree
                                                                   allocator
                                                                   coords
                                                                   !current-row-ids
                                                                   (util/instant->micros sys-time)))
                                    kd-tree
                                    [(->coordinates {:id 101
                                                     :row-id 1
                                                     :sys-time-start sys-time
                                                     :app-time-start #time/instant "2020-01-01T00:00:01.000008Z"
                                                     :app-time-end #time/instant "2020-01-01T00:00:01.000010Z"
                                                     :new-entity? true})])]
      (t/is (= #{}
               @!current-row-ids))

      (t/is (= #{1}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000009Z"))))

      (t/is (= #{}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000010Z"))))

      (t/is (= #{}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000011Z")))))))


(deftest advance-current-row-ids-remove-app-time-end-upper-bound
  (let [sys-time #time/instant "2020-01-01T00:00:01.000001Z"
        kd-tree nil
        !current-row-ids (volatile! #{})]
    (with-open [allocator (RootAllocator.)
                ^Closeable kd-tree (reduce
                                    (fn [cur-kd-tree coords]
                                      (temporal/insert-coordinates cur-kd-tree
                                                                   allocator
                                                                   coords
                                                                   !current-row-ids
                                                                   (util/instant->micros sys-time)))
                                    kd-tree
                                    [(->coordinates {:id 101
                                                     :row-id 1
                                                     :sys-time-start sys-time
                                                     :app-time-end #time/instant "2020-01-01T00:00:01.000010Z"
                                                     :new-entity? true})])]
      (t/is (= #{1}
               @!current-row-ids))

      (t/is (= #{1}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000009Z"))))

      (t/is (= #{}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000010Z"))))

      (t/is (= #{}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000011Z")))))))

(deftest advance-current-row-ids-remove-app-time-end-lower-bound
  (let [sys-time #time/instant "2020-01-01T00:00:01.000001Z"
        kd-tree nil
        !current-row-ids (volatile! #{})]
    (with-open [allocator (RootAllocator.)
                ^Closeable kd-tree (reduce
                                    (fn [cur-kd-tree coords]
                                      (temporal/insert-coordinates cur-kd-tree
                                                                   allocator
                                                                   coords
                                                                   !current-row-ids
                                                                   (util/instant->micros sys-time)))
                                    kd-tree
                                    [(->coordinates {:id 101
                                                     :row-id 1
                                                     :sys-time-start sys-time
                                                     :app-time-end #time/instant "2020-01-01T00:00:01.000002Z"
                                                     :new-entity? true})])]
      (t/is (= #{1}
               @!current-row-ids))

      (t/is (= #{1}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000001Z"))))

      (t/is (= #{}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000002Z"))))

      (t/is (= #{}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000003Z")))))))

(deftest advance-current-row-ids-remove-app-time-start-lower-bound
  (let [sys-time #time/instant "2020-01-01T00:00:01.000001Z"
        kd-tree nil
        !current-row-ids (volatile! #{})]
    (with-open [allocator (RootAllocator.)
                ^Closeable kd-tree (reduce
                                    (fn [cur-kd-tree coords]
                                      (temporal/insert-coordinates cur-kd-tree
                                                                   allocator
                                                                   coords
                                                                   !current-row-ids
                                                                   (util/instant->micros sys-time)))
                                    kd-tree
                                    [(->coordinates {:id 101
                                                     :row-id 1
                                                     :sys-time-start sys-time
                                                     :app-time-start #time/instant "2020-01-01T00:00:01.000001Z"
                                                     :app-time-end #time/instant "2020-01-01T00:00:01.000002Z"
                                                     :new-entity? true})])]
      (t/is (= #{1}
               @!current-row-ids))

      (t/is (= #{1}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000001Z"))))

      (t/is (= #{}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000002Z"))))

      (t/is (= #{}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000003Z")))))))

(deftest current-row-ids-from-start-test
  (let [sys-time #time/instant "2020-01-01T00:00:01.000001Z"
        kd-tree nil
        !current-row-ids (volatile! #{})]
    (with-open [allocator (RootAllocator.)
                ^Closeable kd-tree (reduce
                                    (fn [cur-kd-tree coords]
                                      (temporal/insert-coordinates cur-kd-tree
                                                                   allocator
                                                                   coords
                                                                   !current-row-ids
                                                                   (util/instant->micros sys-time)))
                                    kd-tree
                                    [(->coordinates {:id 101
                                                     :row-id 1
                                                     :sys-time-start sys-time
                                                     :app-time-start sys-time
                                                     :new-entity? true})
                                     (->coordinates {:id 101
                                                     :row-id 2
                                                     :sys-time-start sys-time
                                                     :app-time-start #time/instant "2020-01-01T00:00:01.000010Z"
                                                     :new-entity? false})])]
      (t/is (= #{1}
               (temporal/current-row-ids-from-start
                kd-tree
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000009Z"))))

      (t/is (= #{2}
               (temporal/current-row-ids-from-start
                kd-tree
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000010Z"))))

      (t/is (= #{2}
               (temporal/current-row-ids-from-start
                kd-tree
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000011Z")))))))

(deftest advance-current-row-ids-multiple-periods
  ;; test proves the need to sort additions/removals by valid time
  (let [sys-time #time/instant "2020-01-01T00:00:01.000001Z"
        kd-tree nil
        !current-row-ids (volatile! #{})]
    (with-open [allocator (RootAllocator.)
                ^Closeable kd-tree (reduce
                                    (fn [cur-kd-tree coords]
                                      (temporal/insert-coordinates cur-kd-tree
                                                                   allocator
                                                                   coords
                                                                   !current-row-ids
                                                                   (util/instant->micros sys-time)))
                                    kd-tree
                                    [(->coordinates {:id 101
                                                     :row-id 1
                                                     :sys-time-start sys-time
                                                     :app-time-start sys-time
                                                     :new-entity? true})
                                     (->coordinates {:id 101
                                                     :row-id 2
                                                     :sys-time-start sys-time
                                                     :app-time-start #time/instant "2020-01-01T00:00:01.000003Z"
                                                     :app-time-end #time/instant "2020-01-01T00:00:01.000004Z"
                                                     :new-entity? false
                                                     :tombstone? true})
                                     (->coordinates {:id 101
                                                     :row-id 3
                                                     :sys-time-start sys-time
                                                     :app-time-start #time/instant "2020-01-01T00:00:01.000006Z"
                                                     :app-time-end  #time/instant "2020-01-01T00:00:01.000008Z"
                                                     :new-entity? false
                                                     :tombstone? true})])]
      (t/is (= #{1}
               @!current-row-ids))

      (t/is (= #{1}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000002Z"))))

      (t/is (= #{}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000003Z"))))


      (t/is (= #{1}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000004Z"))))

      (t/is (= #{1}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000005Z"))))

      (t/is (= #{}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000006Z"))))

      (t/is (= #{}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000007Z"))))

      (t/is (= #{1}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000008Z"))))

      (t/is (= #{1}
               (temporal/advance-current-row-ids
                @!current-row-ids kd-tree
                (util/instant->micros sys-time)
                (util/instant->micros #time/instant "2020-01-01T00:00:01.000009Z")))))))

(deftest test-query-empty-db
  (t/is
    (= []
       (xt/q
         tu/*node*
         '{:find [name]
           :where [(match :xt_docs {:first-name name})]}))
    "watermark for empty db will have no basis so don't use current-row-ids"))
