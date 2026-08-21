(ns xtdb.as-of-resolver-test
  (:require [clojure.test :as t]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]))

(def ^:private vt-instants
  [#inst "2018" #inst "2019" #inst "2020" #inst "2021" #inst "2022" #inst "2023"])

(def ^:private op-gen
  (gen/let [id (gen/choose 1 4)
            kind (gen/frequency [[6 (gen/return :put)] [3 (gen/return :delete)] [1 (gen/return :erase)]])
            vf-idx (gen/choose 0 (- (count vt-instants) 2))
            vt-idx (gen/choose (inc vf-idx) (count vt-instants))]
    {:kind kind, :id id
     :valid-from (nth vt-instants vf-idx)
     ;; one past the end stands for an open-ended range
     :valid-to (nth vt-instants vt-idx nil)}))

(defn- ->tx-op [idx {:keys [kind id valid-from valid-to]}]
  (case kind
    :put [:put-docs {:into :docs, :valid-from valid-from, :valid-to valid-to} {:xt/id id, :v idx}]
    :delete [:delete-docs {:from :docs, :valid-from valid-from, :valid-to valid-to} id]
    :erase [:erase-docs :docs id]))

;; a DELETE against a table that doesn't exist yet is a planning error, so the table is seeded rather
;; than left to whichever op the generator puts first. Entity 0 is outside the generated id range.
(def ^:private seed-op
  {:kind :put, :id 0, :valid-from (first vt-instants), :valid-to nil})

(defn- submit-ops!
  "Submits one transaction per op — so each gets its own system time — and returns those times."
  [node ops]
  (mapv (fn [[idx op]] (.getSystemTime (xt/execute-tx node [(->tx-op idx op)])))
        (map-indexed vector (cons seed-op ops))))

(def ^:private cols "_id, v, _valid_from, _valid_to, _system_from, _system_to")

(defn- ->sql-ts [inst] (str (time/->instant inst)))

(defn- as-of-rows [node v s]
  (set (xt/q node (format "SELECT %s FROM docs
                           FOR VALID_TIME AS OF TIMESTAMP '%s'
                           FOR SYSTEM_TIME AS OF TIMESTAMP '%s'"
                          cols (->sql-ts v) (->sql-ts s)))))

(defn- full-resolution-rows
  "The same question routed through the polygon resolver: widening valid time to ALL takes it off the
   as-of path, while the system-time bound and the basis stay as the as-of query has them, so both see
   the same events and treat an erase above the bound alike.

   Widening system time instead would not be an oracle — the polygon resolver returns before applyLog
   for events above the system bound, so an all-system-time query cuts the same winner into more
   segments carrying a non-null _system_to."
  [node v s]
  (set (xt/q node (format "SELECT %s FROM docs
                           FOR ALL VALID_TIME
                           FOR SYSTEM_TIME AS OF TIMESTAMP '%s'
                           WHERE _valid_from <= TIMESTAMP '%s'
                             AND (_valid_to > TIMESTAMP '%s' OR _valid_to IS NULL)"
                          cols (->sql-ts s) (->sql-ts v) (->sql-ts v)))))

(defn- ->node [] (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                 :compactor {:threads 0}}))

;; The resolvers are compared against each other directly in xtdb.operator.scan.AsOfResolverTest, which
;; can reach them — they're `internal`. What only a node can reach is everything around them: the
;; planner's temporal defaults, which pages `filter-pages` admits, and the flushed-and-compacted layout.
(t/deftest ^:property as-of-resolution-agrees-through-page-filtering-and-compaction
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [ops (gen/vector op-gen 4 24)
                  storage (gen/elements [:live :flushed])
                  vt-choice (gen/choose 0 (dec (count vt-instants)))
                  st-choice (gen/choose 0 999)]
                 (with-open [node (->node)]
                   (let [sys-times (submit-ops! node ops)]
                     (when (= :flushed storage)
                       (tu/flush-block! node)
                       (c/compact-all! node #xt/duration "PT30S"))

                     (let [v (nth vt-instants vt-choice)
                           s (nth sys-times (mod st-choice (count sys-times)))]
                       (= (full-resolution-rows node v s)
                          (as-of-rows node v s))))))))

(defn- check-agreement [node ops]
  (let [sys-times (submit-ops! node ops)]
    (doseq [v vt-instants
            s sys-times]
      (t/is (= (full-resolution-rows node v s)
               (as-of-rows node v s))
            (format "as of valid-time %s, system-time %s" v s)))))

(t/deftest erase-above-the-system-bound-still-hides-the-entity
  (with-open [node (->node)]
    (check-agreement node [{:kind :put, :id 1, :valid-from #inst "2018", :valid-to nil}
                           {:kind :put, :id 1, :valid-from #inst "2020", :valid-to #inst "2021"}
                           {:kind :erase, :id 1}])))

(t/deftest delete-as-the-winner-emits-nothing
  (with-open [node (->node)]
    (check-agreement node [{:kind :put, :id 1, :valid-from #inst "2018", :valid-to nil}
                           {:kind :delete, :id 1, :valid-from #inst "2020", :valid-to #inst "2021"}])))

(t/deftest neighbouring-ranges-bracket-the-winners-valid-time
  (with-open [node (->node)]
    (check-agreement node [{:kind :put, :id 1, :valid-from #inst "2018", :valid-to nil}
                           {:kind :put, :id 1, :valid-from #inst "2019", :valid-to #inst "2020"}
                           {:kind :put, :id 1, :valid-from #inst "2022", :valid-to #inst "2023"}])))

(t/deftest events-above-the-bound-do-not-bracket-the-winner
  (with-open [node (->node)]
    (check-agreement node [{:kind :put, :id 1, :valid-from #inst "2018", :valid-to nil}
                           {:kind :put, :id 1, :valid-from #inst "2020", :valid-to #inst "2021"}
                           {:kind :put, :id 1, :valid-from #inst "2021", :valid-to #inst "2022"}])))

(t/deftest one-entitys-events-span-several-pages
  (with-open [node (->node)]
    (let [ops [{:kind :put, :id 1, :valid-from #inst "2018", :valid-to nil}
               {:kind :put, :id 1, :valid-from #inst "2019", :valid-to #inst "2020"}]
          sys-times (submit-ops! node ops)]
      (tu/flush-block! node)

      (let [later (submit-ops! node [{:kind :put, :id 1, :valid-from #inst "2021", :valid-to #inst "2022"}])]
        (doseq [v vt-instants
                s (concat sys-times later)]
          (t/is (= (full-resolution-rows node v s)
                   (as-of-rows node v s))
                (format "as of valid-time %s, system-time %s" v s)))))))
