(ns crux.topology
  (:require [com.stuartsierra.dependency :as dep]))

(defn- add-depends [g deps]
  (reduce (fn [g [node dep]]
            (dep/depend g node dep))
          g
          deps))

(defn- resolve-modules
  "Given a list of topologies
  - resolves wraps/deps relationships,
  - resolves subsequent topologies overriding modules from earlier topologies"
  [topologies]

  (->> topologies
       (mapcat (fn [topology]
                 (for [[k module] topology]
                   (assoc module :module-key k))))

       (reduce (fn [{:keys [modules graph] :as acc} {:keys [module-key deps wraps] :as module}]
                 ;; when we're overriding a module, we want to remove
                 ;; - the overridden module
                 ;; - the wrappers of that modules
                 ;; - modules that directly depend on the wrapping modules

                 (let [to-remove (when-let [{:keys [wrappers]} (get modules module-key)]
                                   (into (set wrappers)
                                         (mapcat #(dep/transitive-dependents graph %)
                                                 wrappers)))]

                   (-> acc
                       (cond-> to-remove (-> (update :modules #(apply dissoc % to-remove))
                                             (update :graph #(reduce dep/remove-all % to-remove))))

                       (assoc-in [:modules module-key] (select-keys module [:start-fn :deps :wraps]))
                       (update :graph add-depends (map vector (repeat module-key) deps))
                       (cond-> wraps (-> (update-in [:modules wraps :wrappers] (fnil conj []) module-key)
                                         (update :graph dep/depend module-key wraps))))))

               {:modules {}
                :graph (dep/graph)})

       :modules))

(defn- module-start-graph [modules]
  ;; some rules:
  ;; - if a node depends on a wrapped node, we want it to depend on the
  ;;   last of the wrappers rather than the node itself
  ;; - if a node wraps another node, we want it to depend on the previous wrapper

  (add-depends (dep/graph)
               (concat #_(for [module-key (keys modules)]
                           [::system module-key])

                       (for [[module-key {:keys [deps wraps]}] modules
                             dep (cond-> deps wraps (conj wraps))]
                         [module-key (or (->> (:wrappers (get modules dep))
                                              (take-while #(not= % module-key))
                                              last)
                                         dep)]))))

;; Topology structure
;; {;; module name
;;  :node3 {;; start-fn returns record that is java.io.Closeable
;;          :start-fn (fn [deps args])
;;          :deps #{:node2}
;;          ;; Optionally
;;          :wraps :node1}}
;; Args structure
;; {:node3/kv-store "rocks"
;;  :someother-thing/test? true}

;; When a dependancy is wrapped, it loses its original key in the dependancy map
;; This needs to be reverted.
;; This function finds any oblect that has been wrapped, collates its wrappers,
;; and does a find->repalace for any wrapper keyword
;; - if key ∌ original deps, resolve wrapper
(defn revert-dependancy-keys [resolved-modules start-graph module new-deps]
  (let [old-deps (get-in resolved-modules [module :deps])]
    (into {} (map (fn [[k v]] (if (contains? old-deps k)
                                [k v]
                                [(or (get-in resolved-modules [k :wraps]) k) v]))
                  new-deps))))

;; TODO add args
;; ^crux.api.ICruxAPI
(defn start-system [topologies args]
  (let [resolved-modules (resolve-modules topologies)
        start-graph (module-start-graph resolved-modules)
        start-order (dep/topo-sort start-graph)
        revert-keys (partial revert-dependancy-keys resolved-modules start-graph)]
    (reduce (fn [started-modules module]
              (let [resolved-module (get resolved-modules module)
                    start-fn (get resolved-module :start-fn)
                    ;; for each of the orginal deps, resolve them wrt wrappers
                    dependencies (get-in start-graph [:dependencies module])
                    resolved-dependencies (select-keys started-modules dependencies)]
                (assoc started-modules
                       module
                       (start-fn (revert-keys module resolved-dependencies)
                                 (into {}
                                       (filter (fn [[arg _]]
                                                 (= (namespace arg) (str (namespace module) "." (name module))))
                                               args))))))
            {} ;; No modules are started initially
            start-order)))
