(ns crux.decorators.core
  (:require [crux.api :as api]))

(defrecord CruxDecoratorNode [methods decorated])
(defrecord CruxDecoratorDataSource [methods decorated])

(defn default-delegate-method
  [method-var]
  (fn delegate [decorator & args]
    (if-let [overide-method (get-in decorator [:methods method-var])]
      (apply overide-method (:decorated decorator) args)
      (apply method-var (:decorated decorator) args))))

(defn extend-default-method
  [atype protocol & {:as overides}]
  (extend atype
    protocol
    (into
      {}
      (for [[k] (:method-map protocol)]
        (if-let [overide (get overides k)]
          [k overide]
          [k (default-delegate-method
               (some
                 (fn [v]
                   (when (= (name k) (name (.-sym ^clojure.lang.Var v))) v))
                 (keys (:method-builders protocol))))])))))

(extend-default-method
  CruxDecoratorNode api/PCruxNode
  :db (fn [{:keys [decorated methods] :as decorator} & args]
        (map->CruxDecoratorDataSource
          {:methods (:data-source methods)
           :decorated (apply (default-delegate-method #'api/db) decorator args)})))

(extend-default-method CruxDecoratorDataSource api/PCruxDatasource)

(defn node-decorator
  [overide-methods]
  (fn decorator [decorated]
    (map->CruxDecoratorNode
      {:methods overide-methods
       :decorated decorated})))
