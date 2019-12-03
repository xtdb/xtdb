(ns webservice.graphql
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [com.walmartlabs.lacinia :as lac]
            [com.walmartlabs.lacinia.schema :as schema]
            [com.walmartlabs.lacinia.util :as util]
            [crux.api :as c]
            [webservice.util :as u]))

(def translation-map
  {:crew/id :crux.db/id
   :crew/name :crew/name
   :crew/rank :crew/rank
   :crew/species :crew/species
   :crew/homeplanet :crew/home-planet
   :planet/id :crux.db/id
   :planet/status :planet/status
   :planet/name :planet/name
   :planet/color :planet/color
   :planet/quadrant :planet/quadrant})

(defn translate [namesp field]
  (get translation-map (keyword namesp (name field))))

(defn normalize [value]
  (cond-> value
    (s/starts-with? value ":")
    (-> (subs 1) keyword)))

(defn context->fields [context]
  ;; TODO theres got to be a better way to write this˯
  (map :field (-> context
                  :com.walmartlabs.lacinia.constants/parsed-query
                  second
                  second
                  first
                  :selections)))

(defn form-query [object]
  (fn [context args value]
    ;; Extract out requrested field
    ;; Map graphql args => crux.db args
    (let [node (:node context)
          fields (context->fields context)
          elems (mapv (fn [field] (gensym (name field))) fields)
          fields-cx (map (partial translate object) fields)
          wheres (mapv (fn [field elem] [(symbol object) field elem]) fields-cx elems)
          where-args (mapv (fn [[k v]] [(symbol object) (translate object k) (normalize v)]) args)
          query {:find elems
                 :where (vec (concat where-args wheres))}]
      ;; NOTE keywords don't really work
      (map #(zipmap fields (map str %)) (c/q (c/db node) query)))))

(def res-map
  {:query/crew (form-query "crew")
   :query/planet (form-query "planet")})

(defn load-schema
  [file]
  (-> (edn/read-string
       (slurp (io/resource file)))))

(defn compile-schema
  [s]
  (-> s
      (util/attach-resolvers res-map)
      schema/compile))

(defn handle-req
  [req schema node]
  {:status 200
   :headers {"Content-Type" "application/json"}
   :body (let [query (get (json/read-str (slurp (:body req))) "query")
               result (lac/execute schema query nil {:node node})]
           (json/write-str result))})

(defn- test-data-schemas []
  (-> (load-schema "smalldataschema.edn")
      (compile-schema)))

(defn ->graphql-handler
  [crux-node]
  (let [schema (test-data-schemas)]
    (fn [request]
      (handle-req request schema crux-node))))
