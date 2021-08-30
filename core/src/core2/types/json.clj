(ns core2.types.json
  (:require [clojure.data.json :as json]
            [clojure.spec.alpha :as s])
  (:import [org.apache.arrow.vector.types Types$MinorType]))

(s/def :json/null nil?)
(s/def :json/boolean boolean?)
(s/def :json/number-float double?)
(s/def :json/number-int int?)
(s/def :json/string string?)

(s/def :json/array (s/coll-of :json/value :kind vector?))
(s/def :json/object (s/map-of keyword? :json/value))

(s/def :json/value (s/or :json/null :json/null
                         :json/boolean :json/boolean
                         :json/number-float :json/number-float
                         :json/number-int :json/number-int
                         :json/string :json/string
                         :json/array :json/array
                         :json/object :json/object))

(def json-hierarchy
  (reduce
   (fn [acc tag]
     (derive acc tag :json/scalar))
   (make-hierarchy)
   [:json/null :json/boolean :json/number-float :josn/number-int :json/string]))

(def json->arrow {:json/null Types$MinorType/NULL
                  :json/boolean Types$MinorType/BIT
                  :json/number-float Types$MinorType/FLOAT8
                  :json/number-int Types$MinorType/BIGINT
                  :json/string Types$MinorType/VARCHAR
                  :json/list Types$MinorType/LIST
                  :json/object Types$MinorType/STRUCT})
