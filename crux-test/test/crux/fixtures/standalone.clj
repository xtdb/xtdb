(ns crux.fixtures.standalone
  (:require [crux.fixtures :as fix]
            [crux.io :as cio]
            [crux.standalone :as standalone]
            [crux.node :as n]))

(defn ^:deprecated with-standalone-node [f]
  (fix/with-standalone-topology f))

(defn ^:deprecated with-standalone-doc-store [f]
  (fix/with-standalone-doc-store f))
