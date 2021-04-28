(ns user
  (:require [clojure.tools.namespace.repl :as ctn]))

(ctn/disable-reload!)

(defn reset []
  (ctn/refresh))

(defn dev []
  (require 'dev)
  (in-ns 'dev))
