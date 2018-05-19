(ns user)

(defn dev []
  (require 'dev :reload-all)
  (in-ns 'dev)
  :ok)
