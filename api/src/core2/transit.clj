(ns core2.transit
  (:require [cognitect.transit :as transit]
            [core2.api :as c2])
  (:import core2.api.TransactionInstant
           java.time.Duration))

(def tj-read-handlers
  {"core2/duration" (transit/read-handler #(Duration/parse %))
   "core2/tx-instant" (transit/read-handler c2/map->TransactionInstant)})

(def tj-write-handlers
  {Duration (transit/write-handler "core2/duration" str)
   TransactionInstant (transit/write-handler "core2/tx-instant" #(into {} %))})
