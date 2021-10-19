(ns core2.transit
  (:require [cognitect.transit :as transit]
            [core2.api :as c2]
            [core2.edn :as edn]
            [core2.error :as err])
  (:import core2.api.TransactionInstant
           [java.time Duration Instant ZonedDateTime]))

(def tj-read-handlers
  {"core2/duration" (transit/read-handler edn/duration-reader)
   "core2/instant" (transit/read-handler edn/instant-reader)
   "core2/zoned-date-time" (transit/read-handler edn/zdt-reader)
   "core2/tx-key" (transit/read-handler c2/map->TransactionInstant)
   "core2/illegal-arg" (transit/read-handler err/-iae-reader)})

(def tj-write-handlers
  {Duration (transit/write-handler "core2/duration" str)
   Instant (transit/write-handler "core2/instant" str)
   ZonedDateTime (transit/write-handler "core2/zoned-date-time" str)
   TransactionInstant (transit/write-handler "core2/tx-key" #(select-keys % [:tx-id :tx-time]))
   core2.IllegalArgumentException (transit/write-handler "core2/illegal-arg" ex-data)})
