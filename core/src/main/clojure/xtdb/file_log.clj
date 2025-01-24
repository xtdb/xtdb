(ns xtdb.file-log
  (:require [cognitect.transit :as transit]
            [xtdb.object-store :as os]
            xtdb.protocols
            [xtdb.util :as util])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.nio.file Path]
           (xtdb.api.log FileLog$Notification)
           (xtdb.api.storage ObjectStore$StoredObject)))

(defrecord FileNotification [added deleted]
  FileLog$Notification
  (getAdded [_] added))

(defn addition [k size]
  (FileNotification. [(os/->StoredObject k size)] []))

(def ^:private transit-write-handlers
  {FileNotification (transit/write-handler "xtdb/file-notification" #(into {} %))
   ObjectStore$StoredObject (transit/write-handler "xtdb/stored-object" (fn [^ObjectStore$StoredObject obj]
                                                                          {:k (.getKey obj)
                                                                           :size (.getSize obj)}))
   Path (transit/write-handler "xtdb/path" str)})

(defn file-notification->transit [n]
  (with-open [os (ByteArrayOutputStream.)]
    (let [w (transit/writer os :msgpack {:handlers transit-write-handlers})]
      (transit/write w n))
    (.toByteArray os)))

(def ^:private transit-read-handlers
  {"xtdb/file-notification" (transit/read-handler map->FileNotification)
   "xtdb/stored-object" (transit/read-handler os/map->StoredObject)
   "xtdb/path" (transit/read-handler util/->path)})

(defn transit->file-notification [bytes]
  (with-open [in (ByteArrayInputStream. bytes)]
    (let [rdr (transit/reader in :msgpack {:handlers transit-read-handlers})]
      (transit/read rdr))))
