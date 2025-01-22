(ns xtdb.file-log
  (:require [cognitect.transit :as transit]
            xtdb.protocols
            [xtdb.util :as util])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.nio.file Path]
           (xtdb.api.log FileLog$Notification)
           (xtdb.api.storage ObjectStore$StoredObject)))

(defrecord StoredObject [k size]
  ObjectStore$StoredObject
  (getKey [_] k)
  (getSize [_] size))

(defrecord FileNotification [added deleted]
  FileLog$Notification
  (getAdded [_] added))

(defn addition [k size]
  (FileNotification. [(->StoredObject k size)] []))

(def ^:private transit-write-handlers
  {FileNotification (transit/write-handler "xtdb/file-notification" #(into {} %))
   Path (transit/write-handler "xtdb/path" (fn [^Path path]
                                             (str path)))})

(defn file-notification->transit [n]
  (with-open [os (ByteArrayOutputStream.)]
    (let [w (transit/writer os :msgpack {:handlers transit-write-handlers})]
      (transit/write w n))
    (.toByteArray os)))

(def ^:private transit-read-handlers
  {"xtdb/file-notification" (transit/read-handler (fn [{:keys [added deleted]}]
                                                    (->FileNotification (->> added (mapv map->StoredObject)) deleted)))
   "xtdb/path" (transit/read-handler (fn [path-str]
                                       (util/->path path-str)))})

(defn transit->file-notification [bytes]
  (with-open [in (ByteArrayInputStream. bytes)]
    (let [rdr (transit/reader in :msgpack {:handlers transit-read-handlers})]
      (transit/read rdr))))
