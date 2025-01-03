(ns xtdb.file-list-cache
  (:require [cognitect.transit :as transit]
            xtdb.protocols
            [xtdb.util :as util])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.nio.file Path]
           (xtdb.api.log FileListCache$Notification)))

(defrecord FileNotification [added deleted]
  FileListCache$Notification)

(defn addition [k size]
  (FileNotification. [{:k k, :size size}] []))

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
  {"xtdb/file-notification" (transit/read-handler map->FileNotification)
   "xtdb/path" (transit/read-handler (fn [path-str]
                                       (util/->path path-str)))})

(defn transit->file-notification [bytes]
  (with-open [in (ByteArrayInputStream. bytes)]
    (let [rdr (transit/reader in :msgpack {:handlers transit-read-handlers})]
      (transit/read rdr))))
