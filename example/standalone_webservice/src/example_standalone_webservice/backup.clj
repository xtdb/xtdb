(ns example-standalone-webservice.backup
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [crux.io :as crux-io]
            [crux.kv :as kv])
  (:import
   [java.io File]
   [java.nio.file Files StandardCopyOption Path]
   [java.util.zip ZipEntry ZipOutputStream]))

(defmulti upload-to-backend :backend)

(defmulti download-from-backend :backend)

(defn check-and-restore
  [{:keys [event-log-dir db-dir] :as system-options} backend-options]
  (when-not (or (-> event-log-dir io/file .exists)
                (-> db-dir io/file .exists))
    (log/info "no existing db on disk trying to restore from backup")
    (download-from-backend backend-options)))

(defn zip-folder
  [dir target-dir]
  (with-open [zip (ZipOutputStream. (io/output-stream target-dir))]
    (doseq [f (file-seq dir) :when (.isFile f)]
      (.putNextEntry zip (ZipEntry. (.getPath f)))
      (io/copy f zip)
      (.closeEntry zip))))

(defn backup-current-version
  [{:keys [kv-store event-log-kv-store] :as crux-system}
   {:keys [event-log-dir db-dir] :as system-options}
   {:keys [backup-dir] :as backup-options}]
  (locking crux-system
    (let [backup-dir (io/file backup-dir)
          checkpoint-dir (io/file backup-dir "checkpoint")
          compressed-checkpoint (io/file backup-dir (format "checkpoint.zip"))]
      (when-not (.exists backup-dir) (.mkdir backup-dir))
      (crux-io/delete-dir checkpoint-dir)
      (.mkdir checkpoint-dir)
      (when (.exists compressed-checkpoint) (.delete compressed-checkpoint))
      (log/infof "creating checkpoint for crux backup: %s" (.getPath checkpoint-dir))

      (kv/backup kv-store (io/file checkpoint-dir "kv-store"))
      (when event-log-kv-store
        (kv/backup event-log-kv-store (io/file checkpoint-dir "event-log-kv-store")))

      (zip-folder checkpoint-dir compressed-checkpoint)

      (upload-to-backend
        (merge backup-options {:checkpoint-file compressed-checkpoint})))))

(defmethod upload-to-backend :file
  [{:keys [file/path file/replace? checkpoint-file]
    :or {replace? false}}]
  (let [backup-folder (io/file path)
        new-file (io/file
                   backup-folder
                   (if replace?
                     "backup.zip"
                     (format "backup-%s.zip" (.getTime (java.util.Date.)))))]
    (when-not (.exists backup-folder) (.mkdir backup-folder))
    (when (.exists new-file) (.delete new-file))
    (io/copy checkpoint-file new-file)))
