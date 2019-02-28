(ns crux.backup
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [crux.io :as crux-io]
            [crux.kv :as kv]
            [clojure.java.shell :refer [sh]])
  (:import [java.io File]))

(s/def ::checkpoint-directory string?)
(s/def ::backend keyword?)

(s/def ::system-options
  (s/keys :req-un [:crux.kv/db-dir]
          :req [::checkpoint-directory ::backend]
          :opt-un [:crux.tx/event-log-dir]))

(defprotocol ISystemBackup
  (write-checkpoint [this system-options]))

(defmulti upload-to-backend ::backend)

(defmulti download-from-backend ::backend)

(defn check-and-restore
  [{:keys [event-log-dir db-dir] :as system-options ::keys [checkpoint-directory]}]
  (s/assert ::system-options system-options)
  (when-not (or (some-> event-log-dir io/file .exists)
                (some-> db-dir io/file .exists))
    (let [^File checkpoint-directory (io/file checkpoint-directory)]
      (log/infof "no data attempting restore from backup" (.getPath checkpoint-directory))
      (sh "mkdir" "-p" (.getPath (io/file checkpoint-directory)))
      (download-from-backend system-options)

      (when db-dir
        (sh "mkdir" "-p" (.getParent (io/file db-dir)))
        (sh "mv"
            (.getPath (io/file checkpoint-directory "kv-store"))
            (.getPath (io/file db-dir))))

      (when event-log-dir
        (sh "mkdir" "-p" (.getParent (io/file event-log-dir)))
        (sh "mv"
            (.getPath (io/file checkpoint-directory "event-log-kv-store"))
            (.getPath (io/file event-log-dir))))

      (crux-io/delete-dir checkpoint-directory))))

(defn backup-current-version
  [{:keys [::checkpoint-directory] :as system-options} crux]
  (locking crux
    (let [checkpoint-directory (io/file checkpoint-directory)]
      (crux-io/delete-dir checkpoint-directory)
      (sh "mkdir" "-p" (.getPath (io/file checkpoint-directory)))
      (log/infof "creating checkpoint for crux backup: %s" (.getPath checkpoint-directory))
      (write-checkpoint crux system-options)
      (upload-to-backend system-options)
      (log/infof "successfully uploaded crux checkpoint")
      (crux-io/delete-dir checkpoint-directory))))

(comment
  (kv/backup kv-store (io/file checkpoint-dir "kv-store"))
  (when event-log-kv-store
    (kv/backup event-log-kv-store (io/file checkpoint-dir "event-log-kv-store"))))

(defmethod upload-to-backend ::sh
  [{::keys [^File checkpoint-directory sh-backup-script]}]
  (let [{:keys [exit] :as shell-response}
        (sh sh-backup-script
            :env (merge {"CRUX_CHECKPOINT_DIRECTORY" checkpoint-directory}
                        (System/getenv)))]
    (when-not (= exit 0) (throw (ex-info "backup-script failed" shell-response)))))

(defmethod download-from-backend ::sh
  [{::keys [^File checkpoint-directory sh-restore-script]}]
  (let [{:keys [exit] :as shell-response}
        (sh sh-restore-script
            :env (merge {"CRUX_CHECKPOINT_DIRECTORY" checkpoint-directory}
                        (System/getenv)))]
    (when-not (= exit 0) (throw (ex-info "restore script failed" shell-response)))))
