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

(s/def ::node-options
  (s/keys :req-un [:crux.kv/db-dir]
          :gen    [:backup-dir]
          :opt-un [:crux.standalone/event-log-dir]))

(s/def ::node-options-w-backend
  (s/keys :req-un [:crux.kv/db-dir]
          :req [::checkpoint-directory ::backend]
          :opt-un [:crux.standalone/event-log-dir]))

(defprotocol INodeBackup
  (write-checkpoint [this node-options]))

(defmulti upload-to-backend ::backend)

(defmethod upload-to-backend :default [sys-opts]
  (log/info "skipping upload to backend"))

(defmulti download-from-backend ::backend)

(defmethod download-from-backend :default [sys-opts]
  (log/info "skipping download from backend"))

(defn restore
  [{:keys [event-log-dir db-dir backup-dir] :as node-options}]
 ;(s/assert ::node-options node-options)
  (if (or (some-> event-log-dir io/file .exists) (some-> db-dir io/file .exists))
    (log/info "found existing data dirs, restore aborted")
    (let [^File checkpoint-directory (io/file backup-dir)]
      (log/infof "no data : attempting restore from backup" (.getPath checkpoint-directory))
      (sh "mkdir" "-p" (.getPath (io/file checkpoint-directory)))
      (let [db-dir-resp
              (when db-dir
                (sh "mkdir" "-p" (.getParent (io/file db-dir)))
                (sh "cp" "-r"
                    (.getPath (io/file checkpoint-directory "kv-store"))
                    (.getPath (io/file db-dir))))
            el-dir-resp
              (when event-log-dir
                (sh "mkdir" "-p" (.getParent (io/file event-log-dir)))
                (sh "cp" "-r"
                    (.getPath (io/file checkpoint-directory "event-log-kv-store"))
                    (.getPath (io/file event-log-dir))))
            dbd-good?     (= 0 (:exit db-dir-resp))
            eld-good? (= 0 (:exit el-dir-resp))]
        (if (and dbd-good? eld-good?)
          (log/info "restore successful")
          (log/info "restore had issues"
                    db-dir-resp
                    (if-not dbd-good?
                      "; db-dir not written")
                    el-dir-resp
                    (if-not dbd-good?
                      "; evt-dir not written")))))))

(defn backup
  [{:keys [backup-dir] :as node-options} crux-node]
 ;(s/assert ::node-options node-options)
  (locking crux-node
    (let [checkpoint-directory (io/file backup-dir)
          node-options (assoc node-options ::checkpoint-directory backup-dir)]
      (crux-io/delete-dir backup-dir)
      (sh "mkdir" "-p" (.getPath (io/file backup-dir)))
      (log/infof "creating checkpoint for crux backup: %s" (.getPath checkpoint-directory))
      (write-checkpoint crux-node node-options)
      (log/infof "checkpoint written for crux backup: %s" (.getPath checkpoint-directory)))))

(defn check-and-restore
  [{:keys [event-log-dir db-dir] :as node-options ::keys [checkpoint-directory]}]
  (s/assert ::node-options-w-backend node-options)
  (when-not (or (some-> event-log-dir io/file .exists)
                (some-> db-dir io/file .exists))
    (let [^File checkpoint-directory (io/file checkpoint-directory)]
      (log/infof "no data attempting restore from backup" (.getPath checkpoint-directory))
      (sh "mkdir" "-p" (.getPath (io/file checkpoint-directory)))
      (download-from-backend node-options)

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
  [{:keys [::checkpoint-directory] :as node-options} crux]
  (s/assert ::node-options-w-backend node-options)
  (locking crux
    (let [checkpoint-directory (io/file checkpoint-directory)]
      (crux-io/delete-dir checkpoint-directory)
      (sh "mkdir" "-p" (.getPath (io/file checkpoint-directory)))
      (log/infof "creating checkpoint for crux backup: %s" (.getPath checkpoint-directory))
      (write-checkpoint crux node-options)
      (upload-to-backend node-options)
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
    (when-not (= exit 0)
      (throw (ex-info "backup-script failed" shell-response)))
    (log/infof "successfully uploaded crux checkpoint")))

(defmethod download-from-backend ::sh
  [{::keys [^File checkpoint-directory sh-restore-script]}]
  (let [{:keys [exit] :as shell-response}
        (sh sh-restore-script
            :env (merge {"CRUX_CHECKPOINT_DIRECTORY" checkpoint-directory}
                        (System/getenv)))]
    (when-not (= exit 0)
      (throw (ex-info "restore script : download failed" shell-response)))))
