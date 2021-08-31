(ns xtdb.checkpoint
  (:require [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.io :as xio]
            [xtdb.system :as sys])
  (:import [java.io Closeable File]
           java.net.URI
           java.nio.charset.StandardCharsets
           [java.nio.file CopyOption Files FileVisitOption LinkOption OpenOption Path Paths StandardOpenOption]
           java.nio.file.attribute.FileAttribute
           [java.time Duration Instant]
           [java.util.concurrent Executors TimeUnit]
           java.util.Date))

(defprotocol CheckpointStore
  (available-checkpoints [store opts])
  (download-checkpoint [store checkpoint dir])
  (upload-checkpoint [store dir opts]))

(alter-meta! #'available-checkpoints assoc :arglists '([store {:keys [::cp-format]}]))
(alter-meta! #'upload-checkpoint assoc :arglists '([store dir {:keys [::cp-format tx]}]))

(defprotocol CheckpointSource
  (save-checkpoint [_ dir]))

(defprotocol Checkpointer
  (try-restore [checkpointer dir cp-format])
  (start ^java.io.Closeable [checkpointer src opts]))

(alter-meta! #'start assoc :arglists '([checkpointer src {:keys [::cp-format]}]))

(defn recent-cp? [{::keys [^Date checkpoint-at]} ^Duration approx-frequency]
  (and checkpoint-at
       (< (.getSeconds (Duration/between (.toInstant checkpoint-at) (Instant/now)))
          (/ (.getSeconds approx-frequency) 2))))

(defn checkpoint [{:keys [dir src store ::cp-format approx-frequency]}]
  (when-not (recent-cp? (first (available-checkpoints store {::cp-format cp-format})) approx-frequency)
    (try
      (when-let [{:keys [tx]} (save-checkpoint src dir)]
        (when tx
          (log/infof "Uploading checkpoint at '%s'" tx)
          (doto (upload-checkpoint store dir {:tx tx, ::cp-format cp-format})
            (->> pr-str (log/info "Uploaded checkpoint:")))))
      (finally
        (xio/delete-dir dir)))))

(defn cp-seq [^Instant start ^Duration freq]
  (lazy-seq
   (cons (.plus start (Duration/ofSeconds (rand-int (.getSeconds freq))))
         (cp-seq (.plus start freq) freq))))

(defrecord ScheduledCheckpointer [store, ^Path checkpoint-dir, ^Duration approx-frequency
                                  keep-dir-between-checkpoints? keep-dir-on-close?]
  Checkpointer
  (try-restore [_ dir cp-format]
    (when (or (not (.exists ^File dir))
              (empty? (.listFiles ^File dir)))
      (log/debug "checking for checkpoints to restore from")
      (when-let [cp (first (available-checkpoints store {::cp-format cp-format}))]
        (log/infof "restoring from %s to %s" cp dir)
        (download-checkpoint store cp dir)
        cp)))

  (start [this src {::keys [cp-format]}]
    (let [checkpoint-dir (or (some-> checkpoint-dir .toFile) (xio/create-tmpdir "checkpointing"))
          ses (Executors/newSingleThreadScheduledExecutor (xio/thread-factory "xtdb-checkpoint"))]
      (letfn [(run [[time & more-times]]
                (try
                  (checkpoint {:approx-frequency approx-frequency,
                               :dir checkpoint-dir,
                               :src src,
                               :store store,
                               ::cp-format cp-format})
                  (catch Exception e
                    (log/warn e "Checkpointing failed"))
                  (catch Throwable t
                    (log/warn t "Checkpointing failed, stopping checkpointing")
                    (throw t))
                  (finally
                    (when-not keep-dir-between-checkpoints?
                      (xio/delete-dir checkpoint-dir))))
                (schedule more-times))
              (schedule [times]
                (when (seq times)
                  (let [now (Instant/now)]
                    (if (.isAfter now (first times))
                      (recur (rest times))
                      (.schedule ses
                                 ^Runnable #(run times)
                                 (.toMillis (Duration/between now (first times)))
                                 TimeUnit/MILLISECONDS)))))]
        (schedule (cp-seq (Instant/now) approx-frequency)))
      (reify Closeable
        (close [_]
          (.shutdownNow ses)
          (when-not keep-dir-on-close?
            (xio/delete-dir checkpoint-dir)))))))

(defn ->checkpointer {::sys/deps {:store {:xtdb/module (fn [_])}}
                      ::sys/args {:checkpoint-dir {:spec ::sys/path
                                                   :required? false}
                                  :keep-dir-between-checkpoints? {:spec ::sys/boolean
                                                                  :required? true
                                                                  :default true}
                                  :keep-dir-on-close? {:spec ::sys/boolean
                                                       :required? true
                                                       :default false}
                                  :approx-frequency {:spec ::sys/duration
                                                     :required? true}}}
  [opts]
  (map->ScheduledCheckpointer opts))

(defn- sync-path [^Path from-root-path ^Path to-root-path]
  (doseq [^Path from-path (-> (Files/walk from-root-path Integer/MAX_VALUE (make-array FileVisitOption 0))
                              .iterator
                              iterator-seq)
          :let [to-path (.resolve to-root-path (.relativize from-root-path from-path))]]
    (cond
      (Files/isDirectory from-path (make-array LinkOption 0))
      (Files/createDirectories to-path (make-array FileAttribute 0))

      (Files/isRegularFile from-path (make-array LinkOption 0))
      (Files/copy from-path to-path ^"[Ljava.nio.file.CopyOption;" (make-array CopyOption 0)))))

(defrecord FileSystemCheckpointStore [^Path root-path]
  CheckpointStore
  (available-checkpoints [_ {::keys [cp-format]}]
    (when (Files/exists root-path (make-array LinkOption 0))
      (for [metadata-path (->> (Files/newDirectoryStream root-path "checkpoint-*.edn")
                               .iterator iterator-seq
                               (sort #(compare %2 %1)))
            ;; Files/readString only added in JDK 11
            :let [cp (-> (Files/readAllBytes metadata-path)
                         (String. StandardCharsets/UTF_8)
                         read-string
                         (update ::cp-path #(Paths/get (URI. %))))]
            :when (= cp-format (::cp-format cp))]
        cp)))

  (download-checkpoint [_ {::keys [cp-path]} dir]
    (let [to-path (.toPath ^File dir)]
      (when-not (or (not (Files/exists to-path (make-array LinkOption 0)))
                    (empty? (Files/list to-path)))
        (throw (IllegalArgumentException. "non-empty checkpoint restore dir: " to-path)))

      (try
        (sync-path cp-path to-path)
        (catch Exception e
          (throw (ex-info e "incomplete checkpoint restore"
                          {:cp-path cp-path
                           :local-dir to-path}))))))

  (upload-checkpoint [_ dir {:keys [tx ::cp-format]}]
    (let [from-path (.toPath ^File dir)
          cp-at (java.util.Date.)
          cp-prefix (format "checkpoint-%s-%s" (::xt/tx-id tx) (xio/format-rfc3339-date cp-at))
          to-path (.resolve root-path cp-prefix)]
      (sync-path from-path to-path)

      (let [cp {::cp-format cp-format,
                :tx tx
                ::cp-uri (str to-path)
                ::checkpoint-at cp-at}]
        (Files/write (.resolve root-path (str cp-prefix ".edn"))
                     (.getBytes (pr-str {::cp-format cp-format,
                                         :tx tx
                                         ::cp-path (str (.toUri to-path))
                                         ::checkpoint-at cp-at})
                                StandardCharsets/UTF_8)
                     ^"[Ljava.nio.file.OpenOption;"
                     (into-array OpenOption #{StandardOpenOption/WRITE StandardOpenOption/CREATE_NEW}))
        cp))))

(defn ->filesystem-checkpoint-store {::sys/args {:path {:spec ::sys/path, :required? true}}} [{:keys [path]}]
  (->FileSystemCheckpointStore path))
