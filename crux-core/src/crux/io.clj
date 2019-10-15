(ns crux.io
  (:require [clojure.instant :as instant]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [taoensso.nippy :as nippy])
  (:import [java.io Closeable DataInputStream DataOutputStream File IOException Reader]
           [java.lang.ref PhantomReference ReferenceQueue]
           java.net.ServerSocket
           [java.nio.file Files FileVisitResult SimpleFileVisitor]
           java.nio.file.attribute.FileAttribute
           java.text.SimpleDateFormat
           java.time.Duration
           [java.util Comparator Date IdentityHashMap PriorityQueue Properties]))

(s/def ::port (s/int-in 1 65536))

;; TODO: Replace with java.lang.ref.Cleaner in Java 9.
;; We currently still support Java 8.
(def ^:private ^ReferenceQueue reference-queue (ReferenceQueue.))
(def ^:private ^IdentityHashMap ref->cleanup-action (IdentityHashMap.))

(defn- cleanup-loop []
  (try
    (loop [ref (.remove reference-queue)]
      (try
        (when-let [cleanup-action (.remove ref->cleanup-action ref)]
          (cleanup-action))
        (catch Exception e
          (log/error e "Error while running cleaner:")))
      (recur (.remove reference-queue)))
    (catch InterruptedException _)))

(def ^:private ^Thread cleaner-thread
  (do (when (and (bound? #'cleaner-thread) cleaner-thread)
        (.interrupt ^Thread cleaner-thread))
      (doto (Thread. ^Runnable cleanup-loop "crux.io.cleaner-thread")
        (.setDaemon true))))

(defn register-cleaner [object action]
  (locking cleaner-thread
    (when-not (.isAlive cleaner-thread)
      (.start cleaner-thread)))
  (.put ref->cleanup-action (PhantomReference. object reference-queue) action))

(def ^:private last-monotonic-date (atom (Date.)))

(defn next-monotonic-date ^java.util.Date []
  (let [date (Date.)
        old-date @last-monotonic-date]
    (if (and (pos? (compare date old-date))
             (compare-and-set! last-monotonic-date old-date date))
      date
      (do (Thread/sleep 1)
          (recur)))))

(defn format-rfc3339-date [^Date d]
  (when d
    (.format ^SimpleDateFormat (.get ^ThreadLocal @#'instant/thread-local-utc-date-format) d)))

(defn parse-rfc3339-or-millis-date [d]
  (if (re-find #"^\d+$" d)
    (Date. (Long/parseLong d))
    (instant/read-instant-date d)))

(defn format-duration-millis [^Duration d]
  (.toMillis d))

(defn free-port ^long []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

(def ^:private files-to-delete (atom []))

(def file-deletion-visitor
  (proxy [SimpleFileVisitor] []
    (visitFile [file _]
      (Files/delete file)
      FileVisitResult/CONTINUE)

    (postVisitDirectory [dir _]
      (Files/delete dir)
      FileVisitResult/CONTINUE)))

(defn delete-dir [dir]
  (let [dir (io/file dir)]
    (when (.exists dir)
      (Files/walkFileTree (.toPath dir) file-deletion-visitor))))

(defn create-tmpdir ^java.io.File [dir-name]
  (let [f (.toFile (Files/createTempDirectory dir-name (make-array FileAttribute 0)))
        known-files (swap! files-to-delete conj f)]
    (when (= 1 (count known-files))
      (.addShutdownHook
       (Runtime/getRuntime)
       (Thread. (fn []
                  (doseq [f @files-to-delete]
                    (delete-dir f)))
                "crux.io.shutdown-hook-thread")))

    f))

(defn folder-size
  "Total size of a file or folder in bytes"
  [^java.io.File f]
  (cond
    (nil? f) 0
    (string? f) (folder-size (io/file f))
    (.isDirectory f) (apply + (map folder-size (.listFiles f)))
    :else (.length f)))

(defn try-close [^Closeable c]
  (try
    (some-> c (.close))
    (catch Throwable t
      (log/error t "Could not close:" c))))

(defn wait-while [p timeout]
  (let [timeout-at (+ timeout (System/currentTimeMillis))]
    (loop []
      (if (p)
        (do (Thread/sleep 100)
            (if (>= (System/currentTimeMillis) timeout-at)
              false
              (recur)))
        true))))

(defn load-properties [^Reader in]
  (->> (doto (Properties.)
         (.load in))
       (into {})))

(defn native-image? []
  (boolean (System/getProperty "org.graalvm.nativeimage.kind")))

;; External Merge Sort

(defn- new-merge-sort-priority-queue ^PriorityQueue [comp sorted-seqs]
  (let [sorted-seqs (remove empty? sorted-seqs)
        pq-comp (reify Comparator
                  (compare [_ [a] [b]]
                    (comp a b)))]
    (doto (PriorityQueue. (count sorted-seqs) pq-comp)
      (.addAll sorted-seqs))))

(defn- merge-sort-priority-queue->seq [^PriorityQueue pq]
  ((fn step []
     (lazy-seq
      (let [[x & xs] (.poll pq)]
        (when x
          (when xs
            (.add pq xs))
          (cons x (step))))))))

(defn merge-sort
  ([sorted-seqs]
   (merge-sort compare sorted-seqs))
  ([comp sorted-seqs]
   (->> (new-merge-sort-priority-queue comp sorted-seqs)
        (merge-sort-priority-queue->seq))))

(def ^:const default-external-sort-part-size (* 1024 1024))

(defn external-sort
  ([seq]
   (external-sort compare seq))
  ([comp seq]
   (external-sort comp seq default-external-sort-part-size))
  ([comp seq external-sort-part-size]
   (let [parts (partition-all external-sort-part-size seq)]
     (if (nil? (second parts))
       (sort comp (first parts))
       (let [files (->> parts
                        (reduce
                         (fn [acc part]
                           (let [file (doto (File/createTempFile "crux-external-sort" ".nippy")
                                        (.deleteOnExit))]
                             (with-open [out (DataOutputStream. (io/output-stream file))]
                               (doseq [x (sort comp part)]
                                 (nippy/freeze-to-out! out x)))
                             (conj acc file)))
                         []))
             seq+cleaner-actions (for [^File file files]
                                   (let [in (DataInputStream. (io/input-stream file))
                                         cleaner-action (fn []
                                                          (.close in)
                                                          (.delete file))
                                         seq ((fn step []
                                                (lazy-seq
                                                 (try
                                                   (when-let [x (nippy/thaw-from-in! in)]
                                                     (cons x (step)))
                                                   (catch Exception e
                                                     (cleaner-action)
                                                     (if (or (instance? IOException e)
                                                             (instance? IOException (.getCause e)))
                                                       nil
                                                       (throw e)))))))]
                                     [seq cleaner-action]))
             pq (->> (map first seq+cleaner-actions)
                     (new-merge-sort-priority-queue comp))]
         (doseq [[_ cleaner-action] seq+cleaner-actions]
           (register-cleaner pq cleaner-action))
         (merge-sort-priority-queue->seq pq))))))
