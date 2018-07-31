(ns crux.io
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [taoensso.nippy :as nippy])
  (:import [java.nio.file Files FileVisitResult SimpleFileVisitor]
           [java.nio.file.attribute FileAttribute]
           [java.io DataInputStream DataOutputStream File IOException]
           [java.lang.ref ReferenceQueue PhantomReference]
           [java.util Comparator Date IdentityHashMap PriorityQueue]
           [java.net ServerSocket]))

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
          (log/error "Error while running cleaner:" e)))
      (recur (.remove reference-queue)))
    (catch InterruptedException _)))

(def ^:private ^Thread cleaner-thread
  (do (when (and (bound? #'cleaner-thread) cleaner-thread)
        (.interrupt ^Thread cleaner-thread))
      (doto (Thread. ^Runnable cleanup-loop "crux.io.cleaner-thread")
        (.setDaemon true))))

(defn register-cleaner [object action]
  (when-not (.isAlive cleaner-thread)
    (.start cleaner-thread))
  (.put ref->cleanup-action (PhantomReference. object reference-queue) action))

(def ^:private last-monotonic-date (atom (Date.)))

(defn next-monotonic-date ^java.util.Date []
  (let [date (Date.)
        old-date @last-monotonic-date]
    (if (and (not= date old-date)
             (compare-and-set! last-monotonic-date old-date date))
      date
      (do (Thread/sleep 1)
          (recur)))))

(defn free-port ^long []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

(defn create-tmpdir ^java.io.File [dir-name]
  (.toFile (Files/createTempDirectory dir-name (make-array FileAttribute 0))))

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

(defn folder-size
  "Total size of a file or folder in bytes"
  [^java.io.File f]
  (cond
    (string? f) (folder-size (io/file f))
    (.isDirectory f) (apply + (map folder-size (.listFiles f)))
    :default (.length f)))

(def units {:KB 1000
            :MB 1000000
            :GB 1000000000
            :TB 1000000000000
            :PB 1000000000000000})

;; There are more elegant ways to do this but they'd require more imports.
(defn ->human-size
  "Converts byte units for human readability."
  ([bytes]
   (if (< bytes 1000)
     (str bytes " B")
     (let [unit (last (filter #(>= bytes (% units)) (keys units)))]
       (->human-size bytes unit))))

  ([bytes unit]
   (as-> bytes b
     (/ b (unit units))
     (double b)
     (format "%.3f" b)
     (str b " " (name unit)))))

(defn folder-human-size [f]
  (->human-size (folder-size f)))

;; External Merge Sort

(defn- new-merge-sort-priority-queue ^PriorityQueue [comp sorted-seqs]
  (let [sorted-seqs (remove empty? sorted-seqs)
        pq-comp (reify Comparator
                  (compare [_ [a] [b]]
                    (comp a b)))]
    (doto (PriorityQueue. (count sorted-seqs) pq-comp)
      (.addAll sorted-seqs))))

(defn- merge-sort-pirority-queue->seq [^PriorityQueue pq]
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
        (merge-sort-pirority-queue->seq))))

(def ^:const external-sort-chunk-size (* 1024 1024))

(defn external-sort
  ([seq]
   (external-sort compare seq))
  ([comp seq]
   (let [chunks (->> (partition-all external-sort-chunk-size seq)
                     (map sort))]
     (if (nil? (second chunks))
       (first chunks)
       (let [files (->> chunks
                        (map (fn [chunk]
                                (let [file (doto (File/createTempFile "crux-external-sort" ".nippy")
                                             (.deleteOnExit))]
                                  (with-open [out (DataOutputStream. (io/output-stream file))]
                                    (doseq [x chunk]
                                      (nippy/freeze-to-out! out x)))
                                  file))))
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
         (merge-sort-pirority-queue->seq pq))))))
