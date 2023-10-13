(ns xtdb.object-store.arrow-writer
  (:require [xtdb.util :as util])
  (:import (java.nio ByteBuffer)
           (java.nio.channels WritableByteChannel)
           (java.util.concurrent CompletableFuture)
           (org.apache.arrow.vector.ipc ArrowWriter)))

(def min-part-size (* 5 1024 1024))
(def maximum-write-ahead 4)

(defn start-upload [os object-key])
(defn upload-part [os upload part-number src])
(defn complete-upload [os upload parts])
(defn abort-upload [os upload])
(defn put-object [os object-key src])

(defn initial-state [object-key buffer-capacity]
  (assert (<= min-part-size buffer-capacity) (format "buffer capacity must be at least min-part-size (%d)" min-part-size))
  {:open true
   :object-key object-key
   :buffer (ByteBuffer/allocateDirect buffer-capacity)
   :parts []})

(defn pending-part-count [state]
  (let [{:keys [parts]} @state]
    (count (remove future-done? parts))))

(defn apply-back-pressure [state]
  (while (<= maximum-write-ahead (pending-part-count state))
    ;; sleep?
    (when (.isInterrupted (Thread/currentThread))
      (throw (InterruptedException.)))))

(defn bubble-async-exception [state]
  (let [{:keys [^CompletableFuture upload parts]} state]
    (when (and upload (.isCompletedExceptionally upload)) @upload)
    (doseq [^CompletableFuture part parts
            :when (.isCompletedExceptionally part)]
      @part)))

(defn start-upload-if-not-started [os state]
  (let [{:keys [upload, object-key]} @state]
    (when-not upload
      (swap! state assoc :upload (start-upload os object-key)))))

(defn upload-byte-buffer [os state ^ByteBuffer src]
  (start-upload-if-not-started os state)
  (let [{:keys [upload, parts]} @state
        out-buf (ByteBuffer/allocateDirect (.remaining src))
        _ (.put out-buf (.duplicate src))
        part-number (count parts)
        new-part-fut (util/then-compose upload (fn [u] (upload-part os u part-number out-buf)))
        new-parts (conj parts new-part-fut)]
    (swap! state update :parts new-parts)))

(defn upload-current-buffer [os state]
  (bubble-async-exception state)
  (apply-back-pressure state)
  (let [{:keys [^ByteBuffer buffer]} @state]
    (upload-byte-buffer os state (.flip buffer))
    (.clear buffer)
    nil))

(defn buffered-byte-count [state] (.position ^ByteBuffer (:buffer @state)))

(defn flush-and-complete-upload [os state]
  (when (pos? (buffered-byte-count state)) (upload-current-buffer os state))
  (let [{:keys [upload, parts]} @state]
    @(complete-upload os @upload (mapv deref parts))))

(defn close [os state]
  (let [{:keys [^ByteBuffer buffer, object-key, upload]} @state]
    (if upload
      (flush-and-complete-upload os state)
      (do @(put-object os object-key (.flip (.duplicate buffer)))
          (.clear buffer)))
    (swap! state assoc :open false)))

(defn append-byte-buffer-if-space-remaining [state ^ByteBuffer src]
  (let [{:keys [^ByteBuffer buffer]} @state]
    (if (<= (.remaining src) (.remaining buffer))
      (do (.put buffer (.duplicate src)) true)
      false)))

(defn grow-buffer-capacity-if-not-enough [state n]
  (let [{:keys [^ByteBuffer buffer]} @state]
    (when (< (.capacity buffer) n)
      (let [new-buffer (ByteBuffer/allocateDirect n)]
        (.put new-buffer (.flip buffer))
        (swap! state assoc :buffer new-buffer)))))

(defn write [os state ^ByteBuffer src]
  (assert (:open @state))
  (grow-buffer-capacity-if-not-enough state (.remaining src))
  (when-not (append-byte-buffer-if-space-remaining state src)
    (upload-current-buffer os state)
    (recur os state src)))

(defn handle-error [os state t]
  (let [{:keys [upload, parts]} @state]
    (doseq [part parts
            :when (not (future-done? part))]
      (future-cancel part))
    (when upload (util/then-compose upload (partial abort-upload os))))
  (swap! state assoc :open false, :error t))

(defrecord ObjectStoreWritableByteChannel [os state]
  WritableByteChannel
  (isOpen [_] (:open @state))
  (close [_]
    (try
      (close os state)
      (catch Throwable t
        (handle-error os state t)
        (throw t))))
  (write [_ src]
    (try
      (write os state src)
      (.position src (.limit src))
      (catch Throwable t
        (handle-error os state t)
        (throw t)))))

(defn signal-end-of-record-batch [os-wbc]
  (let [{:keys [os, state]} os-wbc]
    (when (<= min-part-size (buffered-byte-count state))
      (upload-current-buffer os state))))

(defn object-store-writable-byte-channel [os object-key buffer-capacity]
  (->ObjectStoreWritableByteChannel os (atom (initial-state object-key buffer-capacity))))

(defn arrow-writer [os object-key buffer-capacity vsr]
  (let [os-wbc (object-store-writable-byte-channel os object-key buffer-capacity)]
    (proxy [ArrowWriter] [vsr nil os-wbc]
      (writeBatch []
        (let [^ArrowWriter this this] (proxy-super writeBatch))
        (signal-end-of-record-batch os-wbc) nil))))
