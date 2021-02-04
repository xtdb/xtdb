(ns core2.util
  (:require [clojure.java.io :as io])
  (:import java.io.File
           java.nio.ByteBuffer
           [java.nio.channels FileChannel SeekableByteChannel]
           [java.nio.file Files FileVisitResult OpenOption StandardOpenOption SimpleFileVisitor Path]
           java.nio.file.attribute.FileAttribute
           java.util.Date
           [java.util.function Supplier Function]
           java.util.concurrent.CompletableFuture
           [java.time LocalDateTime ZoneId]))

(defn ->seekable-byte-channel ^java.nio.channels.SeekableByteChannel [^ByteBuffer buffer]
  (let [buffer (.duplicate buffer)]
    (proxy [SeekableByteChannel] []
      (isOpen []
        true)

      (close [])

      (read [^ByteBuffer dst]
        (let [^ByteBuffer src (-> buffer (.slice) (.limit (.remaining dst)))]
          (.put dst src)
          (let [bytes-read (.position src)]
            (.position buffer (+ (.position buffer) bytes-read))
            bytes-read)))

      (position
        ([]
         (.position buffer))
        ([^long new-position]
         (.position buffer new-position)
         this))

      (size []
        (.capacity buffer))

      (write [src]
        (throw (UnsupportedOperationException.)))

      (truncate [size]
        (throw (UnsupportedOperationException.))))))

(defn open-write-file-ch ^java.nio.channels.FileChannel [^File file]
  (FileChannel/open (.toPath file)
                    (into-array OpenOption #{StandardOpenOption/CREATE
                                             StandardOpenOption/WRITE
                                             StandardOpenOption/TRUNCATE_EXISTING})))

(def ^:private file-deletion-visitor
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

(defn mkdirs [^Path path]
  (Files/createDirectories path (make-array FileAttribute 0)))

(def ^:private ^ZoneId utc (ZoneId/of "UTC"))

(defn local-date-time->date ^java.util.Date [^LocalDateTime ldt]
  (Date/from (.toInstant (.atZone ldt utc))))

(defn ->supplier {:style/indent :defn} ^java.util.function.Supplier [f]
  (reify Supplier
    (get [_]
      (f))))

(defn ->jfn {:style/indent :defn} ^java.util.function.Function [f]
  (reify Function
    (apply [_ v]
      (f v))))

(defn then-apply {:style/indent :defn} [^CompletableFuture fut f]
  (.thenApply fut (->jfn f)))

(defn then-compose {:style/indent :defn} [^CompletableFuture fut f]
  (.thenCompose fut (->jfn f)))

(defmacro completable-future {:style/indent 1} [pool & body]
  `(CompletableFuture/supplyAsync (->supplier (fn [] ~@body)) ~pool))
