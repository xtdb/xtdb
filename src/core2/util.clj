(ns core2.util
  (:require [clojure.java.io :as io])
  (:import java.nio.ByteBuffer
           java.nio.channels.SeekableByteChannel
           [java.nio.file Files FileVisitResult SimpleFileVisitor]
           java.util.Date
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

(defn local-date-time->date ^java.util.Date [^LocalDateTime ldt]
  (Date/from (.toInstant (.atZone ldt (ZoneId/of "UTC")))))
