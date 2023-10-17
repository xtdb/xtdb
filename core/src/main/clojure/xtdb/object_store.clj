(ns xtdb.object-store)

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IMultipartUpload
  (^java.util.concurrent.CompletableFuture #_<Void> uploadPart [^java.nio.ByteBuffer buf] 
   "Asynchonously uploads a part to the multipart request - adds to the internal completed parts list")
  (^java.util.concurrent.CompletableFuture #_<Void> complete []
   "Asynchonously completes the multipart-request")
  (^java.util.concurrent.CompletableFuture #_<?> abort []
   "Asynchonously cancels the multipart-request - useful/necessary to cleanup any parts of the multipart upload on an error"))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface SupportsMultipart
  (^java.util.concurrent.CompletableFuture #_<IMultipartUpload> startMultipart [^String k]))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ObjectStore
  (^java.util.concurrent.CompletableFuture #_<ByteBuffer> getObject [^String k]
                                                                    "Asynchonously returns the given object in a ByteBuffer
    If the object doesn't exist, the CF completes with an IllegalStateException.")

  (^java.util.concurrent.CompletableFuture #_<ByteBuffer> getObjectRange
   [^String k ^long start ^long len]
   "Asynchonously returns the given len bytes starting from start (inclusive) of the object in a ByteBuffer
    If the object doesn't exist, the CF completes with an IllegalStateException.

    Out of bounds `start` cause the returned future to complete with an Exception, the type of which is implementation dependent.
    If you supply a len that exceeds the number of bytes remaining (from start) then you will receive less bytes than len.

    Exceptions are thrown immediately if the start is negative, or the len is zero or below. This is
    to ensure consistent boundary behaviour between different object store implementations. You should check for these conditions and deal with them
    before calling getObjectRange.

    Behaviour for a start position at or exceeding the byte length of the object is undefined. You may or may not receive an exception.")

  (^java.util.concurrent.CompletableFuture #_<Path> getObject [^String k, ^java.nio.file.Path out-path]
                                                              "Asynchronously writes the object to the given path.
    If the object doesn't exist, the CF completes with an IllegalStateException.")

  (^java.util.concurrent.CompletableFuture #_<?> putObject [^String k, ^java.nio.ByteBuffer buf]) 
  (^java.lang.Iterable #_<String> listObjects [])
  (^java.lang.Iterable #_<String> listObjects [^String dir])
  (^java.util.concurrent.CompletableFuture #_<?> deleteObject [^String k]))

(defn ensure-shared-range-oob-behaviour [^long i ^long len]
  (when (< i 0)
    (throw (IndexOutOfBoundsException. "Negative range indexes are not permitted")))
  (when (< len 1)
    (throw (IllegalArgumentException. "Negative or zero range requests are not permitted"))))

(defn obj-missing-exception [k]
  (IllegalStateException. (format "Object '%s' doesn't exist." k)))
