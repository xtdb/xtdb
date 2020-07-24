(ns ^:no-doc crux.hash.jnr
  (:require [clojure.tools.logging :as log]
            [crux.hash :as hash]
            [crux.memory :as mem])
  (:import [org.agrona DirectBuffer ExpandableDirectByteBuffer MutableDirectBuffer]
           org.agrona.concurrent.UnsafeBuffer
           java.security.MessageDigest
           java.util.function.Supplier
           [jnr.ffi LibraryLoader Memory NativeType Pointer]))

;; NOTE: The data we hash is currently always on heap, strings,
;; keywords etc. So we currently always first have to copy the data
;; off heap before hashing it, so we don't gain the potential speed up
;; from using this. The target is always off heap.

(def ^ThreadLocal buffer-tl (ThreadLocal/withInitial
                             (reify Supplier
                               (get [_]
                                 (ExpandableDirectByteBuffer.)))))

;; Uses libgcrypt to do the SHA1 native, can be 30% faster than
;; MessageDigest, but not necessarily with sane realistic allocation
;; patterns, might be easier to integrate once everything is in
;; buffers already. It's available by default in Ubuntu 16.04 and
;; likely many other distros.

;; See:
;; https://www.gnupg.org/documentation/manuals/gcrypt/Hashing.html#Hashing
;; https://ubuntuforums.org/archive/index.php/t-337664.html

;; Not properly initialised, requires varargs for gcry_control:
;; https://www.gnupg.org/documentation/manuals/gcrypt/Initializing-the-library.html#Initializing-the-library
;; https://github.com/gpg/libgcrypt/blob/master/src/gcrypt.h.in

(definterface GCrypt
  (^String gcry_check_version [^String req-version])
  (^int gcry_md_map_name [^String name])
  (^int gcry_md_get_algo_dlen [^int algo])
  (^void gcry_md_hash_buffer [^int algo
                              ^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} digest
                              ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} buffer
                              ^{jnr.ffi.types.size_t true :tag int} length]))

(defn init-gcrypt []
  (try
    (let [^GCrypt gcrypt (.load (LibraryLoader/create GCrypt) "gcrypt")
          gcrypt-rt (jnr.ffi.Runtime/getRuntime gcrypt)
          _ (.gcry_check_version gcrypt nil)
          gcrypt-hash-algo (.gcry_md_map_name gcrypt hash/id-hash-algorithm)]
      (if (zero? gcrypt-hash-algo)
        (log/warn "libgcrypt does not support algorithm: " hash/id-hash-algorithm)
        (let [gcrypt-hash-dlen (.gcry_md_get_algo_dlen gcrypt gcrypt-hash-algo)]
          (if-not (= gcrypt-hash-dlen hash/id-hash-size)
            (log/warnf "libgcrypt and MessageDigest disagree on digest size: %d %d"
                       gcrypt-hash-dlen hash/id-hash-size)
            (fn gcrypt-id-hash-buffer ^org.agrona.DirectBuffer [^MutableDirectBuffer to ^DirectBuffer buffer]
              (let [buffer (if (mem/off-heap? buffer)
                             buffer
                             (mem/ensure-off-heap buffer (.get buffer-tl)))]
                (.gcry_md_hash_buffer gcrypt
                                      gcrypt-hash-algo
                                      (Pointer/wrap gcrypt-rt (.addressOffset to))
                                      (Pointer/wrap gcrypt-rt (.addressOffset buffer))
                                      (mem/capacity buffer))
                (mem/limit-buffer to hash/id-hash-size)))))))
    (catch UnsatisfiedLinkError t)))

(def gcrypt-id-hash-buffer (init-gcrypt))

;; https://wiki.openssl.org/index.php/EVP_Message_Digests
;; https://www.openssl.org/docs/man1.1.0/crypto/EVP_DigestInit.html

(definterface OpenSSL
  (^void OpenSSL_add_all_digests [])
  (^jnr.ffi.Pointer EVP_MD_CTX_create [])
  (^int EVP_MD_CTX_init [^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} ctx])
  (^void EVP_MD_CTX_destroy [^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} ctx])
  (^jnr.ffi.Pointer EVP_sha1 [])
  (^jnr.ffi.Pointer EVP_get_digestbyname [^String name])
  (^int EVP_DigestInit_ex [^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} ctx
                           ^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} type
                           ^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} impl])
  (^int EVP_DigestUpdate [^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} ctx
                          ^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} d
                          ^{jnr.ffi.types.size_t true :tag int} cnt])
  (^int EVP_MD_size [^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} md])
  (^int EVP_DigestFinal_ex [^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} ctx
                            ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} md
                            ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} s]))

(defn init-openssl []
  (try
    (let [^OpenSSL openssl (.load (LibraryLoader/create OpenSSL) "crypto")
          openssl-rt (jnr.ffi.Runtime/getRuntime openssl)
          openssl-digest (do (.OpenSSL_add_all_digests openssl)
                             (.EVP_get_digestbyname openssl hash/id-hash-algorithm))]
      (if (nil? openssl-digest)
        (log/warn "OpenSSL does not support algorithm: " hash/id-hash-algorithm)
        (let [openssl-md-size (.EVP_MD_size openssl openssl-digest)]
          (if-not (= openssl-md-size hash/id-hash-size)
            (log/warnf "OpenSSL and MessageDigest disagree on digest size: %d %d"
                       openssl-md-size hash/id-hash-size)
            (let [openssl-md-ctx-tl (ThreadLocal/withInitial
                                     (reify Supplier
                                       (get [_]
                                         (.EVP_MD_CTX_create openssl))))]
              (fn openssl-id-hash-buffer ^org.agrona.DirectBuffer [^MutableDirectBuffer to ^DirectBuffer buffer]
                (let [buffer (if (mem/off-heap? buffer)
                               buffer
                               (mem/ensure-off-heap buffer (.get buffer-tl)))
                      ctx (.get openssl-md-ctx-tl)
                      ^MutableDirectBuffer to (mem/limit-buffer to openssl-md-size)]
                  (assert ctx)
                  (assert (pos? (.EVP_DigestInit_ex openssl ctx openssl-digest nil)))
                  (assert (pos? (.EVP_DigestUpdate openssl ctx (Pointer/wrap openssl-rt (.addressOffset buffer)) (mem/capacity buffer))))
                  (assert (pos? (.EVP_DigestFinal_ex openssl ctx (Pointer/wrap openssl-rt (.addressOffset to)) nil)))
                  to)))))))
    (catch UnsatisfiedLinkError t)))

(def openssl-id-hash-buffer (init-openssl))
