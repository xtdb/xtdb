(ns crux.hash.jnr
  (:require [crux.hash :as hash]
            [crux.memory :as mem])
  (:import [org.agrona DirectBuffer ExpandableDirectByteBuffer MutableDirectBuffer]
           org.agrona.concurrent.UnsafeBuffer
           java.security.MessageDigest
           java.util.function.Supplier
           [jnr.ffi Memory NativeType Pointer]))

;; NOTE: The data we hash is currently always on heap, strings,
;; keywords etc. So we currently always first have to copy the data
;; off heap before hashing it, so we don't gain the potential speed up
;; from using this. The target is always off heap.

;; Uses libgcrypt to do the SHA1 native, can be 30% faster than
;; MessageDigest, but not necessarily with sane realistic allocation
;; patterns, might be easier to integrate once everything is in
;; buffers already. It's available by default in Ubuntu 16.04 and
;; likely many other distros.

;; See:
;; https://www.gnupg.org/documentation/manuals/gcrypt/Hashing.html#Hashing
;; https://ubuntuforums.org/archive/index.php/t-337664.html

(definterface GCrypt
  (^int gcry_md_map_name [^String name])
  (^int gcry_md_get_algo_dlen [^int algo])
  (^void gcry_md_hash_buffer [^int algo
                              ^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} digest
                              ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} buffer
                              ^{jnr.ffi.types.size_t true :tag int} length]))

(def ^:private ^GCrypt gcrypt (.load (jnr.ffi.LibraryLoader/create GCrypt) "gcrypt"))
(def ^:private ^jnr.ffi.Runtime gcrypt-rt (jnr.ffi.Runtime/getRuntime gcrypt))

(def ^:private ^:const gcrypt-hash-algo (.gcry_md_map_name gcrypt hash/id-hash-algorithm))
(assert (not (zero? gcrypt-hash-algo))
        (str "libgcrypt does not support algorithm: " hash/id-hash-algorithm))

(def ^:private ^:const gcrypt-hash-dlen (.gcry_md_get_algo_dlen gcrypt gcrypt-hash-algo))
(assert (= gcrypt-hash-dlen hash/id-hash-size)
        (format "libgcrypt and MessageDigest disagree on digest size: %d %d"
                gcrypt-hash-dlen hash/id-hash-size))

(def ^:private ^ThreadLocal buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

(defn gcrypt-id-hash-buffer ^org.agrona.DirectBuffer [^MutableDirectBuffer to ^DirectBuffer buffer]
  (let [^DirectBuffer buffer (if (mem/off-heap? buffer)
                               buffer
                               (mem/ensure-off-heap buffer (.get buffer-tl)))]
    (.gcry_md_hash_buffer gcrypt
                          gcrypt-hash-algo
                          (Pointer/wrap gcrypt-rt (.addressOffset to))
                          (Pointer/wrap gcrypt-rt (.addressOffset buffer))
                          (mem/capacity buffer))
    (mem/limit-buffer to hash/id-hash-size)))

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

(def ^:private ^OpenSSL openssl (.load (jnr.ffi.LibraryLoader/create OpenSSL) "crypto"))
(def ^:private ^jnr.ffi.Runtime openssl-rt (jnr.ffi.Runtime/getRuntime openssl))
(def ^:private ^jnr.ffi.Pointer openssl-digest (do (.OpenSSL_add_all_digests openssl)
                                                   (.EVP_get_digestbyname openssl hash/id-hash-algorithm)))
(assert openssl-digest (str "OpenSSL does not support algorithm: " hash/id-hash-algorithm))

(def ^:private ^{:tag 'long} openssl-md-size (.EVP_MD_size openssl openssl-digest))
(assert (= openssl-md-size hash/id-hash-size)
        (format "OpenSSL and MessageDigest disagree on digest size: %d %d"
                openssl-md-size hash/id-hash-size))

(def ^:private ^ThreadLocal openssl-md-ctx-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (.EVP_MD_CTX_create openssl)))))

(defn openssl-id-hash-buffer ^org.agrona.DirectBuffer [^MutableDirectBuffer to ^DirectBuffer buffer]
  (let [^DirectBuffer buffer (if (mem/off-heap? buffer)
                               buffer
                               (mem/ensure-off-heap buffer (.get buffer-tl)))
        ctx (.get openssl-md-ctx-tl)
        ^MutableDirectBuffer to (mem/limit-buffer to openssl-md-size)]
    (assert ctx)
    (assert (pos? (.EVP_DigestInit_ex openssl ctx openssl-digest nil)))
    (assert (pos? (.EVP_DigestUpdate openssl ctx (Pointer/wrap openssl-rt (.addressOffset buffer)) (mem/capacity buffer))))
    (assert (pos? (.EVP_DigestFinal_ex openssl ctx (Pointer/wrap openssl-rt (.addressOffset to)) nil)))
    to))
