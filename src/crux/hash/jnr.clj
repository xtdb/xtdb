(ns crux.hash.jnr
  (:require [crux.hash :as hash]
            [crux.memory :as mem])
  (:import [org.agrona DirectBuffer ExpandableDirectByteBuffer]
           org.agrona.concurrent.UnsafeBuffer
           java.security.MessageDigest
           java.util.function.Supplier
           jnr.ffi.Pointer))

;; TODO: Spike using libgcrypt to do the SHA1 native, can be 30%
;; faster than MessageDigest, but not necessarily with sane realistic
;; allocation patterns, might be easier to integrate once everything
;; is in buffers already.

;; Worth exploring a bit as we calculate a lot of hashes. Would allow
;; us to get rid of the pre-pending step of the id type id as well to
;; the digest buffer as we control the offsets here. We would fallback
;; to MessageDigest if we cannot load libgcrypt. It's available by
;; default in Ubuntu 16.04 and likely many other distros.

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

(def ^:private ^ThreadLocal digest-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (mem/allocate-buffer gcrypt-hash-dlen)))))

(def ^:private ^ThreadLocal buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

(defn- gcrypt-id-hash-buffer ^org.agrona.DirectBuffer [^DirectBuffer buffer]
  (let [^DirectBuffer digest (.get digest-tl)
        ^DirectBuffer buffer (if (mem/off-heap? buffer)
                               buffer
                               (mem/ensure-off-heap buffer (.get buffer-tl)))]
    (.gcry_md_hash_buffer gcrypt
                          gcrypt-hash-algo
                          (Pointer/wrap gcrypt-rt (.addressOffset digest))
                          (Pointer/wrap gcrypt-rt (.addressOffset buffer))
                          (mem/capacity buffer))
    digest))

(defn gcrypt-id-hash ^bytes [^bytes bytes]
  (mem/->on-heap (gcrypt-id-hash-buffer (mem/ensure-off-heap bytes (.get buffer-tl)))))
