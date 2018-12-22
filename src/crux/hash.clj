(ns crux.hash
  (:require [clojure.tools.logging :as log]
            [crux.memory :as mem])
  (:import [org.agrona DirectBuffer ExpandableDirectByteBuffer]
           org.agrona.concurrent.UnsafeBuffer
           java.security.MessageDigest
           java.util.function.Supplier))

(def ^:const ^:private gcrypt-enabled? (not (boolean (System/getenv "CRUX_DISABLE_LIBGCRYPT"))))

;; NOTE: Using name without dash as it's supported both by
;; MessageDigest and libgcrypt.
(def ^:const id-hash-algorithm "SHA1")
(def ^:private ^MessageDigest id-digest-prototype (MessageDigest/getInstance id-hash-algorithm))
(def ^:const id-hash-size (.getDigestLength id-digest-prototype))

(declare id-hash)

;; NOTE: Allowing on-heap buffer here for now.
(defn- message-digest-id-hash-buffer ^DirectBuffer [^DirectBuffer buffer]
    (let [^MessageDigest md (try
                              (.clone id-digest-prototype)
                              (catch CloneNotSupportedException e
                                (MessageDigest/getInstance id-hash-algorithm)))]
      (UnsafeBuffer. (.digest md (mem/->on-heap buffer)))))

(defn- message-digest-id-hash ^bytes [^bytes bytes]
  (mem/->on-heap (message-digest-id-hash-buffer (mem/as-buffer bytes))))

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
(defn try-init-gcrypt-hash! []
  (eval
   '(do
      (definterface GCrypt
        (^int gcry_md_map_name [^String name])
        (^int gcry_md_get_algo_dlen [^int algo])
        (^void gcry_md_hash_buffer [^int algo
                                    ^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} digest
                                    ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} buffer
                                    ^{jnr.ffi.types.size_t true :tag int} length]))

      (def ^:private ^crux.hash.GCrypt gcrypt (.load (jnr.ffi.LibraryLoader/create GCrypt) "gcrypt"))
      (def ^:private ^jnr.ffi.Runtime gcrypt-rt (jnr.ffi.Runtime/getRuntime gcrypt))

      (def ^:private ^:const gcrypt-hash-algo (.gcry_md_map_name gcrypt id-hash-algorithm))
      (assert (not (zero? gcrypt-hash-algo))
              (str "libgcrypt does not support algorithm: " id-hash-algorithm))

      (def ^:private ^:const gcrypt-hash-dlen (.gcry_md_get_algo_dlen gcrypt gcrypt-hash-algo))
      (assert (= gcrypt-hash-dlen id-hash-size)
              (format "libgcrypt and MessageDigest disagree on digest size: %d %d"
                      gcrypt-hash-dlen id-hash-size))


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
                                (jnr.ffi.Pointer/wrap gcrypt-rt (.addressOffset digest))
                                (jnr.ffi.Pointer/wrap gcrypt-rt (.addressOffset buffer))
                                (mem/capacity buffer))
          digest))

      (defn- gcrypt-id-hash ^bytes [^bytes bytes]
        (mem/->on-heap (gcrypt-id-hash-buffer (mem/ensure-off-heap bytes (.get buffer-tl))))))))

(defn- jnr-available? []
  (try
    (import 'jnr.ffi.Pointer)
    true
    (catch ClassNotFoundException e
      false)))

(try
  (when (and gcrypt-enabled?
             jnr-available?
             (not (bound? #'id-hash)))
    (try-init-gcrypt-hash!)
    (log/info "Using libgcrypt for ID hashing."))
  (catch Throwable t
    (log/warn t "Could not load libgcrypt, using java.security.MessageDigest for ID hashing.")))

(def id-hash (or (resolve 'gcrypt-id-hash) message-digest-id-hash))
