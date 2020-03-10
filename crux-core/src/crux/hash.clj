(ns crux.hash
  (:require [clojure.tools.logging :as log]
            [crux.memory :as mem])
  (:import [org.agrona DirectBuffer MutableDirectBuffer]
           org.agrona.concurrent.UnsafeBuffer
           java.security.MessageDigest
           crux.ByteUtils))

(def ^:const ^:private gcrypt-enabled? (not (Boolean/parseBoolean (System/getenv "CRUX_DISABLE_LIBGCRYPT"))))
(def ^:const ^:private openssl-enabled? (not (Boolean/parseBoolean (System/getenv "CRUX_DISABLE_LIBCRYPTO"))))
(def ^:const ^:private byte-utils-sha1-enabled? (Boolean/parseBoolean (System/getenv "CRUX_ENABLE_BYTEUTILS_SHA1")))

;; NOTE: Using name without dash as it's supported both by
;; MessageDigest and libgcrypt.
(def ^:const id-hash-algorithm "SHA1")
(def ^:private ^MessageDigest id-digest-prototype (MessageDigest/getInstance id-hash-algorithm))
(def ^:const id-hash-size (.getDigestLength id-digest-prototype))

;; NOTE: Allowing on-heap buffer here for now.
(defn message-digest-id-hash-buffer ^org.agrona.DirectBuffer [^MutableDirectBuffer to ^DirectBuffer buffer]
  (let [^MessageDigest md (try
                            (.clone id-digest-prototype)
                            (catch CloneNotSupportedException e
                              (MessageDigest/getInstance id-hash-algorithm)))]
    (doto ^MutableDirectBuffer (mem/limit-buffer to id-hash-size)
      (.putBytes 0 (.digest md (mem/->on-heap buffer))))))

(defn- jnr-available? []
  (try
    (import 'jnr.ffi.Pointer)
    true
    (catch ClassNotFoundException e
      false)))

(declare id-hash)
(locking #'id-hash
  (defonce id-hash
    (if (and (= "SHA1" id-hash-algorithm)
             byte-utils-sha1-enabled?)
      (do (log/info "Using ByteUtils/sha1 for ID hashing.")
          (fn byte-utils-id-hash-buffer [to from]
            (ByteUtils/sha1 to from)))
      (if-let [openssl-id-hash-buffer (and openssl-enabled?
                                           (jnr-available?)
                                           (some-> 'crux.hash.jnr/openssl-id-hash-buffer requiring-resolve var-get))]
        (do (log/info "Using libcrypto (OpenSSL) for ID hashing.")
            openssl-id-hash-buffer)
        (if-let [gcrypt-id-hash-buffer (and gcrypt-enabled?
                                            (jnr-available?)
                                            (some-> 'crux.hash.jnr/gcrypt-id-hash-buffer requiring-resolve var-get))]
          (do (log/info "Using libgcrypt for ID hashing.")
              gcrypt-id-hash-buffer)
          (do (log/info "Using java.security.MessageDigest for ID hashing.")
              message-digest-id-hash-buffer))))))
