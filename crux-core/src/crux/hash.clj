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

(declare id-hash)

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

(defn id-hash [to buffer]
  (if (and (= "SHA1" id-hash-algorithm)
           byte-utils-sha1-enabled?)
    (do (log/info "Using ByteUtils/sha1 for ID hashing.")
        (def id-hash (fn [to from]
                       (ByteUtils/sha1 to from))))
    (if-let [openssl-id-hash-buffer (and openssl-enabled?
                                         (jnr-available?)
                                         (some-> 'crux.hash.jnr/openssl-id-hash-buffer requiring-resolve var-get))]
      (do (log/info "Using libcrypto (OpenSSL) for ID hashing.")
          (def id-hash openssl-id-hash-buffer))
      (if-let [gcrypt-id-hash-buffer (and gcrypt-enabled?
                                          (jnr-available?)
                                          (some-> 'crux.hash.jnr/gcrypt-id-hash-buffer requiring-resolve var-get))]
        (do (log/info "Using libgcrypt for ID hashing.")
            (def id-hash gcrypt-id-hash-buffer))
        (do (log/info "Using java.security.MessageDigest for ID hashing.")
            (def id-hash message-digest-id-hash-buffer)))))
  (id-hash to buffer))
