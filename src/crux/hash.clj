(ns crux.hash
  (:require [clojure.tools.logging :as log]
            [crux.memory :as mem])
  (:import [org.agrona DirectBuffer]
           org.agrona.concurrent.UnsafeBuffer
           java.security.MessageDigest))

(def ^:const ^:private gcrypt-enabled? (not (boolean (System/getenv "CRUX_DISABLE_LIBGCRYPT"))))

;; NOTE: Using name without dash as it's supported both by
;; MessageDigest and libgcrypt.
(def ^:const id-hash-algorithm "SHA1")
(def ^:private ^MessageDigest id-digest-prototype (MessageDigest/getInstance id-hash-algorithm))
(def ^:const id-hash-size (.getDigestLength id-digest-prototype))

(declare id-hash)

;; NOTE: Allowing on-heap buffer here for now.
(defn message-digest-id-hash-buffer ^org.agrona.DirectBuffer [^DirectBuffer buffer]
  (let [^MessageDigest md (try
                            (.clone id-digest-prototype)
                            (catch CloneNotSupportedException e
                              (MessageDigest/getInstance id-hash-algorithm)))]
    (UnsafeBuffer. (.digest md (mem/->on-heap buffer)))))

(defn message-digest-id-hash ^bytes [^bytes bytes]
  (mem/->on-heap (message-digest-id-hash-buffer (mem/as-buffer bytes))))

(defn- jnr-available? []
  (try
    (import 'jnr.ffi.Pointer)
    true
    (catch ClassNotFoundException e
      false)))

(when-not (bound? #'id-hash)
  (try
    (if-let [gcrypt-id-hash (and gcrypt-enabled?
                                 (jnr-available?)
                                 (requiring-resolve 'crux.hash.jnr/gcrypt-id-hash))]
      (do (log/info "Using libgcrypt for ID hashing.")
          (def id-hash gcrypt-id-hash))
      (do (log/info "Using java.security.MessageDigest for ID hashing.")
          (def id-hash message-digest-id-hash)))
    (catch Throwable t
      (log/warn t "Could not load libgcrypt, falling back to java.security.MessageDigest for ID hashing.")
      (def id-hash message-digest-id-hash))))
