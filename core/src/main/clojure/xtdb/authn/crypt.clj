(ns xtdb.authn.crypt
  (:require [buddy.hashers :as hashers]))

(defn encrypt-pw [pw]
  (hashers/derive pw {:alg :argon2id}))
