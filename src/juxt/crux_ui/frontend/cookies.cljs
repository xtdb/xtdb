(ns juxt.crux-ui.frontend.cookies
  (:refer-clojure :exclude [count get keys vals empty? reset!])
  (:require [goog.net.cookies :as cks]
            [cljs.reader :as reader]))

(defn set!
  "sets a cookie, the max-age for session cookie
   following optional parameters may be passed in as a map:
   :max-age - defaults to -1
   :path - path of the cookie, defaults to the full request path
   :domain - domain of the cookie, when null the browser will use the full request host name
   :secure? - boolean specifying whether the cookie should only be sent over a secure channel
   :raw? - boolean specifying whether content should be stored raw, or as EDN
  "
  [k content & [{:keys [max-age path domain secure? raw?]} :as opts]]
  (let [k (name k)
        content (if raw?
                  (str content)
                  (pr-str content))]
    (if-not opts
      (.set goog.net.cookies k content)
      (.set goog.net.cookies k content (or max-age -1) path domain (boolean secure?)))))

(defn- get-value [k]
  (.get goog.net.cookies k))

(defn get
  "gets the value at the key (as edn), optional default when value is not found"
  [k & [default]]
  (or (some-> k name get-value reader/read-string) default))

(defn contains-key?
  "is the key present in the cookies"
  [k]
  (.containsKey goog.net.cookies (name k)))

(defn empty?
  "true if no cookies are set"
  []
  (.isEmpty goog.net.cookies))

(defn remove!
  "removes a cookie, optionally for a specific path and/or domain"
  ([k]
   (.remove goog.net.cookies (name k)))
  ([k path domain]
   (.remove goog.net.cookies (name k) path domain)))
