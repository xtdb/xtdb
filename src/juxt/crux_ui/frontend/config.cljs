(ns juxt.crux-ui.frontend.config
  (:require ["./ua-regexp.js" :as ua-rgx]))

(def ^:const ua-regex ua-rgx)

(def ^:const user-agent js/navigator.userAgent)

(def ^:const ua-info
  (re-find ua-regex user-agent))

(def ^:const browser-vendor-string (second ua-info))
(def ^:const browser-version-string (nth ua-info 2))

(def ^:const browser-vendor
  (case browser-vendor-string
    "Chrome"  :browser/chrome
    "Firefox" :browser/firefox
    "Opera"   :browser/opera
    "Safari"  :browser/safari
    "Edge"    :browser/edge
    :browser/unknown))

(def ^:const supports-input-datetime?
  (not (#{:browser/firefox :browser/ie :browser/safari :browser/unknown} browser-vendor)))
