(ns juxt.crux-ui.frontend.config
  (:require ["./ua-regexp.js" :as ua-rgx]))

(def url-docs "https://juxt.pro/crux/docs/index.html")
(def url-chat "https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux")
(def url-mail "crux@juxt.pro")
(def url-examples-gist "https://gist.githubusercontent.com/spacegangster/b68f72e3c81524a71af1f3033ea7507e/raw/572396dec0791500c965fea443b2f26a60f500d4/examples.edn")

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
