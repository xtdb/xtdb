(defproject crux-console "derived-from-git"
  :dependencies
    [[org.clojure/clojure         "1.10.0"]
     [org.clojure/clojurescript   "1.10.520"]
     [cljs.bean                   "1.3.0"]
     [reagent                     "0.8.1"]
     [garden                      "1.3.9"]
     [re-frame                    "0.10.8"]
     [day8.re-frame/re-frame-10x  "0.3.3"]
     [re-com                      "2.2.0"]
     [secretary                   "1.2.3"]
     [com.andrewmcveigh/cljs-time "0.5.2"]
     [binaryage/oops              "0.6.4"]
     [hiccup                      "1.0.5"]
     [day8.re-frame/test          "0.1.5"]
     [thheller/shadow-cljs        "2.6.24"]]

  :min-lein-version "2.9.1"
  :repositories [["clojars" "https://repo.clojars.org"]]

  :source-paths ["src" "test"]

  :resource-paths ["resources"])
