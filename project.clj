(defproject crux-console "derived-from-git"
  :dependencies
    [[org.clojure/clojure         "1.10.0"]
     [org.clojure/clojurescript   "1.10.520"]
     [reagent                     "0.8.1"]
     [re-frame                    "0.10.8"]
     [garden                      "1.3.9"]
     [bidi                        "2.1.6"]
     [stylefy                      "1.13.3"]
     [medley                       "1.2.0"]
     [day8.re-frame/re-frame-10x  "0.3.3"]
     [funcool/promesa              "2.0.1"]
     [com.andrewmcveigh/cljs-time "0.5.2"]
     [binaryage/oops              "0.6.4"]
     [hiccup                      "1.0.5"]
     [day8.re-frame/test          "0.1.5"]
     [thheller/shadow-cljs        "2.8.52"]
     [com.google.javascript/closure-compiler-unshaded "v20190819"]
     [org.clojure/google-closure-library "0.0-20190213-2033d5d9"]]

  :min-lein-version "2.9.1"
  :repositories [["clojars" "https://repo.clojars.org"]]
  :plugins [[lein-shadow "0.1.5"]
            [lein-shell  "0.5.0"]]
  :aliases
  {"build"
   ["do"
    ["shell" "yarn" "install"]
    ["shadow" "release" "app"]       ; compile
    ["shadow" "release" "app-perf"]] ; compile production ready performance charts app

   "build-ebs"
   ["do"
    ["build"]
    ["shell" "sh" "./dev/build-ebs.sh"]]}

  :clean-targets ["target"]

  :shadow-cljs
  {:source-paths ["src" "../common/src" "test" "node_modules"]

   :dependencies
   [[reagent                      "0.8.1"]
    [re-frame                     "0.10.8"]
    [garden                       "1.3.9"]
    [bidi                         "2.1.6"]
    [stylefy                      "1.13.3"]
    [medley                       "1.2.0"]
    [day8.re-frame/re-frame-10x   "0.4.1"]
    [funcool/promesa              "2.0.1"]
    [com.andrewmcveigh/cljs-time  "0.5.2"]
    [binaryage/oops               "0.6.4"]]

   :builds
   {:app
    {:target :browser
     :modules {:main {:init-fn juxt.crux-ui.frontend.main/init}}
     :output-dir "resources/static/crux-ui/compiled"
     :compiler-options {:optimizations :advanced}
     :asset-path "/static/crux-ui/compiled"}

    :app-perf
    {:target :browser
     :modules {:main-perf {:init-fn juxt.crux-ui.frontend.main-perf/init}}
     :output-dir "resources/static/crux-ui/compiled"
     :compiler-options {:optimizations :advanced}
     :asset-path "/static/crux-ui/compiled"}


    :test
    {:target      :browser-test
     :test-dir    "resources/static/crux-ui/test"
     :ns-regexp   "-test$"
     :runner-ns   crux-console.test-runner
     :devtools
     {:http-port  4001
      :http-root  "resources/static/crux-ui/test"}}

    :dev
    {:target :browser
     :modules {:main {:init-fn juxt.crux-ui.frontend.main/init}}
     :output-dir "resources/static/crux-ui/compiled"
     :asset-path "/static/crux-ui/compiled"
     :compiler-options {:closure-warnings {:global-this :off}
                        :closure-defines  {re-frame.trace.trace-enabled? true}
                        :optimizations :none}
     :devtools
     {:after-load juxt.crux-ui.frontend.main/reload!
      :preloads   [day8.re-frame-10x.preload]}}

    :dev-perf
    {:target :browser
     :modules
     {:main-perf {:init-fn juxt.crux-ui.frontend.main-perf/init}}
     :output-dir "resources/static/crux-ui/compiled"
     :asset-path "/static/crux-ui/compiled"
     :compiler-options {:closure-warnings {:global-this :off}
                        :closure-defines  {re-frame.trace.trace-enabled? true}
                        :optimizations :none}
     :devtools
     {:after-load juxt.crux-ui.frontend.main-perf/reload!
      :preloads   [day8.re-frame-10x.preload]}}}

   :nrepl {:port 55300
           :init-ns juxt.crux-ui.frontend.main}}

  :source-paths ["src" "../common/src" "test" "node_modules"]

  :resource-paths ["resources"])
