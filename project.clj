(defproject crux-console "derived-from-git"
  :dependencies
    [[org.clojure/clojure          "1.10.0"]
     ; 1.10.1 doesn't work with this aleph
     [aleph                        "0.4.6"]
     [bidi                         "2.1.6"]
     [hiccup                       "1.0.5"]
     [juxt/crux-uberjar            "19.09-1.5.0-alpha"]
     [page-renderer                "0.4.2"]]

  :min-lein-version "2.9.1"
  :main crux-ui-server.main
  :aot  [crux-ui-server.main]
  :uberjar-name "crux-console.jar"
  :source-paths ["src" "test" "node_modules"]
  :resource-paths ["resources"]

  :profiles
  {:dev {:main dev :repl-options {:init-ns dev}}
   :shadow-cljs ; also see package.json deps
   {:dependencies
    [[org.clojure/clojurescript    "1.10.520"]
     [reagent                      "0.8.1"]
     [re-frame                     "0.10.8"]
     [garden                       "1.3.9"]
     [medley                       "1.2.0"]
     [funcool/promesa              "2.0.1"]
     [com.andrewmcveigh/cljs-time  "0.5.2"]
     [binaryage/oops               "0.6.4"]
     [day8.re-frame/test           "0.1.5"]
     [day8.re-frame/re-frame-10x   "0.3.3"]
     [thheller/shadow-cljs         "2.8.52"]
     [com.google.javascript/closure-compiler-unshaded "v20190819"]
     [org.clojure/google-closure-library "0.0-20190213-2033d5d9"]]}}

  ; AWS is using Java 8
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]

  :repositories [["clojars" "https://repo.clojars.org"]]
  :plugins [;[lein-shadow "0.1.5"] ; nasty guy, deletes original shadow-cljs config if you run it
            [lein-shell  "0.5.0"]] ; https://github.com/hypirion/lein-shell

  :clean-targets ^{:protect false} ["target" #_"resources/static/crux-ui/compiled"]

  :aliases
  {"yarn"
   ["do" ["shell" "yarn" "install"]]

   "shadow"
   ["shell" "node_modules/.bin/shadow-cljs"]

   "build"
   ["do"
    ["clean"]
    ["yarn"]
    ["shadow" "release" "app"]       ; compile
    #_["shadow" "release" "app-perf"]] ; compile production ready performance charts app

   "ebs"
   ["do" ["shell" "sh" "./dev/build-ebs.sh"]]

   "build-ebs"
   ["do"
    ["build"]
    ["uberjar"]
    ["ebs"]]

   "cljs-dev"
   ["do"
    ["yarn"]
    ["shadow" "watch" "app"]]

   "dep-tree"
   ["do"
    ["yarn"]
    ["shadow" "pom"]
    ["shell" "mvn" "dependency:tree"]]

   "build-report"
   [["yarn"]
    ["shadow" "run" "shadow.cljs.build-report" "app" "report.html"]]})

