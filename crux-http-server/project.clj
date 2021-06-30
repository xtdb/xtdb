(defproject pro.juxt.crux/crux-http-server "crux-git-version"
  :description "Crux HTTP Server"

  :plugins [[lein-parent "0.3.8"]
            [yogthos/lein-sass "0.1.10"]]

  :parent-project {:path "../project.clj"
                   :inherit [:repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/data.csv "1.0.0"]
                 [pro.juxt.crux/crux-core "crux-git-version"]
                 [com.cognitect/transit-clj "1.0.324"]

                 [com.nimbusds/nimbus-jose-jwt "9.7"]

                 [pro.juxt.clojars-mirrors.hiccup/hiccup "2.0.0-alpha2"]
                 [pro.juxt.clojars-mirrors.crux/crux-http-server-deps "0.0.2"]
                 [pro.juxt.clojars-mirrors.camel-snake-kebab/camel-snake-kebab "0.4.2"]]

  :clean-targets ^{:protect false} ["target"]
  :profiles {:dev
             {:jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"]
              :dependencies [[metosin/reitit-dev "0.5.12"]

                             [org.clojure/clojurescript "1.10.844"]
                             [ch.qos.logback/logback-classic "1.2.3"]
                             [cljsjs/codemirror "5.44.0-1"]
                             [com.bhauman/figwheel-main "0.2.12"
                              :exclusions [com.google.errorprone/error_prone_annotations]]
                             [com.bhauman/rebel-readline-cljs "0.1.4"]
                             [reagent "0.10.0"]
                             [re-frame "0.12.0"]
                             [metosin/reitit-frontend "0.5.12"]
                             [metosin/reitit-spec "0.5.12"]
                             [fork "1.2.5"]
                             [day8.re-frame/http-fx "v0.2.0"]
                             [tick "0.4.23-alpha"]
                             [cljsjs/react-datetime "2.16.2-0"]

                             ;; dependency resolution
                             [com.bhauman/spell-spec "0.1.2"]
                             [ring/ring-devel "1.8.1"]
                             [expound "0.8.7"]]}

             :test {:dependencies [[pro.juxt.clojars-mirrors.clj-http/clj-http "3.12.2"]
                                   [pro.juxt.crux/crux-test "crux-git-version"]]}}

  :aliases {"fig" ["trampoline" "run" "-m" "figwheel.main"]
            "build:cljs" ["do"
                          ["clean"]
                          ["run" "-m" "figwheel.main" "-O" "advanced" "-bo" "dev"]
                          ["sass"]]
            "install" ["do" "build:cljs" "install"]}

  :resource-paths ["resources" "cljs-target" "src-cljs"]
  :jar-exclusions [#"public/cljs-out/dev/.*"]
  :sass {:source "resources/public/scss/" :target "cljs-target/public/css/"}

  :middleware [leiningen.project-version/middleware])
