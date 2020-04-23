(defproject juxt/crux-http-server "derived-from-git"
  :description "Crux HTTP Server"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 #_[ch.qos.logback/logback-classic "1.2.3"]
                 [hiccup "2.0.0-alpha2"]
                 [juxt/crux-core "derived-from-git"]
                 [ring/ring-core "1.8.0"]
                 [ring/ring-jetty-adapter "1.8.0"]
                 [ring/ring-codec "1.1.2"]
                 [ring-cors "0.1.13"]
                 [metosin/muuntaja "0.6.6"]]
  :clean-targets ^{:protect false} ["target"]
  :profiles {:dev
             {:dependencies [[org.clojure/clojurescript "1.10.339"]
                             [com.bhauman/figwheel-main "0.2.4"]
                             [com.bhauman/rebel-readline-cljs "0.1.4"]]}}
  :aliases {"fig" ["trampoline" "run" "-m" "figwheel.main"]}
  :resource-paths ["target" "resources"]
  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn)
