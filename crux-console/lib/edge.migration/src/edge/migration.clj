(ns edge.migration
  (:require
    [rewrite-clj.parser :refer [parse-file]]
    [io.dominic.ednup.core :as ednup]
    [clojure.java.io :as io]
    [clojure.string :as string]
    jansi-clj.auto
    [jansi-clj.core :as j]
    [clojure.edn :as edn])
  (:import
    [java.nio.file Files Paths FileAlreadyExistsException]
    [java.nio.file.attribute FileAttribute]))

(defn- read-deps
  [deps-f]
  (parse-file deps-f))

(defn- lib-paths
  [deps]
  (concat
    (map (fn [lib] [:deps lib]) (keys (:deps deps)))
    (mapcat (fn [alias]
              (map
                (fn [lib] [:aliases alias :extra-deps lib])
                (keys (get-in deps [:aliases alias :extra-deps]))))
            (keys (:aliases deps)))))

(defn- update-libs
  [deps p? f]
  (reduce
    (fn [deps lib-path]
      (if (p? (last lib-path) (get-in deps lib-path))
        (update-in deps lib-path #(f (last lib-path) %))
        deps))
    deps
    (lib-paths deps)))

;; Migrations

;;  Migrations must be very careful!  They must continue to work on any
;;  infinitely old deps.edn, and they must be safe to run multiple times on the
;;  same file over & over.

(defn tdeps-74-mapping
  [lib-dir]
  (let [coord (fn [%] {:local/root (str (io/file lib-dir %))})]
    {['juxt.edge/lib.app "../lib.app"] (coord "edge.app")
     ['juxt.edge/lib.app.dev "../lib.app.dev"] (coord "edge.app.dev")
     ['juxt.edge/app.logging "../lib.app.logging"] (coord "edge.app.logging")
     ['juxt.edge/lib.app.prod "../lib.app.prod"] (coord "edge.app.prod")
     ['edge/asciidoctor "../edge.asciidoctor"] (coord "edge.asciidoctor")
     ['juxt.edge/dev.nrepl "../dev.nrepl"] (coord "edge.dev.nrepl")
     ['juxt.edge/bidi.ig "../lib.ig.bidi"] (coord "edge.ig.bidi")
     ['juxt.edge/yada.ig "../lib.ig.yada"] (coord "edge.ig.yada")
     ['juxt.edge/kick "../edge.kick"] (coord "edge.kick")
     ['juxt.edge/logging.dev "../lib.logging.dev"] (coord "edge.logging.dev")
     ['juxt.edge/rebel.auto-dev "../lib.rebel.auto-dev"] (coord "edge.rebel.auto-dev")
     ['juxt.edge/lib.socket-server "../lib.socket-server"] (coord "edge.socket-server")
     ['juxt.edge/edge.system "../edge.system"] (coord "edge.system")
     ['juxt.edge/test-utils "../edge.test-utils"] (coord "edge.test-utils")
     ['juxt.edge/graphql-ws "../graphql-ws"] (coord "graphql-ws")}))

(defn ->post-tdeps-74
  "Migration of edge modules to a sub-directory in edge, following the fix of TDEPS-74."
  [deps opts]
  (let [tdeps-74-mapping (tdeps-74-mapping (:lib-dir opts))]
    (update-libs deps
                 (fn [lib coord]
                   (contains? tdeps-74-mapping [lib (:local/root coord)]))
                 (fn [lib coord]
                   (merge coord (get tdeps-74-mapping [lib (:local/root coord)]))))))

(defn migrate-file
  [nodemap opts]
  (str (ednup/nm-string
         (->post-tdeps-74
           (ednup/->NodeMap nodemap)
           opts)) \newline))

(defn- app-dirs
  [edge-root]
  (->> edge-root
       (.listFiles)
       (filter #(.isDirectory %))
       (filter #(.exists (io/file % "deps.edn")))))

(defn- run-deps-migrations
  [edge-root opts]
  ;; for now, we only have 1 migration, and it's only job is to update
  ;; non-edge apps, we can figure the rest out later.
  (let [app-dirs (app-dirs edge-root)]
    (run! (fn [app-dir]
            (let [input (io/file app-dir "deps.edn")
                  migrated (migrate-file (read-deps input) opts)]
              (when (not= (slurp input) migrated)
                (spit input migrated)
                (println)
                (println (j/underline (str (.relativize
                                             (.toPath edge-root)
                                             (.toPath input)))))
                (println (j/black-bright "Automatic migration applied, please commit before continuing.")))))
          app-dirs)))

(defn- run-other-migrations
  [edge-root opts]
  (doseq [app-dir (app-dirs edge-root)]
    (let [dest (.resolve (.toPath app-dir) ".vscode/settings.json")]
      (when (let [deps (edn/read (java.io.PushbackReader.
                                   (io/reader (io/file app-dir "deps.edn"))))]
              ;; Detect ClojureScript aliases
              (and (every? #(contains? (:aliases deps) %) [:build :dev/build])
                   (or (contains? (:deps deps) 'org.clojure/clojurescript)
                       (some
                         #(contains? (:extra-deps (val %))
                                     'org.clojure/clojurescript)
                         (:aliases deps)))))
        (io/make-parents (.toFile dest))
        (try
          (Files/createSymbolicLink
            dest
            (Paths/get "../../lib/edge-app-template/links/cljs_calva_settings.json" (into-array String []))
            (into-array FileAttribute []))

          (println)
          (println (j/underline (str (.relativize
                                       (.toPath edge-root)
                                       dest))))
          (println (j/black-bright "Created Calva (VSCode) ClojureScript jack-in settings, please commit with git."))
          (catch FileAlreadyExistsException _))))))

(defn run-migrations
  [edge-root opts]
  (run-deps-migrations edge-root opts)
  (run-other-migrations edge-root opts))

(def cli-options
  ;; An option with a required argument
  [[nil "--lib-dir PORT" "Port number" :default "../lib"]])

(defn- main-errors
  [errors]
  (println
    (j/red
      (str
        "An error occurred:\n"
        (string/join \newline errors))))
  (System/exit 1))

(defn -main [& args]
  (let [{:keys [options arguments errors summary]}
        ((requiring-resolve 'clojure.tools.cli/parse-opts)
         args cli-options)]
    (cond
      errors
      (main-errors errors)
      
      (= 1 (count arguments))
      (run-migrations (io/file (first arguments)) options)

      :else
      (main-errors ["Wrong number of arguments provided"]))))
