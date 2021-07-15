(defproject pro.juxt.crux-labs/core2-jdbc "<inherited>"
  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[pro.juxt.crux-labs/core2]
                 [seancorfield/next.jdbc "1.2.659"]
                 [com.zaxxer/HikariCP "4.0.3"]
                 [org.postgresql/postgresql "42.2.20" :scope "provided"]])
