(defproject pro.juxt.crux-labs/core2-s3 "<inherited>"
  :description "Core2 S3 integration"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition
                             :javac-options]}

  :scm {:dir "../.."}

  :dependencies [[pro.juxt.crux-labs/core2]
                 [software.amazon.awssdk/s3]])
