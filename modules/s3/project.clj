(defproject com.xtdb.labs/core2-s3 "<inherited>"
  :description "Core2 S3 integration"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition
                             :javac-options]}

  :scm {:dir "../.."}

  :java-source-paths ["src"]

  :dependencies [[com.xtdb.labs/core2-api]
                 [com.xtdb.labs/core2-core]
                 [software.amazon.awssdk/s3]

                 [cheshire nil :scope "test"]]

  :test-selectors {:default (complement :s3)
                   :s3 :s3})
