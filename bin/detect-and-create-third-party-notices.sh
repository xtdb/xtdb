#!/usr/bin/env bash

CORE2_PATH=$(realpath $(dirname $0)/..)

# used by Microsoft's `sbom-tool`:
# rm -r $CORE2_PATH/_manifest/

(
  cd $CORE2_PATH

  clojure -X:deps mvn-pom

  # `sbom-tool` will probably have a longer lifespan than neo4j's licensing plugin,
  # but it's also a bit hairier to work with. parking for now. -sd
  # ./sbom-tool-osx-x64 generate -b . -bc . -pn "core2" -pv "0.1.0" -nsb "https://zig"

  mvn org.neo4j.build.plugins:licensing-maven-plugin:check -DfailIfMissing=false && \
  mvn org.neo4j.build.plugins:licensing-maven-plugin:collect-reports && \
  mvn org.neo4j.build.plugins:licensing-maven-plugin:aggregate

  xmllint --format ./target/aggregated-third-party-licensing.xml > ./target/aggregated-third-party-licensing-formatted.xml

  # TODO: in the future, we may want to consider scraping Copyright notices as well, but
  #       for now it doesn't seem like `licensing-maven-plugin` can do that. As such, the
  #       notice/description below only mentions *licenses* and not *copyrights* as per
  #       the original (manual) `THIRD_PARTY_NOTICES` text. -sd
  sed '/^<licensing/a\
  <notice> \
    THIRD-PARTY SOFTWARE NOTICES AND INFORMATION \
    Do Not Translate or Localize \
  </notice> \
  <description> \
    This project incorporates components from the projects listed below. The original licenses under which JUXT Ltd. received such components are set forth below. JUXT Ltd. reserves all rights not expressly granted herein, whether by implication, estoppel, or otherwise. \
  </description> \
' ./target/aggregated-third-party-licensing-formatted.xml > ./THIRD_PARTY_NOTICES.xml
)
