import xtdb.DataReaderTransformer

plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
    id("com.github.johnrengelman.shadow")
}

// TODO: Not sure of how to publish this
// publishing {
//     publications.create("maven", MavenPublication::class) {
//         pom {
//             name.set("XTDB Kafka Connect")
//             description.set("XTDB Kafka Connect")
//         }
//     }
// }

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))
    api(project(":xtdb-http-client-jvm"))

    api("org.apache.kafka", "connect-api", "3.8.0")

    api("org.clojure", "tools.logging", "1.2.4")
    api("cheshire", "cheshire", "5.13.0")
    api("org.slf4j", "slf4j-api", "1.7.36")
}

tasks.shadowJar {
    transform(DataReaderTransformer())
    mergeServiceFiles()
    archiveFileName.set("xtdb-kafka-connect.jar")
}
