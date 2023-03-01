plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("Core2 Clojure HTTP Client")
            description.set("Core2 Clojure HTTP Server")
        }
    }
}

dependencies {
    api(project(":api"))
    api(project(":wire-formats"))

    api("pro.juxt.clojars-mirrors.hato", "hato", "0.8.2")
    api("pro.juxt.clojars-mirrors.metosin", "reitit-core", "0.5.15")
}

