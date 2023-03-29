plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB PGWire Server")
            description.set("XTDB PGWire Server")
        }
    }
}

dependencies {
    api(project(":api"))
    api(project(":core"))

    api("org.clojure", "data.json", "2.4.0")
    api("org.clojure", "tools.logging", "1.2.4")
}
