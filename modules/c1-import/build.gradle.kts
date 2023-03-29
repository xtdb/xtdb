plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB C1 Importer")
            description.set("XTDB C1 Importer")
        }
    }
}

dependencies {
    api(project(":api"))
    api(project(":core"))

    api("com.cognitect", "transit-clj", "1.0.324")
}
