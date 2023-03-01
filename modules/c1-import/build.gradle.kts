plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("Core2 C1 Importer")
            description.set("Core2 C1 Importer")
        }
    }
}

dependencies {
    api(project(":api"))
    api(project(":core"))

    api("com.cognitect", "transit-clj", "1.0.324")
}
