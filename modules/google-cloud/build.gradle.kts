plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB Google Cloud")
            description.set("XTDB Google Cloud")
        }
    }
}

dependencies {
    api(project(":api"))
    api(project(":core"))

    api("com.google.cloud", "google-cloud-storage", "2.23.0")
}
