plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

ext {
    set("labs", true)
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB C1 Importer")
            description.set("XTDB C1 Importer")
        }
    }
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api("com.cognitect", "transit-clj", "1.0.324")
}
