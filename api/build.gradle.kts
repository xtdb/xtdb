plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB API")
            description.set("XTDB API")
        }
    }
}

dependencies {
    compileOnlyApi(files("src/main/resources"))
    implementation("org.clojure", "clojure", "1.11.1")
}
