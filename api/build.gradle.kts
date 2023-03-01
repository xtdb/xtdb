plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("Core2 API")
            description.set("Core2 API")
        }
    }
}

dependencies {
    compileOnlyApi(files("src/main/resources"))
    implementation("org.clojure", "clojure", "1.11.1")
}
