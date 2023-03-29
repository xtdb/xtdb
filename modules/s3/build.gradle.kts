plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB S3")
            description.set("XTDB S3")
        }
    }
}

dependencies {
    api(project(":api"))
    api(project(":core"))

    api("software.amazon.awssdk", "s3", "2.16.76")
}
