plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB JDBC")
            description.set("XTDB JDBC")
        }
    }
}

dependencies {
    api(project(":api"))
    api(project(":core"))

    api("org.clojure", "java.data", "1.0.95")
    api("pro.juxt.clojars-mirrors.com.github.seancorfield", "next.jdbc", "1.2.674")
    api("com.zaxxer", "HikariCP", "4.0.3")
}
