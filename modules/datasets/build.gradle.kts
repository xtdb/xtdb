plugins {
    `java-library`
    alias(libs.plugins.clojurephant)

    `maven-publish`
    signing
    alias(libs.plugins.kotlin.jvm)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":"))
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api(libs.tpch)
    api(libs.clojure.data.csv)
    api(libs.jsonista)

    api(libs.aws.s3)
    api(libs.next.jdbc)

    // the EDGAR PgIndexer (Kotlin) writes Postgres-source rows into XT; brings
    // PgIndexer / RowOp / the Postgres ExternalSource.
    implementation(project(":modules:xtdb-postgres-source"))
    implementation(kotlin("stdlib"))
    // the Postgres sink binds dates/numerics via pgjdbc directly.
    implementation(libs.pgjdbc)
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB Datasets")
            description.set("XTDB Datasets")
        }
    }
}
