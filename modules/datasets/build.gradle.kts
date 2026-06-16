plugins {
    `java-library`
    alias(libs.plugins.clojurephant)

    `maven-publish`
    signing
    alias(libs.plugins.kotlin.jvm)
    // GleifPgIndexer.Factory is @Serializable so it can travel as persisted
    // secondary-database config (its Registration round-trips via protobuf Struct).
    alias(libs.plugins.kotlin.serialization)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":"))
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api(libs.tpch)
    api(libs.jsonista)
    api(libs.next.jdbc)

    // the GLEIF mirror fetches the API (hato) and writes idiomatic records as transit
    api(libs.hato)
    api(libs.transit.clj)

    // the GLEIF PgIndexer (Kotlin) writes Postgres-source rows into XT; brings
    // PgIndexer / RowOp / the Postgres ExternalSource.
    implementation(project(":modules:xtdb-postgres-source"))
    implementation(kotlin("stdlib"))
    // the Postgres sink references PGobject directly (enum/jsonb binding).
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
