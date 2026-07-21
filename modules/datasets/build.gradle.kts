plugins {
    `java-library`
    alias(libs.plugins.clojurephant)

    `maven-publish`
    signing
    alias(libs.plugins.kotlin.jvm)
    // GleifPgIndexer.Factory is @Serializable so it can travel as persisted
    // secondary-database config (its Registration round-trips via protobuf Struct).
    alias(libs.plugins.kotlin.serialization)
    // publishes a stub javadoc jar to satisfy Maven Central (no public Java API here)
    alias(libs.plugins.dokka)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":"))
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api(libs.tpch)
    api(libs.clojure.data.csv)   // EDGAR mirror parses the quarterly TSVs
    api(libs.jsonista)
    api(libs.next.jdbc)

    api(libs.aws.s3)

    // the GLEIF / EDGAR mirrors fetch over HTTP (hato) and write idiomatic
    // records as transit (transit.clj for GLEIF's direct cognitect.transit use).
    api(libs.hato)
    api(libs.transit.clj)

    // the GLEIF / EDGAR PgIndexers (Kotlin) write Postgres-source rows into XT;
    // brings PgIndexer / RowOp / the Postgres ExternalSource.
    implementation(project(":modules:xtdb-postgres-source"))
    implementation(kotlin("stdlib"))
    // the Postgres sinks bind enum/jsonb (GLEIF) and dates/numerics (EDGAR) via pgjdbc.
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
