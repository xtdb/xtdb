import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
    kotlin("jvm")
    kotlin("plugin.serialization")
    id("org.jetbrains.dokka")
    antlr
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB Core")
            description.set("XTDB Core")
        }
    }
}

dependencies {
    api(project(":xtdb-api"))
    compileOnlyApi(files("src/main/resources"))

    implementation(libs.clojure)
    api(libs.clojure.tools.logging)
    api(libs.clojure.spec)
    api(libs.clojure.`data`.json)
    api(libs.clojure.`data`.csv)
    api(libs.clojure.tools.cli)
    api(libs.transit.clj)

    api(libs.arrow.algorithm)
    api(libs.arrow.compression)
    api(libs.arrow.vector)
    api(libs.arrow.memory.netty)
    api(libs.netty.common)

    api("org.roaringbitmap", "RoaringBitmap", "1.0.1")

    api("pro.juxt.clojars-mirrors.integrant", "integrant", "0.8.0")
    api("clj-commons", "clj-yaml", "1.0.27")

    api("org.babashka", "sci", "0.6.37")
    api("commons-codec", "commons-codec", "1.15")
    api("com.carrotsearch", "hppc", "0.9.1")

    api("com.github.ben-manes.caffeine", "caffeine", "3.1.8")
    api("buddy", "buddy-hashers", "2.0.167")

    // healthz server
    api(libs.ring.core)
    api(libs.ring.jetty9.adapter)
    api(libs.jetty.alpn.server)

    api(libs.muuntaja)
    api(libs.reitit.core)
    api(libs.reitit.interceptors)
    api(libs.reitit.ring)
    api(libs.reitit.http)
    api(libs.reitit.sieppari)

    // monitoring
    api(libs.micrometer.core)
    api(libs.micrometer.registry.prometheus)

    api(kotlin("stdlib-jdk8"))
    api("com.charleskorn.kaml", "kaml", "0.56.0")

    api("org.postgresql", "postgresql", "42.7.3")
    api("com.github.seancorfield", "next.jdbc", "1.3.955")
    api("org.xerial", "sqlite-jdbc", "3.47.0.0")


    antlr("org.antlr:antlr4:4.13.1")
    implementation("org.antlr:antlr4-runtime:4.13.1")

    testImplementation("com.github.seancorfield", "next.jdbc", "1.3.939")
    testImplementation("io.mockk", "mockk", "1.13.9")
    testImplementation("org.clojure", "test.check", "1.1.1")
    testImplementation(project(":xtdb-http-client-jvm"))
    testImplementation(project(":xtdb-http-server"))
    testImplementation(project(":modules:xtdb-kafka"))
    testImplementation(project(":modules:xtdb-aws"))
    testImplementation(project(":modules:xtdb-google-cloud"))
    testImplementation(project(":modules:xtdb-azure"))
    testImplementation(project(":modules:xtdb-flight-sql"))
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

tasks.javadoc.get().enabled = false

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_21)

        java {
            freeCompilerArgs.add("-Xjvm-default=all")
        }
    }
}

tasks.compileKotlin {
    dependsOn("generateGrammarSource")
}

tasks.compileTestKotlin {
    dependsOn("generateTestGrammarSource")
}

tasks.generateGrammarSource {
    arguments = listOf(
        "-visitor", "-no-listener",
        "-package", "xtdb.antlr",
        "-Xexact-output-dir",
    )
    outputDirectory = file("${layout.buildDirectory.get().asFile}/generated-src/antlr/main/xtdb/antlr")
}

tasks.dokkaHtmlPartial {
    dokkaSourceSets["main"].run {
        perPackageOption {
            matchingRegex.set(".*")
            suppress.set(true)
        }

        perPackageOption {
            matchingRegex.set("xtdb\\.api.*")
            suppress.set(false)
        }
    }
}
