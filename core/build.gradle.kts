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

    api("org.clojure", "tools.logging", "1.2.4")
    api("org.clojure", "spec.alpha", "0.3.218")
    api("org.clojure", "data.json", "2.4.0")
    api("org.clojure", "data.csv", "1.0.1")
    api("org.clojure", "tools.cli", "1.0.206")
    api("com.cognitect", "transit-clj", "1.0.329")

    api("org.apache.arrow", "arrow-algorithm", "16.1.0")
    api("org.apache.arrow", "arrow-compression", "16.1.0")
    api("org.apache.arrow", "arrow-vector", "16.1.0")
    api("org.apache.arrow", "arrow-memory-netty", "16.1.0")
    api("io.netty", "netty-common", "4.1.82.Final")

    api("org.roaringbitmap", "RoaringBitmap", "1.0.1")

    api("pro.juxt.clojars-mirrors.integrant", "integrant", "0.8.0")
    api("clj-commons", "clj-yaml", "1.0.27")

    api("org.babashka", "sci", "0.6.37")
    api("commons-codec", "commons-codec", "1.15")
    api("com.carrotsearch", "hppc", "0.9.1")

    api("com.github.ben-manes.caffeine", "caffeine", "3.1.8")

    // monitoring
    api("io.micrometer", "micrometer-core", "1.12.2")
    api("io.micrometer", "micrometer-registry-prometheus", "1.12.2")

    api(kotlin("stdlib-jdk8"))
    api("com.charleskorn.kaml","kaml","0.56.0")

    antlr("org.antlr:antlr4:4.13.1")
    implementation("org.antlr:antlr4-runtime:4.13.1")

    testImplementation("io.mockk","mockk", "1.13.9")
    testImplementation("org.clojure","test.check", "1.1.1")
    testImplementation(project(":xtdb-http-server"))
    testImplementation(project(":xtdb-pgwire-server"))
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

tasks.withType(AntlrTask::class.java) {
    arguments = listOf("-package", "xtdb.antlr", "-visitor", "-no-listener")
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
