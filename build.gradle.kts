allprojects {
    group = "juxt"
    version = "20.09-1.12.0-SNAPSHOT"

    repositories {
        mavenCentral()
        jcenter()
        maven { url = uri("https://repo.clojars.org") }
        maven { url = uri("https://ci-artifactory.corda.r3cev.com/artifactory/corda") }
        maven { url = uri("https://repo.gradle.org/gradle/libs-releases") }
    }

    tasks.withType(org.jetbrains.kotlin.gradle.tasks.KotlinCompile::class.java).all {
        kotlinOptions {
            jvmTarget = "1.8"
        }
    }
}

plugins {
    kotlin("jvm") version "1.4.10"
    id("dev.clojurephant.clojure") version "0.6.0-alpha.4"
    id("net.corda.plugins.cordapp") version "5.0.10"
    id("net.corda.plugins.quasar-utils") version "5.0.10"
}
