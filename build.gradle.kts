import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

allprojects {
    group = "pro.juxt.crux"
    version = "0.0.1-SNAPSHOT"

    repositories {
        mavenCentral()
        jcenter()
        maven { url = uri("https://ci-artifactory.corda.r3cev.com/artifactory/corda") }
        maven { url = uri("https://repo.gradle.org/gradle/libs-releases") }
    }

    tasks.withType(KotlinCompile::class.java).all {
        kotlinOptions {
            jvmTarget = "1.8"
        }
    }
}

plugins {
    kotlin("jvm") version "1.4.10"
    id("dev.clojurephant.clojure") version "0.6.0"
    id("net.corda.plugins.cordapp") version "5.0.10"
    id("net.corda.plugins.quasar-utils") version "5.0.10"
}
