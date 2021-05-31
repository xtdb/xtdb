import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.4.21"
    `java-library`
    `maven-publish`
    signing
}

group = "pro.juxt"
version = "0.0.1-SNAPSHOT"

java {
    withJavadocJar()
    withSourcesJar()
}

repositories {
    mavenCentral()
    maven {
        name = "Clojars"
        url = uri("https://repo.clojars.org/")
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "crux-kotlin-dsl"
            from(components["java"])
            pom {
                name.set("Crux Kotlin DSL")
                description.set("A Kotlin DSL for Crux which enables pretty transactions and queries")
                url.set("https://github.com/crux-labs/crux-kotlin-dsl")
                licenses {
                    license {
                        name.set("The MIT License")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }
                developers {
                    developer {
                        id.set("AlistairONeill")
                        name.set("Alistair O'Neill")
                        email.set("aon@juxt.pro")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/crux-labs/crux-kotlin-dsl.git")
                    developerConnection.set("scm:git:ssh://github.com:crux-labs/crux-kotlin-dsl.git")
                    url.set("https://github.com/crux-labs/crux-kotlin-dsl")
                }
            }
        }
    }

    repositories {
        maven {
            name = "ossrh"
            credentials(PasswordCredentials::class)
            val releasesRepoUrl = "https://oss.sonatype.org/service/local/staging/deploy/maven2"
            val snapshotsRepoUrl = "https://oss.sonatype.org/content/repositories/snapshots"
            url = uri(if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl)
        }
    }
}

signing {
    sign(publishing.publications["mavenJava"])
}

tasks.javadoc {
    if (JavaVersion.current().isJava9Compatible) {
        (options as StandardJavadocDocletOptions).addBooleanOption("html5", true)
    }
}

dependencies {
    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    // TODO: I'm not sure if we need both `implementation` and `api`? -sd
    implementation("juxt:crux-core:21.04-1.16.0-beta")
    api("juxt:crux-core:21.04-1.16.0-beta")

    testImplementation(kotlin("test-junit5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.6.0")
    testImplementation("com.natpryce:hamkrest:1.8.0.1")
    testImplementation("juxt:crux-rocksdb:21.04-1.16.0-beta")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.6.0")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}
