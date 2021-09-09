import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.10"
    `java-library`
    `maven-publish`
    signing
}

group = "com.xtdb.labs"
version = "0.1.0-SNAPSHOT"

java {
    withJavadocJar()
    withSourcesJar()
}

repositories {
    mavenCentral()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "xtdb-kotlin-dsl"
            from(components["java"])
            pom {
                name.set("XTDB Kotlin DSL")
                description.set("A Kotlin DSL for XTDB which enables pretty transactions and queries")
                url.set("https://github.com/xtdb/xtdb")
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
                    connection.set("scm:git:git://github.com/xtdb/xtdb.git")
                    developerConnection.set("scm:git:ssh://github.com:xtdb/xtdb.git")
                    url.set("https://github.com/xtdb/xtdb")
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

    api("com.xtdb:xtdb-core:1.19.0-beta1") {
        isTransitive = true
    }

    testImplementation(kotlin("test-junit5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.6.0")
    testImplementation("com.natpryce:hamkrest:1.8.0.1")
    testImplementation("com.xtdb:xtdb-rocksdb:1.19.0-beta1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.6.0")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}
