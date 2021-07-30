plugins {
    kotlin("jvm")
    `maven-publish`
    signing
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set("Crux Corda State")
                description.set("A Crux module that allows you to pipe verified Corda transactions into a Crux node, to then query using Crux’s bitemporal Datalog query engine. ")
                url.set("https://github.com/juxt/crux")
                licenses {
                    license {
                        name.set("The MIT License")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }
                developers {
                    developer {
                        id.set("juxt")
                        name.set("JUXT")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/juxt/crux.git")
                    developerConnection.set("scm:git:ssh://github.com:juxt/crux.git")
                    url.set("https://github.com/juxt/crux")
                }
            }
        }
    }

    repositories {
        maven {
            name = "ossrh"
            val releasesRepoUrl = "https://oss.sonatype.org/service/local/staging/deploy/maven2"
            val snapshotsRepoUrl = "https://oss.sonatype.org/content/repositories/snapshots"
            url = uri(if (!version.toString().endsWith("-SNAPSHOT")) releasesRepoUrl else snapshotsRepoUrl)

            credentials {
                username = project.properties["ossrhUsername"] as String
                password = project.properties["ossrhPassword"] as String
            }
        }
    }
}

signing {
    useGpgCmd()
    sign(publishing.publications["maven"])
}

java {
    withJavadocJar()
    withSourcesJar()
}

tasks.javadoc {
}
