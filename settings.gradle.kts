pluginManagement {
    plugins {
        kotlin("jvm") version "2.1.0"
        kotlin("plugin.serialization") version "2.1.0"
        id("org.jetbrains.dokka") version "1.9.10"
    }
}

rootProject.name = "xtdb"

include("api", "core", "jdbc")
project(":api").name = "xtdb-api"
project(":core").name = "xtdb-core"
project(":jdbc").name = "xtdb-jdbc"

include("http-server", "http-client-jvm")
project(":http-server").name = "xtdb-http-server"
project(":http-client-jvm").name = "xtdb-http-client-jvm"

include("lang:test-harness")
project(":lang:test-harness").name = "test-harness"

include("docker:standalone", "docker:aws", "docker:azure", "docker:google-cloud")
include("cloud-benchmark", "cloud-benchmark:aws", "cloud-benchmark:azure", "cloud-benchmark:google-cloud", "cloud-benchmark:local")

include("modules:kafka", "modules:kafka-connect", "modules:aws", "modules:azure", "modules:google-cloud")
project(":modules:kafka").name = "xtdb-kafka"
project(":modules:kafka-connect").name = "xtdb-kafka-connect"
project(":modules:aws").name = "xtdb-aws"
project(":modules:azure").name = "xtdb-azure"
project(":modules:google-cloud").name = "xtdb-google-cloud"

include("modules:c1-import", "modules:flight-sql")
project(":modules:flight-sql").name = "xtdb-flight-sql"

include("modules:bench", "modules:datasets")
project(":modules:datasets").name = "xtdb-datasets"

include("monitoring", "monitoring:docker-image")
