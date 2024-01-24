pluginManagement {
    plugins {
        kotlin("jvm") version "1.9.22"
        kotlin("plugin.serialization") version "1.9.22"
        id("org.jetbrains.dokka") version "1.9.10"
    }
}

rootProject.name = "xtdb"

include("api", "core")
project(":api").name = "xtdb-api"
project(":core").name = "xtdb-core"

include("http-server", "http-client-clj", "pgwire-server")
project(":http-server").name = "xtdb-http-server"
project(":http-client-clj").name = "xtdb-http-client-clj"
project(":pgwire-server").name = "xtdb-pgwire-server"

include("docker:standalone","docker:aws")

include("modules:jdbc", "modules:kafka", "modules:s3", "modules:azure", "modules:google-cloud")
project(":modules:jdbc").name = "xtdb-jdbc"
project(":modules:kafka").name = "xtdb-kafka"
project(":modules:s3").name = "xtdb-s3"
project(":modules:azure").name = "xtdb-azure"
project(":modules:google-cloud").name = "xtdb-google-cloud"

include("modules:c1-import", "modules:flight-sql")
project(":modules:flight-sql").name = "xtdb-flight-sql"

include("modules:bench", "modules:datasets")
