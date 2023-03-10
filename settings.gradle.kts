rootProject.name = "core2"

include("api", "core", "wire-formats")
include("http-server", "http-client-clj", "pgwire-server")
include("docker")

include("modules:jdbc", "modules:kafka", "modules:s3")
include("modules:c1-import", "modules:flight-sql")
include("modules:bench", "modules:datasets")
