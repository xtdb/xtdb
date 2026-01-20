# Clojure REPL Evaluation

Evaluate Clojure code via nREPL using `clj-nrepl-eval`.

## Discovering nREPL Servers

Before evaluating, discover available nREPL ports:

```bash
clj-nrepl-eval --discover-ports
```

This scans `.nrepl-port` files and running JVM/Babashka processes in the current directory.

To see previously connected servers:

```bash
clj-nrepl-eval --connected-ports
```

## Starting a REPL in XTDB

Start the Gradle REPL:

```bash
./gradlew :clojureRepl
```

For a specific port:

```bash
./gradlew :clojureRepl -PreplPort=7888
```

## Evaluating Code

### Simple Expressions

```bash
clj-nrepl-eval -p 7888 "(+ 1 2 3)"
```

### Multiline Code (Heredoc)

Use heredoc for complex code to avoid escaping issues:

```bash
clj-nrepl-eval -p 7888 <<'EOF'
(require '[xtdb.api :as xt])
(xt/status dev/node)
EOF
```

### Piping Code

```bash
echo "(+ 1 2 3)" | clj-nrepl-eval -p 7888
```

## Session Persistence

Sessions persist by default. Each host:port has its own session file. State (vars, namespaces, loaded libraries) persists across invocations until the nREPL server restarts.

```bash
# First call - define a var
clj-nrepl-eval -p 7888 "(def x 10)"

# Later call - x is still available
clj-nrepl-eval -p 7888 "(+ x 20)"
```

### Reset Session

If session becomes corrupted:

```bash
clj-nrepl-eval -p 7888 --reset-session
```

Or reset and evaluate:

```bash
clj-nrepl-eval -p 7888 --reset-session "(def x 1)"
```

## Common XTDB Workflows

### Reload and Test a Namespace

```bash
clj-nrepl-eval -p 7888 "(require 'xtdb.vector.reader :reload)"
```

### Run Tests

```bash
clj-nrepl-eval -p 7888 <<'EOF'
(require 'xtdb.vector.reader-test :reload)
(clojure.test/run-tests 'xtdb.vector.reader-test)
EOF
```

### Check dev Node Status

```bash
clj-nrepl-eval -p 7888 <<'EOF'
(require 'dev)
(in-ns 'dev)
(xt/status node)
EOF
```

## Options

- `-p, --port PORT` - nREPL port (required)
- `-H, --host HOST` - nREPL host (default: 127.0.0.1)
- `-t, --timeout MILLISECONDS` - Timeout (default: 120000)
- `-r, --reset-session` - Reset persistent nREPL session
- `-c, --connected-ports` - List active connections
- `-d, --discover-ports` - Discover nREPL servers
- `-h, --help` - Show help

## When to Use

Use this skill to:
- Verify code changes compile correctly
- Test functions interactively
- Run tests after modifications
- Check REPL state during development

Do NOT use for:
- File operations (use Read, Edit, Write, Glob, Grep instead)
