## GraphQL Webservice

An example webservice showing an end-to-end Crux deployment, with the following properties:

* deployed using Docker, using a Confluent on-prem Kafka broker (also in Docker)
* a GraphQL API over a sample dataset stored in Crux
* a GraphIQL UI to introspect the schema and run queries

## Starting the service
### Using the REPL

1. Starting a REPL

You'll want to start a Clojure deps.edn REPL in the normal way for your editor.

For Emacs/Vim, I'd recommend adding the following to your `~/.clojure/deps.edn`:

```clojure
{:aliases {:user/cider {:extra-deps {nrepl {:mvn/version "0.6.0"}, cider/cider-nrepl {:mvn/version "0.22.4"}}
                        :main-opts ["-m" "nrepl.cmdline" "--middleware" "[cider.nrepl/cider-middleware]"]}

           :user/prepl {:extra-deps {olical/propel {:mvn/version "1.3.0"}}
                        :main-opts ["m" "propel.main" "-w"]}}}
```

then (emacs):
* run `clojure -A:user/cider` in this directory to start the CIDER REPL
* connect to that REPL using `cider-connect`

(vim)
* for `vim-fireplace` follow the emacs instructions
* for `conjure` run `clojure -A:user/prepl` to start the pREPL and write a port to `.prepl-file`. To connect add the following to `~/.config/conjure/conjure.edn`
```
{:conns {:clj {:port #slurp-edn ".prepl-port"}}}
```

Incidentally, you can then use `clojure -A:user/cider`/`clojure -A:user/prepl` to start CIDER REPLs/pREPLs for any Clojure deps.edn project.

2. Starting the application

Head to the `webservice.main` namespace, eval it, and then eval the `(swap! !ig-system (comp start-system stop-system))` form within the `-main` function. This starts the Crux node, and an HTTP server on port 3000.

This also serves as your 'reload server' form (`stop-system` is a no-op if there's not a started system).

* That said, you shouldn't need to reload the whole server on a regular basis - by referring to the handlers as vars (e.g. `#'root-handler`), re-eval'ing the handler form is sufficient to update the running HTTP server.
* A server reload (using that form) is required if you change the routes, or the Crux configuration
* A JVM reload is required if you add/upgrade dependencies (in deps.edn)

### Using Docker

Run `docker-compose up -d` in the root of this folder.

### In the browser

Head to <http://localhost:3000>.
