# Crux Console


## Release flow
1. `lein build` will produce you production ready assets
2. `lein build-ebs` does the build above and packs it into
    ebs which we upload to AWS to see console.crux.cloud


## Dev flow
To launch development REPL

```sh
# once
yarn install

dev/shadow-dev &
# will launch shadow-cljs watch build
# it runs a local version

dev/run.sh 
# will launch the server
```

#### Plotly
Note that Plotly has its own packaging model (due to heavy weight beneath),
so search for different packages on npm if you need more power.
e.g.

- All-in-one https://www.npmjs.com/package/plotly.js-dist
