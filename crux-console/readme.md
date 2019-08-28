# Crux Console

Console is a webapp that gives allows you to query Crux and see data from different perpectives.
![Screenshot of Crux Console](./resources/screenshot-1.png)

## How to build or develop

### Prerequisites

All builds require `node` and `yarn` to be installed.

`yarn` is, at the moment, a much better alternative to npm.
Yarn ROI is that in 2-3 uses it will save more time than you spent
installing and learning it, compared to 2-3 runs of npm install.


### Release flow
1. `lein build` will produce you production ready assets
2. `lein build-ebs` does the build above and packs it into
    Elastic Beanstalk package which we upload to AWS to see console.crux.cloud


### Dev flow
To launch development REPL

#### Preferred
```sh
lein cljs-dev  # will install all the node modules, launch cljs compiling guard with code hotswapping
dev/run.sh     # will launch edge rebel repl
```


#### Alternative
```sh
# once
yarn install

dev/shadow-dev &
# will launch shadow-cljs watch build
# it runs a local version

dev/run.sh 
# will launch the server
```



## Sidenotes

#### Plotly
Note that Plotly has its own packaging model (due to heavy weight beneath),
so search for different packages on npm if you need more fine-grained control.
e.g.

- All-in-one https://www.npmjs.com/package/plotly.js-dist
