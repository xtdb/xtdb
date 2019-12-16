FROM clojure:openjdk-11-tools-deps
EXPOSE 3000
CMD ["clojure", "-m", "crux.docker"]

WORKDIR /usr/lib/crux
ADD deps.edn .
ADD src/ ./src/