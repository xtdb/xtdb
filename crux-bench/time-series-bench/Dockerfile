FROM 955308952094.dkr.ecr.eu-west-2.amazonaws.com/crux-bench:base

CMD cd crux/ && \
    git pull && \
    lein sub install && \
    cd crux-bench/time-series-bench && \
    cp -r /usr/local/lib/crux-bench/data . && \
    lein run
