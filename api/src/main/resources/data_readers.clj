;; THIRD-PARTY SOFTWARE NOTICE
;; The time readers in this file are derivative of the 'time-literals' project.
;; The time-literals license is available at https://github.com/henryw374/time-literals/blob/2a1c7e195a91dcf3638df1a2876445fc7b7872ef/LICENSE
;; https://github.com/henryw374/time-literals

{xt/tx-key xtdb.serde/map->TxKey
 xt/tx-result xtdb.serde/tx-result-read-fn
 xt/tx-opts xtdb.serde/tx-opts-read-fn
 xt/clj-form xtdb.api/->ClojureForm
 xt/illegal-arg xtdb.serde/iae-reader
 xt/runtime-err xtdb.serde/runex-reader
 xt/key-fn xtdb.serde/read-key-fn

 xt/period-duration xtdb.serde/period-duration-reader
 xt/interval-ym xtdb.serde/interval-ym-reader
 xt/interval-dt xtdb.serde/interval-dt-reader
 xt/interval-mdn xtdb.serde/interval-mdn-reader
 xt/tstz-range xtdb.serde/tstz-range-reader

 xt.tx/sql xtdb.serde/sql-op-reader
 xt.tx/xtql xtdb.serde/xtql-reader
 xt/tx-op xtdb.tx-ops/parse-tx-op

 time/period xtdb.mirrors.time-literals/period
 time/date xtdb.mirrors.time-literals/date
 time/date-time xtdb.mirrors.time-literals/date-time
 time/zoned-date-time xtdb.mirrors.time-literals/zoned-date-time
 time/offset-time xtdb.mirrors.time-literals/offset-time
 time/instant xtdb.mirrors.time-literals/instant
 time/offset-date-time xtdb.mirrors.time-literals/offset-date-time
 time/zone xtdb.mirrors.time-literals/zone
 time/day-of-week xtdb.mirrors.time-literals/day-of-week
 time/time xtdb.mirrors.time-literals/time
 time/month xtdb.mirrors.time-literals/month
 time/month-day xtdb.mirrors.time-literals/month-day
 time/duration xtdb.mirrors.time-literals/duration
 time/year xtdb.mirrors.time-literals/year
 time/year-month xtdb.mirrors.time-literals/year-month}
