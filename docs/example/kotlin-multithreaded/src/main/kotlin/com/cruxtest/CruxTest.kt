package com.cruxtest

import clojure.lang.Keyword
import crux.api.Crux

fun main(args: Array<String>) {
    val cruxThreads = (0..2).map { i ->
        println("doing a thread $i")
        val ti = Thread {
            val m = mapOf(
                Keyword.intern("kv-backend") to "crux.kv.memdb.MemKv",
                Keyword.intern("db-dir") to "data-$i/db-dir",
                Keyword.intern("event-log-dir") to "data-$i/eventlog"
            )

            println("Starting Crux! $i")
            val node = Crux.startStandaloneNode(m)
            println("Started Crux! $i")
            val db = node.db()
            println("Obtained Crux datasource $i")
        }
        ti.setName("crux-runner-$i")
        ti.start()
        ti
    }

    readLine()
}
