package com.cruxtest

import clojure.java.api.Clojure
import clojure.lang.IFn
import clojure.lang.Keyword
import taoensso.nippy.*
import crux.*
import taoensso.encore.*
import crux.index.*
import clojure.tools.reader.*
import crux.api.Crux

fun main(args: Array<String>) {

    val cruxThreads = (0..2).map { i ->
        val ti = Thread {
            val m = mapOf(
                Keyword.intern("crux.node/topology") to Keyword.intern("crux.standalone/topology"),
                Keyword.intern("crux.node/kv-store") to "crux.kv.memdb/kv",
                Keyword.intern("crux.standalone/event-log-kv-store") to "crux.kv.memdb/kv",
                Keyword.intern("crux.standalone/db-dir") to "data-$i/db-dir",
                Keyword.intern("crux.standalone/event-log-dir") to "data-$i/eventlog"
            )

            println("Starting Crux! $i")
            val node = Crux.startNode(m)
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
