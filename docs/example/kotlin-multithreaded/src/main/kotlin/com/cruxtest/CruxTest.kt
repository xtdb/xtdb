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

    val require = Clojure.`var`("clojure.core", "require")
    require.invoke(Clojure.read("crux.api"))
    require.invoke(Clojure.read("crux.memory"))
    require.invoke(Clojure.read("crux.io"))
    require.invoke(Clojure.read("crux.codec"))
    require.invoke(Clojure.read("crux.kv.memdb"))

    val t = Thread { println("hey") }
    t.getState()

    val new = Thread.State.NEW

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
