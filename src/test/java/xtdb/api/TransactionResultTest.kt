package xtdb.api

import clojure.lang.Keyword
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.JSON_SERDE
import xtdb.trimJson
import java.time.Instant

class TransactionResultTest {

    private val testError = xtdb.RuntimeException(Keyword.intern("xtdb.error", "error-key"), "test")

    @Test
    fun testTransactionResultSerialization() {
        val committed = TransactionCommitted(1, Instant.ofEpochMilli(0))

        assertEquals(
            """{"txId":1,"systemTime":"1970-01-01T00:00:00Z","committed":true}""".trimJson(),
            JSON_SERDE.encodeToString(committed)
        )

        val aborted = TransactionAborted(1, Instant.ofEpochMilli(0), testError)
        assertEquals(
            """
            {
              "txId": 1,
              "systemTime": "1970-01-01T00:00:00Z",
              "committed": false,
              "error": {
                  "@type":"xt:error",
                  "@value":{"xtdb.error/message":"test",
                  "xtdb.error/class":"xtdb.RuntimeException",
                  "xtdb.error/error-key":"xtdb.error/error-key","xtdb.error/data":{}
                }
              }
            }
            """.trimJson(),
            JSON_SERDE.encodeToString(aborted)
        )
    }

    @Test
    fun testTransactionResultDeserialization() {
        val committed = TransactionCommitted(1, Instant.ofEpochMilli(0))
        assertEquals(
            committed,
            JSON_SERDE.decodeFromString<TransactionResult>("""{"txId":1,"systemTime":"1970-01-01T00:00:00Z","committed":true}""")
        )

        val aborted = TransactionAborted(1, Instant.ofEpochMilli(0), testError)
        assertEquals(
            aborted,
            JSON_SERDE.decodeFromString<TransactionResult>(
                """
                {
                  "txId": 1,
                  "systemTime": "1970-01-01T00:00:00Z",
                  "committed": false,
                  "error": {
                      "@type":"xt:error",
                      "@value":{"xtdb.error/message":"test",
                      "xtdb.error/class":"xtdb.RuntimeException",
                      "xtdb.error/error-key":"xtdb.error/error-key","xtdb.error/data":{}
                    }
                  }
                }
                """
            )
        )
    }

}