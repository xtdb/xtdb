package xtdb.api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.JSON_SERDE
import xtdb.error.Incorrect
import xtdb.trimJson
import java.time.Instant

class TransactionResultTest {

    private val testError = Incorrect(errorCode = "xtdb.error/error-key", message = "test")

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
                  "@value":{
                    "xtdb.error/message":"test",
                    "xtdb.error/class":"xtdb.error.Incorrect",
                    "xtdb.error/data":{
                      "xtdb.error/code":{"@type":"xt:keyword","@value":"xtdb.error/error-key"}
                    }
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
                    "@value":{
                      "xtdb.error/message":"test",
                      "xtdb.error/class":"xtdb.error.Incorrect",
                      "xtdb.error/data":{
                        "xtdb.error/code":{"@type":"xt:keyword", "@value": "xtdb.error/error-key"}
                      }
                    }
                  }
                }
                """
            )
        )
    }

}