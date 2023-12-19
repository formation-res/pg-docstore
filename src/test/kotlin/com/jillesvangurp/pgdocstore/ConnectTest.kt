package com.jillesvangurp.pgdocstore

import com.github.jasync.sql.db.QueryResult
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.random.Random
import kotlin.random.nextULong


@Suppress("UNCHECKED_CAST")
class ConnectTest : DbTest() {

    @Test
    fun shouldConnect() = coRun {
        db.sendPreparedStatement(
            """select 1+1 as answer""", listOf()
        ).let {
            it.rows.first()["answer"] shouldBe 2
        }

        db.inTransaction { c ->
            c.sendQuery("select 2+2 as answer").let { qr ->
                qr.rows.first()["answer"] shouldBe 4
            }
        }
    }

    @Test
    fun shouldCreateTableAndMessWithSomeCursors() = coRun {
        db.reCreateDocStoreSchema()

        val ids = mutableSetOf<String>()
        db.inTransaction { c->
            repeat(2000) {count ->
                val id = UUID.randomUUID().toString()
                ids.add(id)
                val text = """
                    OHAI, this is doc #$count
                """.trimIndent()
                val tags = listOf("foo", "bar")
                c.sendPreparedStatement(
                    """
                        INSERT INTO docstore (id, json,tags)
                        VALUES (?,?,?); 
                    """.trimIndent(),
                    listOf(id,text,tags)
                )
            }
        }
        db.inTransaction {c->
            c.sendQuery("select count(*) as count from docstore").rows.first()["count"] shouldBe 2000

            val cursorId = "cursor_${Random.nextULong()}"
            c.sendQuery("""
                DECLARE $cursorId CURSOR FOR 
                select * from docstore 
                order by id;
            """.trimIndent())

            val fetchedIds = mutableListOf<String>()
            try {
                println("$cursorId created")
                var resp: QueryResult? = null
                while (resp == null || resp.rows.isNotEmpty()) {
                    println("FETCH")
                    resp = c.sendQuery("FETCH 99 FROM $cursorId;")
                    resp.rows.forEach { row ->
                        fetchedIds.add(row.getString("id")!!)
                    }
                }
            } finally {
                println("closing")
                c.sendQuery("CLOSE $cursorId;")
            }
            fetchedIds.size shouldBe 2000

            c.sendPreparedStatement(
                """SELECT tags from docstore where id = ?""", listOf(fetchedIds.first())
            ).let { qr ->
                val ts = qr.rows.first()["tags"]!! as List<String>

                println(ts)
            }
        }
    }
}

