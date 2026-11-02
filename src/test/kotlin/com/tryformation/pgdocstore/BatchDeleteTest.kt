package com.tryformation.pgdocstore

import java.sql.Connection
import javax.sql.DataSource
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

@Execution(ExecutionMode.SAME_THREAD)
class BatchDeleteTest : DbTestBase() {
    private fun store(source: DataSource = db): IDocStore<TestModelWithId> =
        DocStore(source, TestModelWithId.serializer(), tableName)

    @BeforeEach
    fun clearTable() = coRun {
        db.withConnection { conn -> conn.createStatement().use { it.execute("TRUNCATE $tableName") } }
    }

    @Test
    fun `empty input does not acquire a connection`() = coRun {
        val unavailable = object : DataSource by db {
            override fun getConnection(): Connection = error("Must not open a connection")
            override fun getConnection(username: String?, password: String?): Connection =
                error("Must not open a connection")
        }
        assertEquals(0L, store(unavailable).deleteByIds(emptyList()))
    }

    @Test
    fun `counts only deleted rows and leaves other documents intact`() = coRun {
        val ds = store()
        val docs = (1..3).map { TestModelWithId("document $it") }
        ds.bulkInsert(docs)

        assertEquals(0L, ds.deleteByIds(listOf("missing", "also missing")))
        assertEquals(2L, ds.deleteByIds(listOf(docs[0].id, "missing", docs[0].id, docs[1].id)))
        assertNull(ds.getById(docs[0].id))
        assertNull(ds.getById(docs[1].id))
        assertEquals(docs[2], ds.getById(docs[2].id))
        assertEquals(1L, ds.count())
        assertEquals(0L, ds.deleteByIds(listOf(docs[0].id, docs[1].id)))
        assertEquals(1L, ds.deleteByIds(setOf(docs[2].id)))
    }

    @Test
    fun `binds arbitrary ids as data`() = coRun {
        val ds = store()
        val ids = listOf("", "'); DELETE FROM $tableName; --", "{a,b}", "\"quoted\"", "back\\slash", "日本語")
        ds.bulkInsert(ids.map { TestModelWithId("special id", it) } + TestModelWithId("keep", "keep"))
        assertEquals(ids.size.toLong(), ds.deleteByIds(ids))
        assertEquals(1L, ds.count())
        assertNotNull(ds.getById("keep"))
    }

    @Test
    fun `joins nested transactions and rolls back deletions`() = coRun {
        val ds = store()
        val existing = TestModelWithId("existing")
        val inserted = TestModelWithId("inserted in transaction")
        ds.create(existing)
        assertFailsWith<IllegalStateException> {
            ds.transact { tx ->
                tx.create(inserted)
                tx.transact { nested ->
                    assertEquals(2L, nested.deleteByIds(listOf(existing.id, inserted.id)))
                }
                assertNull(tx.getById(existing.id))
                assertNull(tx.getById(inserted.id))
                error("rollback")
            }
        }
        assertEquals(existing, ds.getById(existing.id))
        assertNull(ds.getById(inserted.id))
        ds.transact { tx -> assertEquals(1L, tx.deleteByIds(listOf(existing.id))) }
        assertNull(ds.getById(existing.id))
    }

    @Test
    fun `deletes a batch larger than the JDBC parameter limit`() = coRun {
        val ds = store()
        val docs = (1..70_000).map { TestModelWithId("batch document", "batch-$it") }
        ds.bulkInsert(docs, chunkSize = 1000)
        assertEquals(docs.size.toLong(), ds.deleteByIds(docs.map { it.id }))
        assertEquals(0L, ds.count())
    }
}
