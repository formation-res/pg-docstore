package com.tryformation.pgdocstore

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource
import kotlin.test.*
import kotlin.time.Clock
import kotlin.time.Duration.Companion.days
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

@Execution(ExecutionMode.SAME_THREAD)
class ReliabilityTest : DbTestBase() {
    @BeforeEach
    fun clearTable() = coRun {
        db.withConnection { conn -> conn.createStatement().use { it.execute("TRUNCATE $tableName") } }
    }

    private fun store(source: DataSource = db) = DocStore(
        source, TaggedModel.serializer(), tableName,
        tagExtractor = TaggedModel::tags, textExtractor = TaggedModel::title
    )

    @Test
    fun `updates join outer transaction and see its inserts`() = coRun {
        val ds = store()
        val existing = TaggedModel("before")
        val inserted = TaggedModel("new")
        ds.create(existing)
        assertFailsWith<IllegalStateException> {
            ds.transact { tx ->
                tx.update(existing.id) { it.copy(title = "after") }
                tx.create(inserted)
                tx.update(inserted.id) { it.copy(title = "changed new") }
                assertEquals("after", tx.getById(existing.id)!!.title)
                assertEquals("changed new", tx.getById(inserted.id)!!.title)
                error("rollback")
            }
        }
        assertEquals("before", ds.getById(existing.id)!!.title)
        assertNull(ds.getById(inserted.id))
    }

    @Test
    fun `updates commit through nested transactions`() = coRun {
        val ds = store()
        val doc = TaggedModel("before")
        ds.transact { outer ->
            outer.create(doc)
            outer.transact { inner -> inner.update(doc.id) { it.copy(title = "after") } }
        }
        assertEquals("after", ds.getById(doc.id)!!.title)
    }

    @Test
    fun `concurrent increments are serialized`() = coRun {
        val ds = store()
        val doc = TaggedModel("0")
        ds.create(doc)
        coroutineScope {
            (1..24).map {
                async {
                    ds.update(doc.id) { current ->
                        delay(5) // Widen the read-modify-write race without coordinating under the row lock.
                        current.copy(title = (current.title.toInt() + 1).toString())
                    }
                }
            }.awaitAll()
        }
        assertEquals("24", ds.getById(doc.id)!!.title)
    }

    @Test
    fun `delete waits for an in-progress update`() = coRun {
        val ds = store()
        val doc = TaggedModel("before")
        ds.create(doc)
        val updating = CompletableDeferred<Unit>()
        val finishUpdate = CompletableDeferred<Unit>()
        coroutineScope {
            val update = async {
                ds.update(doc.id) {
                    updating.complete(Unit)
                    finishUpdate.await()
                    it.copy(title = "after")
                }
            }
            updating.await()
            val deletion = async { ds.delete(doc.id) }
            try {
                awaitBlockedQuery()
                assertFalse(deletion.isCompleted)
            } finally {
                finishUpdate.complete(Unit)
            }
            update.await()
            deletion.await()
        }
        assertNull(ds.getById(doc.id))
    }

    @Test
    fun `cancellation cancels a blocked write before the lock is released`() = coRun {
        val ds = store()
        val doc = TaggedModel("before")
        ds.create(doc)
        val locked = CompletableDeferred<Unit>()
        val releaseLock = CompletableDeferred<Unit>()
        coroutineScope {
            val holder = async {
                db.withTransaction { conn ->
                    conn.prepareStatement("SELECT id FROM $tableName WHERE id = ? FOR UPDATE").use { ps ->
                        ps.setString(1, doc.id)
                        ps.executeQuery().use { rs -> assertTrue(rs.next()) }
                    }
                    locked.complete(Unit)
                    releaseLock.await()
                }
            }
            locked.await()
            val writer = async {
                ds.transact { tx -> tx.create(doc.copy(title = "after"), onConflictUpdate = true) }
            }
            try {
                awaitBlockedQuery()
                writer.cancel()
                // This must finish while the other transaction still owns the row lock.
                withTimeout(5000) { writer.join() }
            } finally {
                releaseLock.complete(Unit)
            }
            holder.await()
        }
        assertEquals("before", ds.getById(doc.id)!!.title)
    }

    @Test
    fun `cancellation cancels a blocked batch deletion before the lock is released`() = coRun {
        val ds = store()
        val doc = TaggedModel("keep")
        ds.create(doc)
        val locked = CompletableDeferred<Unit>()
        val releaseLock = CompletableDeferred<Unit>()
        coroutineScope {
            val holder = async {
                db.withTransaction { conn ->
                    conn.prepareStatement("SELECT id FROM $tableName WHERE id = ? FOR UPDATE").use { ps ->
                        ps.setString(1, doc.id)
                        ps.executeQuery().use { rs -> assertTrue(rs.next()) }
                    }
                    locked.complete(Unit)
                    releaseLock.await()
                }
            }
            locked.await()
            val deletion = async { ds.deleteByIds(listOf(doc.id)) }
            try {
                awaitBlockedQuery()
                deletion.cancel()
                withTimeout(5000) { deletion.join() }
            } finally {
                releaseLock.complete(Unit)
            }
            holder.await()
        }
        assertEquals(doc, ds.getById(doc.id))
        assertEquals(1L, ds.deleteByIds(listOf(doc.id)))
    }

    @Test
    fun `transaction helper rolls back cancellation observed after blocking work`() = coRun {
        val doc = TaggedModel("before")
        store().create(doc)
        val entered = CompletableDeferred<Unit>()
        val unblock = CountDownLatch(1)
        coroutineScope {
            val writer = async {
                db.withTransaction { conn ->
                    conn.prepareStatement("UPDATE $tableName SET text = 'uncommitted' WHERE id = ?").use { ps ->
                        ps.setString(1, doc.id)
                        ps.executeUpdate()
                    }
                    entered.complete(Unit)
                    check(unblock.await(5, TimeUnit.SECONDS))
                }
            }
            entered.await()
            writer.cancel()
            unblock.countDown()
            writer.join()
        }
        assertEquals("before", store().getEntryById(doc.id)!!.text)
    }

    @Test
    fun `transactions roll back Errors as well as Exceptions`() = coRun {
        val ds = store()
        val doc = TaggedModel("error")
        assertFailsWith<AssertionError> {
            ds.transact { tx -> tx.create(doc); throw AssertionError("failure") }
        }
        assertNull(ds.getById(doc.id))
    }

    @Test
    fun `scrolling uses a server cursor and releases its connection on early completion`() = coRun {
        val tracking = InspectingDataSource(db, tableName)
        val ds = store(tracking)
        ds.bulkInsert((1..250).map { TaggedModel("document $it") })
        assertEquals(3, ds.documentsByRecencyScrolling(fetchSize = 7).take(3).count())
        val query = tracking.queries.single()
        assertFalse(query.autoCommit)
        assertEquals(7, query.fetchSize)
        assertTrue(query.hasCursor, "pg_cursors must contain a named server portal")
        assertEquals(0, tracking.activeConnections.get())
        assertEquals(250, ds.documentsByRecencyScrolling(fetchSize = 11).count())
        assertEquals(0, tracking.activeConnections.get())
    }

    @Test
    fun `scrolling cancellation releases its connection`() = coRun {
        val tracking = InspectingDataSource(db, tableName)
        val ds = store(tracking)
        ds.bulkInsert((1..250).map { TaggedModel("document $it") })
        val received = CompletableDeferred<Unit>()
        coroutineScope {
            val collector = launch {
                ds.entriesByRecencyScrolling(fetchSize = 7).collect {
                    received.complete(Unit)
                    awaitCancellation()
                }
            }
            received.await()
            collector.cancelAndJoin()
        }
        assertEquals(0, tracking.activeConnections.get())
        assertEquals(250, ds.count())
    }

    @Test
    fun `all query variants share normalized text tags and date parameters`() = coRun {
        val ds = DocStore(db, TaggedModel.serializer(), tableName,
            tagExtractor = TaggedModel::tags, textExtractor = TaggedModel::title,
            indexNormalizer = { it.reversed() })
        val now = Clock.System.now()
        val expected = TaggedModel("abcdefg", listOf("a", "b"))
        ds.create(expected, now - 2.days)
        ds.create(TaggedModel("abcdefg", listOf("a")), now - 2.days)
        ds.create(TaggedModel("abcdefg", listOf("a", "b")), now - 5.days)
        for (threshold in listOf(0.1, 1.0)) {
            val docs = ds.documentsByRecency(tags = listOf("a", "b"), query = "abcdefg", similarityThreshold = threshold,
                updatedAtAfter = now - 3.days, updatedAtBefore = now - 1.days)
            val entries = ds.entriesByRecency(tags = listOf("a", "b"), query = "abcdefg", similarityThreshold = threshold,
                updatedAtAfter = now - 3.days, updatedAtBefore = now - 1.days)
            val streamed = ds.documentsByRecencyScrolling(tags = listOf("a", "b"), query = "abcdefg", similarityThreshold = threshold,
                updatedAtAfter = now - 3.days, updatedAtBefore = now - 1.days, fetchSize = 1).toList()
            val streamedEntries = ds.entriesByRecencyScrolling(tags = listOf("a", "b"), query = "abcdefg", similarityThreshold = threshold,
                updatedAtAfter = now - 3.days, updatedAtBefore = now - 1.days, fetchSize = 1).toList()
            assertEquals(listOf(expected), docs)
            assertEquals(docs, streamed)
            assertEquals(entries, streamedEntries)
            assertEquals(listOf(expected.id), entries.map { it.id })
            if (threshold != 1.0) assertEquals(1.0f, entries.single().similarity)
        }
        assertEquals(3, ds.documentsByRecencyScrolling(query = "   ").count())
        assertEquals(3, ds.entriesByRecencyScrolling(query = "").count())
    }

    @Test
    fun `rank and matching both use bound normalized query text`() = coRun {
        val ds = DocStore(db, TaggedModel.serializer(), tableName,
            textExtractor = TaggedModel::title, indexNormalizer = { it.reversed() })
        val exact = TaggedModel("hello ' quoted; -- world")
        ds.create(exact)
        ds.create(TaggedModel("hello quoted worlds"))
        val results = ds.entriesByRecency(query = exact.title, similarityThreshold = 0.1)
        assertEquals(exact.id, results.first().id)
        assertEquals(1.0f, results.first().similarity)
    }

    @Test
    fun `thresholds remain exact and do not leak across pooled queries`() = coRun {
        val ds = store()
        ds.create(TaggedModel("bar"))
        ds.create(TaggedModel("bar foo"))
        val cases = listOf(0.0, 0.01, 0.1, 0.3, 0.499999999, 0.5, 0.500000001, 0.9, 0.999)
        val expected = cases.associateWith { threshold ->
            db.withConnection { conn ->
                conn.prepareStatement("SELECT id FROM $tableName WHERE similarity(text, ?) >= ?::double precision ORDER BY similarity(text, ?) DESC, updated_at DESC, id").use { ps ->
                    ps.setString(1, "bar"); ps.setDouble(2, threshold); ps.setString(3, "bar")
                    ps.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getString(1)) } }
                }
            }
        }
        coroutineScope {
            (cases + cases.reversed()).map { threshold ->
                async { assertEquals(expected.getValue(threshold), ds.entriesByRecency(query = "bar", similarityThreshold = threshold).map { it.id }) }
            }.awaitAll()
        }
        db.withConnection { conn ->
            conn.createStatement().use { st ->
                st.execute("SET pg_trgm.similarity_threshold = 0.42")
                val bound = DocStore(db, TaggedModel.serializer(), tableName, connection = conn)
                conn.autoCommit = false
                bound.entriesByRecency(query = "bar", similarityThreshold = 0.1)
                bound.entriesByRecencyScrolling(query = "bar", similarityThreshold = 0.9).toList()
                st.executeQuery("SHOW pg_trgm.similarity_threshold").use { rs -> rs.next(); assertEquals("0.42", rs.getString(1)) }
                conn.rollback()
                conn.autoCommit = true
                st.execute("RESET pg_trgm.similarity_threshold")
            }
        }
    }

    @Test
    fun `tag and fuzzy queries can use the supplied GIN indexes`() = coRun {
        val tracking = InspectingDataSource(db, tableName, explain = true)
        val ds = store(tracking)
        ds.bulkInsert((1..2000).map { TaggedModel("unique document $it", listOf("tag$it")) })
        db.withConnection { conn -> conn.createStatement().use { it.execute("ANALYZE $tableName") } }
        ds.documentsByRecency(tags = listOf("tag42"))
        ds.documentsByRecency(tags = listOf("tag42", "tag43"), tagsClauseOperator = BooleanOperator.OR)
        ds.documentsByRecency(query = "unique document 42", similarityThreshold = 0.9)
        assertTrue(tracking.queries[0].plan!!.contains("idx_${tableName}_tags"))
        assertTrue(tracking.queries[1].plan!!.contains("idx_${tableName}_tags"))
        assertTrue(tracking.queries[2].plan!!.contains("idx_${tableName}_text"))
    }

    @Test
    fun `bulk chunks commit independently outside a transaction`() = coRun {
        val ds = store()
        val first = TaggedModel("first")
        assertFailsWith<IllegalStateException> {
            ds.bulkInsert(sequence {
                yield(first)
                error("upstream failure")
            }, chunkSize = 1)
        }
        assertNotNull(ds.getById(first.id))
        val flowed = TaggedModel("flowed")
        val visible = CompletableDeferred<Boolean>()
        ds.bulkInsert(flow {
            emit(flowed)
            // Flow chunking runs upstream concurrently, so wait until the inserted chunk is visible.
            visible.complete(withTimeout(5000) {
                while (ds.getById(flowed.id) == null) delay(10)
                true
            })
        }, chunkSize = 1)
        assertTrue(visible.await())
    }

    @Test
    fun `explicit transactions keep bulk chunks atomic`() = coRun {
        val ds = store()
        val doc = TaggedModel("chunk")
        assertFailsWith<IllegalStateException> {
            ds.transact { tx ->
                tx.bulkInsert(listOf(doc, doc.copy(id = "second")), chunkSize = 1)
                assertEquals(2, tx.count())
                assertEquals(0, ds.count())
                error("rollback all chunks")
            }
        }
        assertEquals(0, ds.count())
    }

    @Test
    fun `bulk flow failure rolls back all chunks in an explicit transaction`() = coRun {
        val ds = store()
        assertFailsWith<IllegalStateException> {
            ds.transact { tx ->
                tx.bulkInsert(flow {
                    emit(TaggedModel("first"))
                    emit(TaggedModel("second"))
                    error("flow failed")
                }, chunkSize = 1)
            }
        }
        assertEquals(0, ds.count())
    }

    @Test
    fun `early fuzzy flow termination leaves enclosing transaction usable and restores settings`() = coRun {
        val ds = store()
        ds.bulkInsert((1..150).map { TaggedModel("bar $it") })
        db.withTransaction { conn ->
            conn.createStatement().use { it.execute("SET LOCAL pg_trgm.similarity_threshold = 0.42") }
            val tx = DocStore(db, TaggedModel.serializer(), tableName, textExtractor = TaggedModel::title, connection = conn)
            assertEquals(2, tx.entriesByRecencyScrolling(query = "bar", fetchSize = 1).take(2).count())
            assertEquals(150, tx.count())
            conn.createStatement().use { st ->
                st.executeQuery("SHOW pg_trgm.similarity_threshold").use { rs ->
                    rs.next()
                    assertEquals("0.42", rs.getString(1))
                }
            }
            tx.update(tx.documentsByRecency(limit = 1).single().id) { it.copy(title = "changed") }
        }
        assertEquals(1, ds.documentsByRecency(query = "changed", similarityThreshold = 1.0).size)
    }

    @Test
    fun `scrolling is cold and can be collected repeatedly`() = coRun {
        val tracking = InspectingDataSource(db, tableName)
        val ds = store(tracking)
        val results = ds.documentsByRecencyScrolling(fetchSize = 1)
        assertEquals(0, tracking.activeConnections.get())
        assertTrue(tracking.queries.isEmpty())
        ds.create(TaggedModel("one"))
        assertEquals(1, results.count())
        ds.create(TaggedModel("two"))
        assertEquals(2, results.count())
        assertEquals(0, tracking.activeConnections.get())
    }

    @Test
    fun `query validation rejects invalid sizes and thresholds before opening connections`() = coRun {
        val tracking = InspectingDataSource(db, tableName)
        val ds = store(tracking)
        assertFailsWith<IllegalArgumentException> { ds.documentsByRecencyScrolling(fetchSize = 0) }
        assertFailsWith<IllegalArgumentException> { ds.entriesByRecency(limit = -1) }
        assertFailsWith<IllegalArgumentException> { ds.documentsByRecency(similarityThreshold = Double.NaN) }
        assertEquals(0, tracking.activeConnections.get())
        assertTrue(tracking.queries.isEmpty())
    }

    @Test
    fun `re-extraction preserves JSON creation timestamps and explicit row ids`() = coRun {
        val original = DocStore(db, TaggedModel.serializer(), tableName)
        val doc = TaggedModel("abcdefg", listOf("newtag"), id = "embedded-id")
        original.create("explicit-id", doc, Clock.System.now() - 10.days)
        val before = original.getEntryById("explicit-id")!!
        val reindexer = DocStore(db, TaggedModel.serializer(), tableName,
            tagExtractor = TaggedModel::tags, textExtractor = TaggedModel::title, indexNormalizer = { it.reversed() })
        reindexer.reExtract()
        val after = original.getEntryById("explicit-id")!!
        assertEquals(before.copy(tags = listOf("newtag"), text = "gfedcba", updatedAt = after.updatedAt), after)
        assertTrue(after.updatedAt > before.updatedAt)
        assertNull(original.getById("embedded-id"))
    }

    @Test
    fun `re-extraction respects concurrent updates and deletes`() = coRun {
        val ds = store()
        val deleted = TaggedModel("to delete", id = "a")
        val updated = TaggedModel("to update", id = "b")
        ds.bulkInsert(listOf(deleted, updated))
        val extracted = CompletableDeferred<Unit>()
        val proceed = CountDownLatch(1)
        val reindexer = DocStore(db, TaggedModel.serializer(), tableName, tagExtractor = {
            if (it.id == "a") {
                extracted.complete(Unit)
                check(proceed.await(5, TimeUnit.SECONDS))
            }
            listOf("re-extracted")
        })
        coroutineScope {
            val task = async { reindexer.reExtract() }
            try {
                extracted.await()
                ds.delete(deleted.id)
                ds.update(updated.id) { it.copy(title = "newer", tags = listOf("current")) }
            } finally {
                proceed.countDown()
            }
            task.await()
        }
        assertNull(ds.getById(deleted.id))
        assertEquals("newer", ds.getById(updated.id)!!.title)
        assertEquals(listOf("current"), ds.getEntryById(updated.id)!!.tags)
        assertEquals("newer", ds.getEntryById(updated.id)!!.text)
    }

    @Test
    fun `re-extraction works with a single connection and joins an outer transaction`() = coRun {
        connectionPool(jdbcUrl = System.getProperty("pgdocstore.test.jdbcUrl", "jdbc:postgresql://localhost:55432/docstore"), maximumPoolSize = 1, minimumIdle = 0).use { pool ->
            val ds = DocStore(pool, TaggedModel.serializer(), tableName, tagExtractor = { listOf("new") })
            store().bulkInsert((1..205).map { TaggedModel("document $it", listOf("old")) })
            assertFailsWith<IllegalStateException> {
                ds.transact { tx ->
                    tx.reExtract()
                    assertEquals(205, tx.documentsByRecencyScrolling(tags = listOf("new"), fetchSize = 7).count())
                    error("rollback")
                }
            }
            assertEquals(205, store().documentsByRecencyScrolling(tags = listOf("old")).count())
            ds.reExtract()
            assertEquals(205, ds.documentsByRecencyScrolling(tags = listOf("new")).count())
        }
    }
    private suspend fun awaitBlockedQuery() = withTimeout(5000) {
        while (true) {
            val blocked = db.withConnection { conn ->
                conn.prepareStatement("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND wait_event_type = 'Lock' AND query LIKE ?)").use { ps ->
                    ps.setString(1, "%$tableName%")
                    ps.executeQuery().use { rs -> rs.next(); rs.getBoolean(1) }
                }
            }
            if (blocked) break
            delay(10)
        }
    }

}

/** Inspect real JDBC execution without replacing the database or exposing production query internals. */
private class InspectingDataSource(
    private val delegate: DataSource,
    private val tableName: String,
    private val explain: Boolean = false,
) : DataSource by delegate {
    data class Query(val autoCommit: Boolean, val fetchSize: Int, val hasCursor: Boolean, val plan: String?)
    val queries = CopyOnWriteArrayList<Query>()
    val activeConnections = AtomicInteger()

    override fun getConnection(): Connection {
        val conn = delegate.connection
        activeConnections.incrementAndGet()
        return proxy(Connection::class.java) { method, args ->
            when (method.name) {
                "close" -> try { invoke(method, conn, args) } finally { activeConnections.decrementAndGet() }
                "prepareStatement" -> {
                    val ps = invoke(method, conn, args) as PreparedStatement
                    val sql = args[0] as String
                    if (sql.startsWith("SELECT *") && sql.contains("FROM $tableName ")) inspect(conn, ps, sql) else ps
                }
                else -> invoke(method, conn, args)
            }
        }
    }

    private fun inspect(conn: Connection, ps: PreparedStatement, sql: String): PreparedStatement {
        val setters = mutableListOf<Pair<Method, Array<out Any?>>>()
        return proxy(PreparedStatement::class.java) { method, args ->
            if (method.name in setOf("setString", "setArray", "setTimestamp", "setObject")) setters += method to args
            val result = invoke(method, ps, args)
            if (method.name == "executeQuery") {
                val cursor = conn.createStatement().use { st ->
                    st.executeQuery("SELECT count(*) FROM pg_cursors WHERE name <> ''").use { rs -> rs.next(); rs.getInt(1) > 0 }
                }
                val plan = if (explain) {
                    conn.createStatement().use { it.execute("SET LOCAL enable_seqscan = off; SET LOCAL enable_indexscan = off; SET LOCAL enable_sort = off") }
                    conn.prepareStatement("EXPLAIN (FORMAT JSON) $sql").use { ep ->
                        setters.forEach { (setter, values) -> invoke(setter, ep, values) }
                        ep.executeQuery().use { rs -> rs.next(); rs.getString(1) }
                    }
                } else null
                queries += Query(conn.autoCommit, ps.fetchSize, cursor, plan)
            }
            result
        }
    }

    private fun invoke(method: Method, target: Any, args: Array<out Any?>): Any? = try {
        method.invoke(target, *args)
    } catch (e: InvocationTargetException) { throw e.targetException }

    private fun <T> proxy(type: Class<T>, handler: (Method, Array<out Any?>) -> Any?): T = type.cast(
        Proxy.newProxyInstance(type.classLoader, arrayOf(type)) { _, method, args -> handler(method, args ?: emptyArray()) }
    )
}
