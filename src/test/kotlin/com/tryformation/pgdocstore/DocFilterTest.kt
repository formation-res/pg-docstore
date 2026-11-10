package com.tryformation.pgdocstore

import java.sql.Timestamp
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.time.toJavaInstant
import org.junit.jupiter.api.Test

class DocFilterTest : DbTestBase() {
    private val store get() = DocStore(
        dataSource = db,
        serializationStrategy = TaggedModel.serializer(),
        tableName = tableName,
        tagExtractor = { it.tags.ifEmpty { null } },
    )

    @Test
    fun `nested filters apply workspace scope and exclusions before the limit`() = coRun {
        val a = TaggedModel("other workspace", listOf("workspace:other", "visible", "parent:a"), "query-1")
        val b = TaggedModel("deleted", listOf("workspace:test", "visible", "parent:a", "deleted"), "query-2")
        val c = TaggedModel("first child", listOf("workspace:test", "visible", "parent:a"), "query-3")
        val d = TaggedModel("second child", listOf("workspace:test", "visible", "parent:b"), "query-4")
        val e = TaggedModel("unrelated", listOf("workspace:test", "visible", "parent:c"), "query-5")
        val f = TaggedModel("legacy exception", listOf("workspace:test", "visible"), "query-6")
        val hidden = TaggedModel("hidden child", listOf("workspace:test", "parent:a"), "query-0")
        val rows = listOf(a, b, c, d, e, f, hidden)
        store.bulkInsert(rows)

        val filter = allOf(
            tagsAll("workspace:test", "visible"),
            anyOf(tagsAny("parent:a", "parent:b"), idsIn(listOf(f.id))),
            not(tagsAny("deleted")),
        )
        assertEquals(listOf(c, d), store.find(filter, limit = 2))
        assertEquals(3L, store.count(filter))
        assertEquals(listOf(c.id, d.id), store.pageIds(filter, limit = 2).items)
        assertEquals(listOf(c, d, f), store.pageDocuments(filter).items)
        assertEquals(emptyList(), store.find(filter, limit = 0))
        assertFailsWith<IllegalArgumentException> { store.pageIds(filter, limit = 0) }
    }

    @Test
    fun `empty operands and null tags have defined Boolean semantics`() = coRun {
        val tagged = TaggedModel("tagged", listOf("Boolean's tag"), "boolean-1")
        val untagged = TaggedModel("no tags", id = "boolean-2")
        store.bulkInsert(listOf(tagged, untagged))
        val scope = idsIn(listOf(tagged.id, untagged.id))
        val tag = tagsAll("Boolean's tag")
        val everything = listOf(tagged, untagged)

        assertEquals(everything, store.find(allOf(scope, allOf(), tagsAll(), updatedAt())))
        assertEquals(emptyList(), store.find(allOf(scope, anyOf())))
        assertEquals(emptyList(), store.find(allOf(scope, tagsAny())))
        assertEquals(emptyList(), store.find(allOf(scope, idsIn(emptyList()))))
        assertEquals(listOf(untagged), store.find(allOf(scope, not(tag))))
        assertEquals(listOf(tagged), store.find(allOf(scope, not(not(tag)))))
        assertEquals(listOf(untagged), store.find(allOf(scope, not(anyOf(tag, tagsAny("absent"))))))
    }

    @Test
    fun `large ID and tag groups bind as arrays`() = coRun {
        val doc = TaggedModel("large filters", listOf("member:target"), "array-target")
        store.create(doc)
        val alternatives = List(66_000) { "unused:$it" }

        val filter = allOf(idsIn(alternatives + doc.id), tagsAny(alternatives + doc.tags))
        assertEquals(listOf(doc), store.find(filter))
    }

    @Test
    fun `cursor order uses the full storage timestamp and ID across pages`() = coRun {
        val tagged = listOf("cursor-test")
        val later = TaggedModel("later", tagged)
        val first = TaggedModel("first", tagged)
        val second = TaggedModel("second", tagged)
        val nullA = TaggedModel("null A", tagged)
        val nullB = TaggedModel("null B", tagged)
        val instant = Instant.parse("2026-01-01T00:00:00.000123Z")
        val next = instant + 1.seconds
        val docs = listOf(first, second, later, nullA, nullB)
        val keys = listOf("cursor-b", "cursor-c", "cursor-a", "cursor-d", "cursor-e")
        store.bulkInsertWithId(keys.zip(docs))

        // JDBC-created legacy rows may have microsecond precision or lack a timestamp.
        db.withConnection { connection ->
            connection.prepareStatement("UPDATE $tableName SET updated_at = ? WHERE id = ?").use { statement ->
                keys.zip(listOf(instant, instant, next, null, null)).forEach { (key, timestamp) ->
                    statement.setTimestamp(1, timestamp?.let { Timestamp.from(it.toJavaInstant()) })
                    statement.setString(2, key)
                    statement.addBatch()
                }
                statement.executeBatch()
            }
        }

        val filter = tagsAll(tagged)
        assertEquals(docs, store.find(filter, order = DocOrder.UpdatedAtAscending))
        assertEquals(listOf(later, first, second, nullA, nullB), store.find(filter, order = DocOrder.IdAscending))
        assertEquals(listOf(first, second), store.find(allOf(filter, updatedAt(instant, instant))))
        assertEquals(1L, store.count(allOf(filter, updatedAt(fromInclusive = next))))
        val firstPage = store.pageIds(filter, limit = 1)
        assertEquals(DocPage(listOf(keys[0]), DocCursor(instant, keys[0])), firstPage)
        val secondPage = store.pageDocuments(filter, after = firstPage.nextCursor, limit = 3)
        assertEquals(DocPage(docs.subList(1, 4), DocCursor(null, keys[3])), secondPage)
        assertEquals(DocPage(listOf(keys[4]), null), store.pageIds(filter, after = secondPage.nextCursor, limit = 1))
        assertEquals(DocPage(emptyList(), null), store.pageIds(filter, after = DocCursor(null, keys[4])))
    }

    @Test
    fun `counts and ID pages work without decoding the stored document`() = coRun {
        val id = "unreadable-payload"
        db.withConnection { connection ->
            connection.prepareStatement("INSERT INTO $tableName (id, json) VALUES (?, 'not valid JSON')").use {
                it.setString(1, id)
                it.executeUpdate()
            }
        }
        val filter = idsIn(listOf(id))
        assertEquals(1L, store.count(filter))
        assertEquals(DocPage(listOf(id), null), store.pageIds(filter, limit = 1))
    }

    @Test
    fun `query operations see transaction writes and expire with the binding`() = coRun {
        val store = store
        val doc = TaggedModel("pending", listOf("transaction"), "query-transaction")
        val filter = idsIn(listOf(doc.id))
        lateinit var escaped: IDocStore<TaggedModel>

        assertFailsWith<IllegalStateException> {
            db.transact {
                val bound = bind(store)
                escaped = bound
                bound.create(doc)
                assertEquals(listOf(doc), bound.find(filter))
                assertEquals(1L, bound.count(filter))
                assertEquals(listOf(doc.id), bound.pageIds(filter).items)
                assertEquals(listOf(doc), bound.pageDocuments(filter).items)
                assertEquals(0L, store.count(filter))
                error("rollback")
            }
        }
        assertEquals(0L, store.count(filter))
        assertFailsWith<IllegalStateException> { escaped.pageIds(filter) }
        store.transact { bound ->
            bound.create(doc)
            assertEquals(listOf(doc), bound.find(filter))
        }
        assertEquals(1L, store.count(filter))
    }
}
