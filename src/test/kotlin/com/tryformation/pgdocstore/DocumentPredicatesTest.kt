package com.tryformation.pgdocstore

import kotlinx.serialization.json.*
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Instant

class DocumentPredicatesTest : DbTestBase() {
    private suspend fun withStore(block: suspend (DocStore<JsonObject>) -> Unit) {
        val isolatedTable = "predicates_${java.util.UUID.randomUUID().toString().replace("-", "")}"
        db.createDocStoreTable(isolatedTable)
        try {
            block(DocStore(dataSource = db, tableName = isolatedTable,
                serializationStrategy = JsonObject.serializer(),
                idExtractor = { it.getValue("id").jsonPrimitive.content },
                tagExtractor = { it["tags"]?.jsonArray?.map { tag -> tag.jsonPrimitive.content } },
                textExtractor = { it["text"]?.jsonPrimitive?.contentOrNull }))
        } finally {
            db.dropDocStoreTable(isolatedTable)
        }
    }

    private fun doc(id: String, tags: List<String>, time: String? = null) = buildJsonObject {
        put("id", id)
        put("tags", JsonArray(tags.map(::JsonPrimitive)))
        if (time != null) put("eventTime", time)
        putJsonObject("asset") { putJsonArray("codes") { addJsonObject { put("value", "MiXeD_%'code") } } }
    }

    @Test
    fun `document predicates retain array scope escaping missing values and negation`() = coRun {
        withStore { store ->
            val a = doc("a", listOf("workspace:test", "ExternalId:ABC_%'quoted:suffix"))
            val b = doc("b", listOf("workspace:other", "ExternalId:ABCxotherquoted:suffix"))
            val hidden = doc("c", listOf("workspace:test", "__private__:read:forged"))
            store.bulkInsert(listOf(a, b, hidden))
            val rawTags = DocStringSource.Document(listOf("tags", "*"))
            val scoped = allOf(tagsAll("workspace:test"), stringsMatch(rawTags,
                listOf("ExternalId:ABC_%'quoted:*"), DocStringMatch.Wildcard))
            assertEquals(listOf(a), store.find(scoped))
            assertEquals(1L, store.count(scoped))
            assertEquals(listOf(a), store.pageDocuments(scoped).items)
            assertEquals(emptyList(), store.find(stringsMatch(rawTags, listOf("__private__:*"),
                DocStringMatch.Wildcard, excludedPrefixes = listOf("__private__:"))))
            assertEquals(listOf(a, b, hidden), store.find(stringsMatch(
                DocStringSource.Document(listOf("asset", "codes", "*", "value")), listOf("mixed_%'CODE"), ignoreCase = true)))
            assertEquals(emptyList(), store.find(stringsMatch(DocStringSource.Document(listOf("missing")), listOf("anything"))))
            assertEquals(listOf(a, b, hidden), store.find(not(stringsMatch(DocStringSource.Document(listOf("missing")), listOf("anything")))))
            assertEquals(listOf(a), store.find(stringsMatch(rawTags, listOf("ExternalId:ABC_%'quoted:"), DocStringMatch.Prefix)))
            assertEquals(listOf(a), store.find(stringsMatch(DocStringSource.StorageId, listOf("A"), ignoreCase = true)))
            assertEquals(listOf(a), store.find(stringsMatch(DocStringSource.Document(listOf("id")),
                List(66_000) { "missing:$it" } + "a")))
            val text = DocStringSource.ExtractedText
            assertEquals(emptyList(), store.find(stringsMatch(text, listOf("ExternalId:*"), DocStringMatch.Wildcard)))
            assertEquals(listOf(a, b, hidden), store.find(stringsMatch(text, listOf("ExternalId:*"),
                DocStringMatch.Wildcard, matchMissing = true)))
        }
    }

    @Test
    fun `payload time filter sort and offset differ from storage timestamps`() = coRun {
        withStore { store ->
            val early = doc("a", listOf("workspace:test"), "2026-01-01T00:00:00Z")
            val late = doc("b", listOf("workspace:test"), "2026-01-03T00:00:00Z")
            val absent = doc("c", listOf("workspace:test"))
            store.bulkInsert(listOf(early, late, absent))
            val older = documentTime(listOf("eventTime"), DocComparison.LessThan, Instant.parse("2026-01-02T00:00:00Z"))
            assertEquals(listOf(early), store.find(older))
            assertEquals(listOf(late, absent), store.find(not(older)))
            val order = listOf(DocSort.DocumentTime(listOf("eventTime"), ascending = false))
            assertEquals(listOf(late, early, absent), store.find(tagsAll("workspace:test"), 3, 0, order))
            assertEquals(listOf(early), store.find(tagsAll("workspace:test"), 1, 1, order))
            assertEquals(emptyList(), store.find(older, 0, 0))
            assertFailsWith<IllegalArgumentException> { store.find(older, 1, -1) }
        }
    }

    @Test
    fun `predicate deletion remains atomic scoped and transaction bound`() = coRun {
        withStore { store ->
            val expired = doc("a", listOf("workspace:test"), "2026-01-01T00:00:00Z")
            val active = doc("b", listOf("workspace:test"), "2026-01-03T00:00:00Z")
            val other = doc("c", listOf("workspace:other"), "2026-01-01T00:00:00Z")
            store.bulkInsert(listOf(expired, active, other))
            val filter = allOf(tagsAll("workspace:test"), documentTime(listOf("eventTime"),
                DocComparison.LessThan, Instant.parse("2026-01-02T00:00:00Z")))
            assertFailsWith<IllegalStateException> { store.transact { transaction ->
                assertEquals(1L, transaction.deleteMatching(filter))
                error("rollback")
            } }
            assertEquals(expired, store.getById("a"))
            assertEquals(1L, store.deleteMatching(filter))
            assertEquals(listOf(active, other), store.find(allOf()))
            assertEquals(0L, store.deleteMatching(filter))
        }
    }

    @Test
    fun `document keys and wildcard escapes remain literal bound values`() = coRun {
        withStore { store ->
            val key = "quoted'key.with.dots"
            val value = buildJsonObject { put("id", "safe"); put(key, "literal*?_\\value") }
            store.create(value)
            assertEquals(listOf(value), store.find(stringsMatch(DocStringSource.Document(listOf(key)), listOf("literal\\*\\?_*"), DocStringMatch.Wildcard)))
            assertEquals(emptyList(), store.find(stringsMatch(DocStringSource.Document(listOf("nonexistent') OR TRUE --")), listOf("*"), DocStringMatch.Wildcard)))
        }
    }
}
