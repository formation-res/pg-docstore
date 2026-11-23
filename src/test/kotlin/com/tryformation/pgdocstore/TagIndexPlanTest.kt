package com.tryformation.pgdocstore

import kotlinx.serialization.json.JsonObject
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TagIndexPlanTest : DbTestBase() {
    @Test
    fun `tag equality and case insensitive text candidates use existing indexes with generic plans`() = coRun {
        val table = "index_plan_${UUID.randomUUID().toString().replace("-", "") }"
        db.createDocStoreTable(table)
        try {
            db.withConnection { connection -> connection.createStatement().use { statement ->
                statement.executeUpdate("""
                    INSERT INTO $table (id, json, tags, text)
                    SELECT n::text,
                        jsonb_build_object('id', CASE WHEN n=12345 THEN 'TargetCode' ELSE md5(n::text) END,
                            'assetInformation', jsonb_build_object('specificIds', jsonb_build_array(jsonb_build_object(
                                'id', CASE WHEN n=12345 THEN 'TargetCode' ELSE md5(n::text) END))))::text,
                        ARRAY['ExternalId:' || CASE WHEN n=12345 THEN 'TargetCode' ELSE md5(n::text) END],
                        'ExternalId:' || CASE WHEN n=12345 THEN 'TargetCode' ELSE md5(n::text) END || E'\\n' ||
                        'id:' || CASE WHEN n=12345 THEN 'TargetCode' ELSE md5(n::text) END || E'\\n' ||
                        'SpecificId:' || CASE WHEN n=12345 THEN 'TargetCode' ELSE md5(n::text) END
                    FROM generate_series(1,20000) n
                """.trimIndent())
                statement.execute("VACUUM ANALYZE $table")
            } }
            val store = DocStore(dataSource = db, tableName = table, serializationStrategy = JsonObject.serializer())
            val exact = stringsMatch(DocStringSource.ExtractedTags, listOf("ExternalId:TargetCode"))
            val folded = allOf(
                stringsMatch(DocStringSource.ExtractedText, listOf("*externalid:targetcode*"), DocStringMatch.Wildcard, ignoreCase = true),
                stringsMatch(DocStringSource.ExtractedTags, listOf("externalid:targetcode"), ignoreCase = true),
            )
            val mixedCode = anyOf(folded,
                allOf(stringsMatch(DocStringSource.ExtractedText, listOf("*id:targetcode*"), DocStringMatch.Wildcard, ignoreCase = true),
                    stringsMatch(DocStringSource.Document(listOf("id")), listOf("targetcode"), ignoreCase = true)),
                allOf(stringsMatch(DocStringSource.ExtractedText, listOf("*SpecificId:targetcode*"), DocStringMatch.Wildcard, ignoreCase = true),
                    stringsMatch(DocStringSource.Document(listOf("assetInformation", "specificIds", "*", "id")), listOf("targetcode"), ignoreCase = true)),
            )
            assertEquals(1L, store.count(mixedCode))
            assertEquals(1L, store.count(exact))
            assertEquals(1L, store.count(folded))
            for ((filter, expectedIndex) in listOf(exact to "idx_${table}_tags", folded to "idx_${table}_text", mixedCode to "idx_${table}_text")) {
                val parameters = mutableListOf<Any>()
                val predicate = filter.toSql(parameters)
                val plan = db.withConnection { connection ->
                    connection.autoCommit = false
                    try {
                        connection.createStatement().use { it.execute("SET LOCAL plan_cache_mode = force_generic_plan") }
                        val name = "${table}_query"
                        var parameter = 0
                        val prepared = predicate.replace(Regex.escape("?").toRegex()) { "\u0024${++parameter}" }
                        val types = parameters.joinToString { if (it is List<*>) "text[]" else "text" }
                        fun literal(value: String) = "E'" + value.replace("\\", "\\\\").replace("'", "''") + "'"
                        val arguments = parameters.joinToString { value ->
                            if (value is List<*>) value.joinToString(",", "ARRAY[", "]::text[]") { literal(it.toString()) }
                            else literal(value.toString())
                        }
                        connection.createStatement().use { statement ->
                            statement.execute("PREPARE $name ($types) AS SELECT count(*) FROM $table WHERE $prepared")
                            try {
                                statement.executeQuery("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE $name ($arguments)").use { rows ->
                                    rows.next(); rows.getString(1)
                                }
                            } finally { statement.execute("DEALLOCATE $name") }
                        }
                    } finally { connection.rollback() }
                }
                assertTrue(plan.contains(expectedIndex), plan)
                assertTrue(plan.contains("\u00241"), "Expected a generic plan with parameter references: $plan")
                println("Existing-index plan ($expectedIndex): $plan")
            }
        } finally { db.dropDocStoreTable(table) }
    }
}
