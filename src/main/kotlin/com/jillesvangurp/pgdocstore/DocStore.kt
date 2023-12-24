@file:Suppress("unused", "MemberVisibilityCanBePrivate")

package com.jillesvangurp.pgdocstore

import com.github.jasync.sql.db.QueryResult
import com.github.jasync.sql.db.RowData
import com.github.jasync.sql.db.SuspendingConnection
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.map
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import kotlinx.datetime.toKotlinInstant
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import java.time.OffsetDateTime
import kotlin.random.Random
import kotlin.random.nextULong
import kotlin.reflect.KProperty
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

data class DocStoreEntry(
    val id: String,
    val createdAt: Instant,
    val updatedAt: Instant,
    val json: String,
    val tags: List<String>?,
    val text: String?
)

fun String.camelCase2SnakeCase(): String {
    val re = "(?<=[a-z0-9])[A-Z]".toRegex()
    return re.replace(this) { m -> "_${m.value}" }.lowercase()
}

inline fun <reified T> DocStoreEntry.document(json: Json = DEFAULT_JSON): T = json.decodeFromString<T>(this.json)
fun <T> DocStoreEntry.document(serializationStrategy: KSerializer<T>, json: Json = DEFAULT_JSON): T =
    json.decodeFromString(serializationStrategy, this.json)

val RowData.docStoreEntry
    get() = DocStoreEntry(
        id = getString(DocStoreEntry::id.name) ?: error("column not found"),
        createdAt = get(DocStoreEntry::createdAt.name.camelCase2SnakeCase())?.let {
            it as OffsetDateTime
            it.toInstant().toKotlinInstant()
        } ?: error("column not found"),
        updatedAt = get(DocStoreEntry::updatedAt.name.camelCase2SnakeCase())?.let {
            it as OffsetDateTime
            it.toInstant().toKotlinInstant()
        } ?: error("column not found"),
        json = getString(DocStoreEntry::json.name) ?: "column not found",
        tags = get(DocStoreEntry::tags.name)?.let {
            @Suppress("UNCHECKED_CAST")
            it as List<String>
        },
        text = getString(DocStoreEntry::text.name),
    )

class DocStore<T : Any>(
    // not private so people can have their own extension functions using this
    val connection: SuspendingConnection,
    private val serializationStrategy: KSerializer<T>,
    private val tableName: String,
    private val json: Json = DEFAULT_JSON,
    private val tagExtractor: (T) -> List<String>? = { null },
    private val textExtractor: (T) -> String? = { null },
    private val idExtractor: (T) -> String = { d ->
        val p = d::class.members.find { it.name == "id" } as KProperty<*>?
        p?.getter?.call(d)?.toString() ?: error("property id not found")
    },

    ) {

    /**
     * If you want to do multiple docstore operations in a transaction, use transact. Note most DocStore operations
     * do not have their own transaction (except scrolling queries).
     *
     * It creates a new DocStore with a connection from jasync's inTransaction function that
     * does the appropriate things to ensure transactions don't overlap.
     *
     * Note, jasync and transactions are tricky because you cannot assume the connection from a thread pool is not
     * shared between threads. That's why inTransaction gives you a new connection that isn't shared for the duration
     * of the transaction. This function in turn uses that to give you a DocStore instance that uses that connection.
     */
    suspend fun <R> transact(block: suspend (DocStore<T>) -> R): R {
        return connection.inTransaction { c ->
            block.invoke(DocStore(c, serializationStrategy, tableName, json, tagExtractor, textExtractor, idExtractor))
        }
    }

    suspend fun create(
        doc: T,
        timestamp: Instant = Clock.System.now()
    ) {
        create(idExtractor.invoke(doc), doc, timestamp)
    }

    suspend fun create(id: String, doc: T, timestamp: Instant = Clock.System.now()) {
        val txt = json.encodeToString(serializationStrategy, doc)
        val tags = tagExtractor.invoke(doc)
        val text = textExtractor.invoke(doc)

        connection.sendPreparedStatement(
            """
                INSERT INTO $tableName (id, created_at, updated_at, json, tags, text)
                VALUES (?,?,?,?,?,to_tsvector(coalesce(?)))
            """.trimIndent(), listOf(id, timestamp, timestamp, txt, tags, text)
        )

    }

    suspend fun getById(id: String): T? {
        return connection.sendPreparedStatement(
            """
            select ${DocStoreEntry::json.name} from $tableName where id = ?
        """.trimIndent(), listOf(id)
        ).let {
            if (it.rows.isNotEmpty()) {
                it.rows.first().getString(DocStoreEntry::json.name)?.let { str ->
                    json.decodeFromString(serializationStrategy, str)
                }
            } else {
                null
            }
        }
    }

    suspend fun getEntryById(id: String): DocStoreEntry? {
        return connection.sendPreparedStatement(
            """
            select * from $tableName where id = ?
        """.trimIndent(), listOf(id)
        ).let {
            if (it.rows.isNotEmpty()) {
                if (it.rows.size > 1) error("duplicate rows for $id")
                it.rows.first().docStoreEntry
            } else {
                null
            }
        }
    }

    suspend fun update(
        doc: T,
        timestamp: Instant = Clock.System.now(),
        updateFunction: (T) -> T
    ) = update(idExtractor.invoke(doc), timestamp, updateFunction)


    suspend fun update(
        id: String,
        timestamp: Instant = Clock.System.now(),
        updateFunction: (T) -> T
    ): T {
        val newValue = connection.sendPreparedStatement(
            """
                select json from $tableName where id = ?
            """.trimIndent(), listOf(id)
        ).let {
            if (it.rows.isNotEmpty()) {
                val firstRow = it.rows.first()

                firstRow.getString(DocStoreEntry::json.name)?.let { str ->
                    json.decodeFromString(serializationStrategy, str).let { original ->
                        val updated = updateFunction.invoke(original)
                        val tags = tagExtractor.invoke(updated)
                        val text = textExtractor.invoke(updated)
                        val txt = json.encodeToString(serializationStrategy, updated)
                        connection.sendPreparedStatement(
                            """
                                UPDATE $tableName
                                SET 
                                    updated_at = ?,
                                    json= ?,
                                    tags = ?,
                                    text = to_tsvector(coalesce(?))
                                WHERE id = ?    
                            """.trimIndent(), listOf(timestamp, txt, tags, text, id)
                        )
                        updated
                    }
                } ?: error("row has no json")
            } else {
                // FIXME exception handling
                error("not found")
            }
        }

        return newValue
    }

    suspend fun delete(
        doc: T,
    ) = delete(idExtractor.invoke(doc))


    suspend fun delete(id: String) {
        connection.sendPreparedStatement(
            """DELETE FROM $tableName WHERE id = ?""", listOf(id)
        )
    }

    suspend fun bulkInsert(
        flow: Flow<T>,
        chunkSize: Int = 100, delayMillis: Duration = 1.seconds
    ) {
        bulkInsertWithId(
            flow = flow.map { d ->
                idExtractor.invoke(d) to d
            },
            chunkSize = chunkSize,
            delayMillis = delayMillis
        )
    }

    suspend fun bulkInsert(
        sequence: Sequence<T>,
        chunkSize: Int = 100
    ) {
        bulkInsertWithId(
            sequence = sequence.map { d ->
                idExtractor.invoke(d) to d
            },
            chunkSize = chunkSize
        )
    }

    suspend fun bulkInsert(
        iterable: Iterable<T>,
        chunkSize: Int = 100
    ) {
        bulkInsertWithId(
            iterable = iterable.map { d ->
                idExtractor.invoke(d) to d
            },
            chunkSize = chunkSize
        )
    }

    suspend fun bulkInsertWithId(flow: Flow<Pair<String, T>>, chunkSize: Int = 100, delayMillis: Duration = 1.seconds) {
        flow.chunked(chunkSize = chunkSize, delayMillis = delayMillis).collect { chunk ->
            insertList(chunk)
        }
    }

    suspend fun bulkInsertWithId(iterable: Iterable<Pair<String, T>>, chunkSize: Int = 100) {
        bulkInsertWithId(iterable.asSequence(), chunkSize)
    }

    suspend fun bulkInsertWithId(sequence: Sequence<Pair<String, T>>, chunkSize: Int = 100) {
        sequence.chunked(chunkSize).forEach { chunk ->
            insertList(chunk)
        }
    }

    suspend fun insertList(chunk: List<Pair<String, T>>) {
        // Base SQL for INSERT
        val baseSql = """
                    INSERT INTO $tableName (id, json, tags, created_at, updated_at, text)
                    VALUES 
                """.trimIndent()

        // Constructing the values part of the SQL
        val values = chunk.joinToString(", ") { " (?, ?, ?, ?, ?, to_tsvector(coalesce(?)))" } + " "
        val conflictAction = """
                    ON CONFLICT (id) DO UPDATE SET
                    json = EXCLUDED.json,
                    tags = EXCLUDED.tags,                   
                    text = EXCLUDED.text,                   
                    updated_at = EXCLUDED.updated_at
                """.trimIndent()

        val sql = "$baseSql $values $conflictAction"

        // Flatten the list of lists into a single list of parameters
        val params = chunk.map { (id, doc) ->
            val now = Clock.System.now()
            val tags = tagExtractor.invoke(doc)
            val text = textExtractor.invoke(doc)
            listOf(id, json.encodeToString(serializationStrategy, doc), tags, now, now, text)
        }.flatten()

        // Create and execute the PreparedStatement

        try {
            connection.sendPreparedStatement(sql, params)
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    suspend fun count(): Long {
        return connection.sendQuery("SELECT count(id) FROM $tableName").let {
            it.rows.first()[0]!! as Long
        }
    }

    suspend fun documentsByRecency(
        tags: List<String> = emptyList(),
        orTags: Boolean = false,
        query: String? = null,
        limit: Int = 100,

        ): List<T> {
        val q = constructQuery(tags, query, orTags, limit)
        return connection.sendPreparedStatement(q, tags + listOfNotNull(query)).let { result ->
            result.rows.map { row ->
                val js = row.getString(DocStoreEntry::json.name) ?: error("no json")
                json.decodeFromString(serializationStrategy, js)
            }
        }
    }

    suspend fun entriesByRecency(
        tags: List<String> = emptyList(),
        orTags: Boolean = false,
        query: String? = null,
        limit: Int = 100,

        ): List<DocStoreEntry> {
        val q = constructQuery(tags, query, orTags, limit)
        return connection.sendPreparedStatement(q, tags + listOfNotNull(query)).let { result ->
            result.rows.map { row ->
                row.docStoreEntry
            }
        }
    }

    suspend fun entriesByRecencyScrolling(
        tags: List<String> = emptyList(),
        orTags: Boolean = false,
        query: String? = null,
    ): Flow<DocStoreEntry> {
        val q = constructQuery(tags, query, orTags)
        return queryFlow(q, tags + listOfNotNull(query)) { row ->
            row.docStoreEntry
        }
    }

    suspend fun documentsByRecencyScrolling(
        tags: List<String> = emptyList(),
        orTags: Boolean = false,
        query: String? = null,
    ): Flow<T> {
        val q = constructQuery(tags, query, orTags)
        return queryFlow(q, tags + listOfNotNull(query)) { row ->
            json.decodeFromString(
                serializationStrategy,
                row.getString(DocStoreEntry::json.name) ?: error("empty json")
            )
        }
    }

    private fun constructQuery(
        tags: List<String>,
        query: String?,
        orTags: Boolean,
        limit: Int? = null,
    ): String {
        val whereClause = if (tags.isEmpty() && query.isNullOrBlank()) {
            ""
        } else {
            "WHERE " + listOfNotNull(
                tags.takeIf { it.isNotEmpty() }
                    ?.let { tags.joinToString(if (orTags) " OR " else " AND ") { "? = ANY(tags)" } },
                query?.takeIf { it.isNotBlank() }?.let {
                    """text @@ websearch_to_tsquery('english',?)"""
                }
            ).joinToString(" AND ")

        }

        val limitClause = if (limit != null) " LIMIT $limit" else ""

        return "SELECT * FROM $tableName $whereClause ORDER BY updated_at DESC$limitClause"
    }

    private suspend fun <R> queryFlow(
        query: String,
        params: List<Any>,
        fetchSize: Int = 100,
        rowProcessor: (RowData) -> R
    ): Flow<R> {
        // use channelFlow for thread safety
        return channelFlow {
            val producer = this
            val cursorId = "cursor_${Random.nextULong()}"
            connection.inTransaction { c ->
                try {
                    c.sendPreparedStatement(
                        """
                            DECLARE $cursorId CURSOR FOR 
                            $query
                        """.trimIndent(), params
                    )

                    var resp: QueryResult? = null
                    while (resp == null || resp.rows.isNotEmpty()) {
                        resp = c.sendQuery("FETCH $fetchSize FROM $cursorId;")
                        resp.rows.forEach { row ->
                            rowProcessor.invoke(row)

                            producer.send(rowProcessor.invoke(row))
                        }
                    }
                } finally {
                    c.sendQuery("CLOSE $cursorId;")
                }
            }
        }
    }
}

