@file:Suppress("unused", "MemberVisibilityCanBePrivate")

package com.tryformation.pgdocstore

import com.github.jasync.sql.db.QueryResult
import com.github.jasync.sql.db.RowData
import com.github.jasync.sql.db.SuspendingConnection
import io.github.oshai.kotlinlogging.KotlinLogging
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

private val logger = KotlinLogging.logger {  }

data class DocStoreEntry(
    val id: String,
    val createdAt: Instant,
    val updatedAt: Instant,
    val json: String,
    val tags: List<String>?,
    val text: String?,
    val similarity: Float? = null,
)

enum class BooleanOperator {
    OR,
    AND
}

fun String.sanitizeInputForDB(): String {
    // Define a regular expression for disallowed characters or strings
    // For example, this regex will remove single quotes, double quotes, semicolons, and SQL comment syntax
    val disallowedPattern = """['";]|(--)|(/\*)|(\*/)|(#\s)""".toRegex()

    // Replace disallowed characters with a space
    return disallowedPattern.replace(this, " ")
}

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
        // only there on searches with a text
        similarity = this.size.takeIf { it > 6 }?.let { getFloat("rank") },
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
    private val logging: Boolean = false,
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
        if(logging) logger.info { "$tableName start transaction" }
        return connection.inTransaction { c ->
            block.invoke(DocStore(c, serializationStrategy, tableName, json, tagExtractor, textExtractor, idExtractor))
        }.also {
            if(logging) logger.info { "$tableName returning from transaction" }
        }
    }

    /**
     * Create a document. The [idExtractor] will be used to extract an id from the document.
     *
     * Optionally, you can override the [timestamp] and specify that the document should be
     * overwritten if it already exists with [onConflictUpdate].
     *
     * Returns the created [DocStoreEntry]
     */
    suspend fun create(
        doc: T,
        timestamp: Instant = Clock.System.now(),
        onConflictUpdate: Boolean = false
    ): DocStoreEntry {
        return create(idExtractor.invoke(doc), doc, timestamp, onConflictUpdate)
    }

    /**
     * Create a documen and use the specified [id] as the id.
     *
     * Optionally, you can override the [timestamp] and specify that the document should be
     * overwritten if it already exists with [onConflictUpdate]
     *
     * Returns the created [DocStoreEntry]
     *
     */
    suspend fun create(
        id: String,
        doc: T,
        timestamp: Instant = Clock.System.now(),
        onConflictUpdate: Boolean = false
    ): DocStoreEntry {
        val txt = json.encodeToString(serializationStrategy, doc)
        val tags = tagExtractor.invoke(doc)
        val text = textExtractor.invoke(doc)
        if(logging) logger.info { "$tableName creating $id" }

        connection.sendPreparedStatement(
            """
                INSERT INTO $tableName (id, created_at, updated_at, json, tags, text)
                VALUES (?,?,?,?,?,?)
                ${
                if (onConflictUpdate) """
                        ON CONFLICT (id) DO UPDATE SET
                            json = EXCLUDED.json,
                            tags = EXCLUDED.tags,                   
                            text = EXCLUDED.text,                                     
                            updated_at = EXCLUDED.updated_at
                    """.trimIndent()
                else ""
            }
            """.trimIndent(), listOf(id, timestamp, timestamp, txt, tags, text)
        )

        return DocStoreEntry(id, timestamp, timestamp, txt, tags, text).also {
            if(logging) logger.info { "$tableName created $it" }
        }
    }

    /**
     * Retrieve a document by [id].
     *
     * Returns the deserialized document or null if it doesn't exist.
     */
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
        }.also {
            if(logging) logger.info { "$tableName get $id: $it" }
        }
    }

    /**
     * Retrieve multiple documents by their [ids].
     */
    suspend fun multiGetById(ids: List<String>): List<T> {
        return if (ids.isEmpty()) {
            emptyList()
        } else {
            connection.sendPreparedStatement(
                // little hack with question marks because jasync doesn't handle lists in prepared statements
                query = """
                            select ${DocStoreEntry::json.name} from $tableName where id in (${ids.joinToString(",") { "?" }})
                        """.trimIndent(),
                values = ids,
                // release because number of question marks may vary ...
                release = true
            ).rows.mapNotNull { row ->
                row.getString(DocStoreEntry::json.name)?.let { str ->
                    json.decodeFromString(serializationStrategy, str)
                }
            }
        }.also {
            if(logging) logger.info { "$tableName get $ids: returning ${it.size}" }
        }
    }

    /**
     * Retrieve a [DocStoreEntry] by its [id]. Returns null if there is no matching entry.
     */
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
        }.also {
            if(logging) logger.info { "$tableName get entry $id: returning ${it}" }
        }
    }

    /**
     * Retrieve multiple [DocStoreEntry] by their [ids]
     */
    suspend fun multiGetEntryById(ids: List<String>): List<DocStoreEntry> {
        return connection.sendPreparedStatement(
            // little hack with question marks because jasync doesn't handle lists in prepared statements
            query = """
                        select * from $tableName where id in (${ids.joinToString(",") { "?" }})
                    """.trimIndent(),
            values = ids,
            // release because number of question marks may vary ...
            release = true
        ).let { r ->
            r.rows.map {
                it.docStoreEntry
            }
        }.also {
            if(logging) logger.info { "$tableName get entries $ids: returning ${it.size}" }
        }
    }

    /**
     * Update a document. The document will be retrieved from the database and then the [updateFunction]
     * is applied to it to modify it.
     *
     * You can optionally specify a different [timestamp] than now.
     *
     * Note,
     */
    suspend fun update(
        doc: T,
        timestamp: Instant = Clock.System.now(),
        updateFunction: suspend (T) -> T
    ) = update(idExtractor.invoke(doc), timestamp, updateFunction)


    suspend fun update(
        id: String,
        timestamp: Instant = Clock.System.now(),
        updateFunction: suspend (T) -> T
    ): T {
        logger.info { "$tableName start update transaction ${connection.hashCode()}" }
        return connection.inTransaction { tsc ->
            logger.info { "$tableName in update transaction ${connection.hashCode()}" }

            tsc.sendPreparedStatement(
                """
                    select json from $tableName where id = ?
                """.trimIndent(), listOf(id)
            ).let {
                if (it.rows.isNotEmpty()) {
                    val firstRow = it.rows.first()

                    firstRow.getString(DocStoreEntry::json.name)?.let { str ->
                        json.decodeFromString(serializationStrategy, str).let { original ->
                            val updated = updateFunction.invoke(original)
                            // FIXME co-routine madness; some lambdas can hang netty here
                            val tags = tagExtractor(updated)
                            val text = textExtractor(updated)

                            val txt = json.encodeToString(serializationStrategy, updated)
                            tsc.sendPreparedStatement(
                                """
                                    UPDATE $tableName
                                    SET 
                                        updated_at = ?,
                                        json= ?,
                                        tags = ?,
                                        text = ?
                                    WHERE id = ?    
                                """.trimIndent(), listOf(timestamp, txt, tags, text, id)
                            )
                            updated
                        }
                    } ?: error("row has no json")
                } else {
                    logger.info { "$tableName document not found $id" }
                    throw DocumentNotFoundException(id)
                }
            }
        }.also {
            if(logging) logger.info { "$tableName updated $id: returning ${it}" }
        }
    }

    /**
     * Deletes a [document].
     */
    suspend fun delete(
        document: T,
    ) = delete(idExtractor.invoke(document))

    /**
     * Delete a document by its [id].
     */
    suspend fun delete(id: String) {
        connection.sendPreparedStatement(
            """DELETE FROM $tableName WHERE id = ?""", listOf(id)
        ).also {
            if(logging) logger.info { "$tableName deleted $id" }
        }
    }

    /**
     * Bulk insert documents from a [flow].
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Note, unless you surround this with transact,
     * no transaction is used.
     *
     * Useful in combination with [documentsByRecencyScrolling], which returns a flow of documents.
     */
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

    /**
     * Bulk insert a [sequence].
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Note, unless you surround this with transact,
     * no transaction is used.
     */
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

    /**
     * Bulk insert any [iterable] (List, Set, etc.).
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Note, unless you surround this with transact,
     * no transaction is used.
     */
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

    /**
     * Bulk insert documents from a [flow].
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Note, unless you surround this with transact,
     * no transaction is used.
     *
     * Useful in combination with [documentsByRecencyScrolling], which returns a flow of documents.
     */
    suspend fun bulkInsertWithId(flow: Flow<Pair<String, T>>, chunkSize: Int = 100, delayMillis: Duration = 1.seconds) {
        if(logging) logger.info { "$tableName done bulk inserting" }

        flow.chunked(chunkSize = chunkSize, delayMillis = delayMillis).collect { chunk ->
            insertList(chunk)
        }
        if(logging) logger.info { "$tableName done bulk inserting" }

    }

    /**
     * Bulk insert documents from an [iterable].
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Note, unless you surround this with transact,
     * no transaction is used.
     */
    suspend fun bulkInsertWithId(iterable: Iterable<Pair<String, T>>, chunkSize: Int = 100) {
        bulkInsertWithId(iterable.asSequence(), chunkSize)
    }

    /**
     * Bulk insert documents from a [sequence].
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Note, unless you surround this with transact,
     * no transaction is used.
     */
    suspend fun bulkInsertWithId(sequence: Sequence<Pair<String, T>>, chunkSize: Int = 100) {
        sequence.chunked(chunkSize).forEach { chunk ->
            insertList(chunk)
        }
    }

    /**
     * Insert a list of id, document pairs. Used by the various bulk inserts to insert documents.
     *
     * This falls back to overwriting the document with ON CONFLICT (id) DO UPDATE in case
     * the document already exists.
     */
    suspend fun insertList(chunk: List<Pair<String, T>>, timestamp: Instant = Clock.System.now()) {
        // Base SQL for INSERT
        val baseSql = """
                    INSERT INTO $tableName (id, json, tags, created_at, updated_at, text)
                    VALUES 
                """.trimIndent()

        // Constructing the values part of the SQL
        val values = chunk.joinToString(", ") { " (?, ?, ?, ?, ?, ?)" } + " "
        val conflictAction = """
                    ON CONFLICT (id) DO UPDATE SET
                    json = EXCLUDED.json,
                    tags = EXCLUDED.tags,                   
                    text = EXCLUDED.text,                                     
                    updated_at = EXCLUDED.created_at
                """.trimIndent() // uses rejected created_at as the update timestamp

        val sql = "$baseSql $values $conflictAction"

        // Flatten the list of lists into a single list of parameters
        val params = chunk.map { (id, doc) ->
            val now = timestamp
            val tags = tagExtractor.invoke(doc)
            val text = textExtractor.invoke(doc)
            listOf(id, json.encodeToString(serializationStrategy, doc), tags, now, now, text)
        }.flatten()

        // Create and execute the PreparedStatement

        connection.sendPreparedStatement(sql, params).also {
            if(logging) logger.info { "$tableName inserted list of ${chunk.size}" }
        }
    }

    suspend fun count(): Long {
        return connection.sendQuery("SELECT count(id) FROM $tableName").let {
            it.rows.first()[0]!! as Long
        }
    }

    /**
     * Returns a list of documents ordered by recency. This fetches a maximum of [limit] document
     * from the specified [offset]
     *
     * You can optionally constrain
     * the query with [tags]. If you set [whereClauseOperator] to OR,
     * a logical OR will be used instead of the default AND.
     *
     * You can also specify a text [query]. In that case the results will be ordered by
     * their ranking. You can use [similarityThreshold] to control how strict it matches.
     */
    suspend fun documentsByRecency(
        tags: List<String> = emptyList(),
        query: String? = null,
        tagsClauseOperator: BooleanOperator = BooleanOperator.AND,
        whereClauseOperator: BooleanOperator = BooleanOperator.AND,
        limit: Int = 100,
        offset: Int = 0,
        similarityThreshold: Double = 0.1,
    ): List<T> {
        val q = constructQuery(
            tags = tags,
            query = query,
            tagsClauseOperator = tagsClauseOperator,
            limit = limit,
            offset = offset,
            whereClauseOperator = whereClauseOperator,
            similarityThreshold = similarityThreshold
        )
        return connection.sendPreparedStatement(q, tags + listOfNotNull(query)).let { result ->
            result.rows.map { row ->
                val js = row.getString(DocStoreEntry::json.name) ?: error("no json")
                json.decodeFromString(serializationStrategy, js)
            }
        }
    }

    /**
     * Similar to [documentsByRecency] but returns a list of the [DocStoreEntry] with the full row data.
     */
    suspend fun entriesByRecency(
        tags: List<String> = emptyList(),
        query: String? = null,
        tagsClauseOperator: BooleanOperator = BooleanOperator.AND,
        whereClauseOperator: BooleanOperator = BooleanOperator.AND,
        limit: Int = 100,
        offset: Int = 0,
        similarityThreshold: Double = 0.1,
    ): List<DocStoreEntry> {
        val q = constructQuery(
            tags = tags,
            query = query,
            tagsClauseOperator = tagsClauseOperator,
            limit = limit,
            offset = offset,
            whereClauseOperator = whereClauseOperator,
            similarityThreshold = similarityThreshold
        )
        return connection.sendPreparedStatement(q, tags + listOfNotNull(query)).let { result ->
            result.rows.map { row ->
                row.docStoreEntry
            }
        }
    }


    /**
     * Returns a flow of documents ordered by recency. This will scroll through the results
     * and fetch pages of results [fetchSize] documents at the time. Each document is emitted
     * to the flow as it comes in.
     *
     * You can use this to efficiently process all documents in your store.
     *
     * You can optionally constrain
     * the query with [tags]. If you set [whereClauseOperator] to OR,
     * a logical OR will be used instead of the default AND.
     *
     * You can also specify a text [query]. In that case the results will be ordered by
     * their ranking. You can use [similarityThreshold] to control how strict it matches.
     */
    suspend fun documentsByRecencyScrolling(
        tags: List<String> = emptyList(),
        query: String? = null,
        tagsClauseOperator: BooleanOperator = BooleanOperator.AND,
        whereClauseOperator: BooleanOperator = BooleanOperator.AND,
        similarityThreshold: Double = 0.1,
        fetchSize: Int = 100,
    ): Flow<T> {
        val q = constructQuery(
            tags = tags,
            query = query,
            tagsClauseOperator = tagsClauseOperator,
            whereClauseOperator = whereClauseOperator,
            similarityThreshold = similarityThreshold
        )
        return queryFlow(
            query = q,
            // query is used in select and then once more in the where
            params = tags + listOfNotNull(query),
            fetchSize = fetchSize
        ) { row ->
            json.decodeFromString(
                serializationStrategy,
                row.getString(DocStoreEntry::json.name) ?: error("empty json")
            )
        }
    }

    /**
     * Similar to [documentsByRecencyScrolling] but returns a flow of the [DocStoreEntry] with the full row data.
     */
    suspend fun entriesByRecencyScrolling(
        tags: List<String> = emptyList(),
        query: String? = null,
        tagsClauseOperator: BooleanOperator = BooleanOperator.AND,
        whereClauseOperator: BooleanOperator = BooleanOperator.AND,
        fetchSize: Int = 100,
        similarityThreshold: Double = 0.1,
    ): Flow<DocStoreEntry> {
        return queryFlow(
            query = constructQuery(
                tags = tags,
                query = query,
                tagsClauseOperator = tagsClauseOperator,
                whereClauseOperator = whereClauseOperator,
                similarityThreshold = similarityThreshold
            ),
            // query is used in select and then once more in the where
            params = tags + listOfNotNull(query),
            fetchSize = fetchSize
        ) { row ->
            row.docStoreEntry
        }
    }

    private fun constructQuery(
        tags: List<String>,
        query: String?,
        tagsClauseOperator: BooleanOperator = BooleanOperator.AND,
        whereClauseOperator: BooleanOperator = BooleanOperator.AND,
        limit: Int? = null,
        offset: Int = 0,
        similarityThreshold: Double = 0.01
    ): String {
        val rankSelect = if (!query.isNullOrBlank()) {
            // prepared statement does not work for this
            ", similarity(text, '${query.sanitizeInputForDB()}') AS rank"
        } else {
            ""
        }
        val whereClause = if (tags.isEmpty() && query.isNullOrBlank()) {
            ""
        } else {
            "WHERE " + listOfNotNull(
                tags.takeIf { it.isNotEmpty() }
                    ?.let {
                        tags.joinToString(
                            " $tagsClauseOperator "
                        ) { "? = ANY(tags)" }
                    }
                    ?.let {
                        // surround with parentheses
                        "($it)"
                    },
                query?.takeIf { q -> q.isNotBlank() }?.let {
                    """similarity(text, ?) > $similarityThreshold"""
                }
            ).joinToString(" $whereClauseOperator ")

        }

        val limitClause = if (limit != null) {
            " LIMIT $limit" + if (offset > 0) " OFFSET $offset" else ""
        } else ""

        val orderByClause = if (query.isNullOrBlank()) {
            "ORDER BY updated_at DESC"
        } else {
            "ORDER BY rank DESC, updated_at DESC"
        }
        return "SELECT *$rankSelect FROM $tableName $whereClause $orderByClause$limitClause"
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

    /**
     * Updates all documents in the store and re-runs all the extract functions.
     *
     * This is useful when you change the way those functions work.
     *
     * Note, this may take a while to run on a big database.
     * Also, while documentsByRecencyScrolling necessarily has its
     * own transaction (scrolling has to be done in a transaction), bulkInsert does not.
     * You can wrap the bulkInsert with a separate transaction but it will be a separate one.
     */
    suspend fun reExtract() {
        bulkInsert(documentsByRecencyScrolling())
    }
}

