package com.tryformation.pgdocstore

import com.jillesvangurp.serializationext.DEFAULT_JSON

import java.sql.Connection
import java.sql.Timestamp
import javax.sql.DataSource
import kotlin.reflect.KProperty
import kotlin.time.Duration
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withContext
import kotlinx.datetime.Instant
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import mu.KotlinLogging


private val logger = KotlinLogging.logger { }

fun Instant.toSqlTimestamp(): Timestamp =
    Timestamp.from(java.time.Instant.ofEpochMilli(this.toEpochMilliseconds()))

class DocStore<T : Any>(
    private val dataSource: DataSource,
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
    /** Not null if in a transaction.  */
    private val connection: Connection? = null,

    ) : IDocStore<T> {
    private suspend fun <T> withDocStoreConnection(block: suspend (Connection) -> T): T {
        return if (connection != null) {
            block(connection)
        } else {
            dataSource.withConnection(block)
        }
    }

    override suspend fun <R> transact(block: suspend (IDocStore<T>) -> R): R {
        // already in transaction
        return if (connection != null) block(this)
        // create a new transaction
        else withDocStoreConnection { conn ->
            try {
                conn.autoCommit = false
                val result = block(
                    // create a copy of the docstore with the connection
                    DocStore(
                        dataSource = dataSource,
                        serializationStrategy = serializationStrategy,
                        tableName = tableName,
                        json = json,
                        tagExtractor = tagExtractor,
                        textExtractor = textExtractor,
                        idExtractor = idExtractor,
                        logging = logging,
                        connection = conn
                    )
                )
                conn.commit()
                result
            } catch (e: Exception) {
                logger.error(e) { "Error during transaction in $tableName: ${e.message}" }
                conn.rollback()
                throw e
            }

        }
    }

    override suspend fun create(doc: T, timestamp: Instant, onConflictUpdate: Boolean): DocStoreEntry {
        return create(idExtractor.invoke(doc), doc, timestamp, onConflictUpdate)
    }

    override suspend fun create(id: String, doc: T, timestamp: Instant, onConflictUpdate: Boolean): DocStoreEntry {
        val serialized = json.encodeToString(serializationStrategy, doc)
        val tags = tagExtractor.invoke(doc)
        val text = textExtractor.invoke(doc)
        if (logging) logger.info { "$tableName creating $id" }

        return withDocStoreConnection { connection ->

            connection.prepareStatement(
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
                """.trimIndent()
            ).use { statement ->

                statement.setString(1, id)
                statement.setTimestamp(2, timestamp.toSqlTimestamp())
                statement.setTimestamp(3, timestamp.toSqlTimestamp())
                statement.setString(4, serialized)
                statement.setArray(5, connection.createArrayOf("text", tags?.toTypedArray()))
                statement.setString(6, text)

                statement.executeUpdate()
            }

            DocStoreEntry(id, timestamp, timestamp, serialized, tags, text).also {
                if (logging) logger.info { "$tableName created $it" }
            }
        }
    }

    override suspend fun getById(id: String): T? {
        return withDocStoreConnection { conn ->
            conn.prepareStatement(
                """
                    select ${DocStoreEntry::json.name} from $tableName where id = ?
                """.trimIndent()
            ).use { statement ->
                statement.setString(1, id)
                statement.executeQuery().use { resultSet ->
                    if (resultSet.next()) {
                        val jsonString = resultSet.getString(DocStoreEntry::json.name)
                        json.decodeFromString(serializationStrategy, jsonString)
                    } else {
                        null
                    }
                }

            }
        }
    }

    override suspend fun multiGetById(ids: List<String>): List<T> {
        if (ids.isEmpty()) return emptyList()

        return withDocStoreConnection { connection ->
            val placeholders = ids.joinToString(",") { "?" }
            val sql = """
                SELECT ${DocStoreEntry::json.name} 
                FROM $tableName 
                WHERE id IN ($placeholders)
            """.trimIndent()

            connection.prepareStatement(sql).use { statement ->
                ids.forEachIndexed { index, id ->
                    statement.setString(index + 1, id)
                }

                statement.executeQuery().use { resultSet ->
                    val results = mutableListOf<T>()
                    while (resultSet.next()) {
                        val jsonStr = resultSet.getString(DocStoreEntry::json.name)
                        val decoded = json.decodeFromString(serializationStrategy, jsonStr)
                        results.add(decoded)
                    }
                    if (logging) logger.info { "$tableName get $ids: returning ${results.size}" }
                    results
                }
            }
        }
    }

    override suspend fun getEntryById(id: String): DocStoreEntry? {
        return withDocStoreConnection { connection ->
            connection.prepareStatement(
                """
            SELECT id, created_at, updated_at, json, tags, text 
            FROM $tableName 
            WHERE id = ?
            """.trimIndent()
            ).use { statement ->
                statement.setString(1, id)
                statement.executeQuery().use { resultSet ->
                    if (resultSet.next()) {
                        val entry = DocStoreEntry(
                            id = resultSet.getString("id"),
                            createdAt = Instant.fromEpochMilliseconds(resultSet.getTimestamp("created_at").time),
                            updatedAt = Instant.fromEpochMilliseconds(resultSet.getTimestamp("updated_at").time),
                            json = resultSet.getString("json"),
                            tags = resultSet.getArray("tags")?.array?.let { it as Array<*> }?.map { it.toString() },
                            text = resultSet.getString("text")
                        )
                        if (logging) logger.info { "$tableName get entry $id: returning $entry" }
                        entry
                    } else {
                        if (logging) logger.info { "$tableName get entry $id: not found" }
                        null
                    }
                }
            }
        }
    }

    override suspend fun multiGetEntryById(ids: List<String>): List<DocStoreEntry> {
        if (ids.isEmpty()) return emptyList()

        return withDocStoreConnection { connection ->
            val placeholders = ids.joinToString(",") { "?" }
            val sql = """
            SELECT id, created_at, updated_at, json, tags, text 
            FROM $tableName 
            WHERE id IN ($placeholders)
        """.trimIndent()

            connection.prepareStatement(sql).use { statement ->
                ids.forEachIndexed { index, id ->
                    statement.setString(index + 1, id)
                }

                statement.executeQuery().use { resultSet ->
                    val results = mutableListOf<DocStoreEntry>()
                    while (resultSet.next()) {
                        results.add(
                            DocStoreEntry(
                                id = resultSet.getString("id"),
                                createdAt = Instant.fromEpochMilliseconds(resultSet.getTimestamp("created_at").time),
                                updatedAt = Instant.fromEpochMilliseconds(resultSet.getTimestamp("updated_at").time),
                                json = resultSet.getString("json"),
                                tags = resultSet.getArray("tags")?.array?.let { it as Array<*> }?.map { it.toString() },
                                text = resultSet.getString("text")
                            )
                        )
                    }
                    if (logging) logger.info { "$tableName get entries $ids: returning ${results.size}" }
                    results
                }
            }
        }
    }

    override suspend fun update(doc: T, timestamp: Instant, updateFunction: suspend (T) -> T): T =
        update(idExtractor.invoke(doc), timestamp, updateFunction)

    override suspend fun update(
        id: String,
        timestamp: Instant,
        updateFunction: suspend (T) -> T
    ): T {
        if (logging) logger.info { "$tableName start update transaction" }

        return dataSource.withTransaction { conn ->
            // Fetch the existing document
            val existing = conn.prepareStatement(
                "SELECT json FROM $tableName WHERE id = ?"
            ).use { stmt ->
                stmt.setString(1, id)
                stmt.executeQuery().use { rs ->
                    if (rs.next()) {
                        val jsonStr = rs.getString("json")
                        json.decodeFromString(serializationStrategy, jsonStr)
                    } else {
                        if (logging) logger.info { "$tableName document not found $id" }
                        throw DocumentNotFoundException(id)
                    }
                }
            }

            // Apply the update function
            val updated = updateFunction(existing)
            val updatedJson = json.encodeToString(serializationStrategy, updated)
            val tags = tagExtractor(updated)
            val text = textExtractor(updated)

            // Update the document
            conn.prepareStatement(
                """
                UPDATE $tableName
                SET 
                    updated_at = ?,
                    json = ?,
                    tags = ?,
                    text = ?
                WHERE id = ?
            """.trimIndent()
            ).use { stmt ->
                stmt.setTimestamp(1, timestamp.toSqlTimestamp())
                stmt.setString(2, updatedJson)
                stmt.setArray(3, conn.createArrayOf("text", tags?.toTypedArray()))
                stmt.setString(4, text)
                stmt.setString(5, id)
                stmt.executeUpdate()
            }

            if (logging) logger.info { "$tableName updated $id: returning $updated" }
            updated
        }
    }

    override suspend fun delete(document: T) {
        delete(idExtractor(document))
    }

    override suspend fun delete(id: String) {
        withDocStoreConnection { connection ->
            connection.prepareStatement(
                """
            DELETE FROM $tableName WHERE id = ?
            """.trimIndent()
            ).use { statement ->
                statement.setString(1, id)
                val deleted = statement.executeUpdate()
                if (logging) {
                    if (deleted > 0) logger.info { "$tableName delete $id: deleted" }
                    else logger.info { "$tableName delete $id: not found" }
                }
            }
        }
    }

    override suspend fun bulkInsert(
        flow: Flow<T>,
        chunkSize: Int, delayMillis: Duration
    ) {

        bulkInsertWithId(
            flow = flow.map { d ->
                idExtractor.invoke(d) to d
            },
            chunkSize = chunkSize,
            delayMillis = delayMillis
        )
    }

    override suspend fun bulkInsert(
        sequence: Sequence<T>,
        chunkSize: Int
    ) {
        bulkInsertWithId(
            sequence = sequence.map { d ->
                idExtractor.invoke(d) to d
            },
            chunkSize = chunkSize
        )
    }

    override suspend fun bulkInsert(
        iterable: Iterable<T>,
        chunkSize: Int
    ) {
        bulkInsertWithId(
            iterable = iterable.map { d ->
                idExtractor.invoke(d) to d
            },
            chunkSize = chunkSize
        )
    }

    override suspend fun bulkInsertWithId(flow: Flow<Pair<String, T>>, chunkSize: Int, delayMillis: Duration) {
        if (logging) logger.info { "$tableName done bulk inserting" }

        transact { tDocstore ->
            flow.chunked(chunkSize = chunkSize, delayMillis = delayMillis).collect { chunk ->
                tDocstore.insertList(chunk)
            }
        }
        if (logging) logger.info { "$tableName done bulk inserting" }

    }

    override suspend fun bulkInsertWithId(iterable: Iterable<Pair<String, T>>, chunkSize: Int) {
        bulkInsertWithId(iterable.asSequence(), chunkSize)
    }

    override suspend fun bulkInsertWithId(sequence: Sequence<Pair<String, T>>, chunkSize: Int) {
        transact { tDocstore ->
            sequence.chunked(chunkSize).forEach { chunk ->
                tDocstore.insertList(chunk)
            }
        }
    }

    override suspend fun insertList(chunk: List<Pair<String, T>>, timestamp: Instant) {
        if (chunk.isEmpty()) return

        withDocStoreConnection { connection ->
            val baseSql = """
            INSERT INTO $tableName (id, json, tags, created_at, updated_at, text)
            VALUES
        """.trimIndent()

            val valuesPart = chunk.joinToString(", ") { "(?, ?, ?, ?, ?, ?)" }
            val conflictAction = """
            ON CONFLICT (id) DO UPDATE SET
                json = EXCLUDED.json,
                tags = EXCLUDED.tags,
                text = EXCLUDED.text,
                updated_at = EXCLUDED.created_at
        """.trimIndent()

            val sql = "$baseSql $valuesPart $conflictAction"

            connection.prepareStatement(sql).use { statement ->
                var paramIndex = 1
                chunk.forEach { (id, doc) ->
                    val encoded = json.encodeToString(serializationStrategy, doc)
                    val tags = tagExtractor.invoke(doc)
                    val text = textExtractor.invoke(doc)
                    val ts = timestamp.toSqlTimestamp()

                    statement.setString(paramIndex++, id)
                    statement.setString(paramIndex++, encoded)
                    statement.setArray(paramIndex++, connection.createArrayOf("text", tags?.toTypedArray()))
                    statement.setTimestamp(paramIndex++, ts)
                    statement.setTimestamp(paramIndex++, ts)
                    statement.setString(paramIndex++, text)
                }

                statement.executeUpdate()
            }

            if (logging) logger.info { "$tableName inserted list of ${chunk.size}" }
        }
    }

    override suspend fun count(): Long {
        return withDocStoreConnection { connection ->
            connection.prepareStatement(
                """
            SELECT COUNT(*) FROM $tableName
            """.trimIndent()
            ).use { statement ->
                statement.executeQuery().use { resultSet ->
                    if (resultSet.next()) {
                        resultSet.getLong(1)
                    } else {
                        0L
                    }
                }
            }
        }
    }

    override suspend fun documentsByRecency(
        tags: List<String>,
        query: String?,
        tagsClauseOperator: BooleanOperator,
        whereClauseOperator: BooleanOperator,
        limit: Int,
        offset: Int,
        similarityThreshold: Double
    ): List<T> {
        val sql =
            constructQuery(tags, query, tagsClauseOperator, whereClauseOperator, limit, offset, similarityThreshold)
        return withDocStoreConnection { connection ->
            connection.prepareStatement(sql).use { statement ->
                setQueryParams(statement, tags, query)
                statement.executeQuery().use { rs ->
                    val result = mutableListOf<T>()
                    while (rs.next()) {
                        result += json.decodeFromString(serializationStrategy, rs.getString("json"))
                    }
                    result
                }
            }
        }
    }

    override suspend fun entriesByRecency(
        tags: List<String>,
        query: String?,
        tagsClauseOperator: BooleanOperator,
        whereClauseOperator: BooleanOperator,
        limit: Int,
        offset: Int,
        similarityThreshold: Double
    ): List<DocStoreEntry> {
        val sql =
            constructQuery(tags, query, tagsClauseOperator, whereClauseOperator, limit, offset, similarityThreshold)
        return withDocStoreConnection { connection ->
            connection.prepareStatement(sql).use { statement ->
                setQueryParams(statement, tags, query)
                statement.executeQuery().use { rs ->
                    val result = mutableListOf<DocStoreEntry>()
                    while (rs.next()) {
                        result += toEntry(rs)
                    }
                    result
                }
            }
        }
    }

    override suspend fun documentsByRecencyScrolling(
        tags: List<String>,
        query: String?,
        tagsClauseOperator: BooleanOperator,
        whereClauseOperator: BooleanOperator,
        similarityThreshold: Double,
        fetchSize: Int
    ): Flow<T> = queryFlow(
        constructQuery(tags, query, tagsClauseOperator, whereClauseOperator, null, 0, similarityThreshold),
        tags + listOfNotNull(query),
        fetchSize
    ) { rs ->
        json.decodeFromString(serializationStrategy, rs.getString("json"))
    }

    override suspend fun entriesByRecencyScrolling(
        tags: List<String>,
        query: String?,
        tagsClauseOperator: BooleanOperator,
        whereClauseOperator: BooleanOperator,
        fetchSize: Int,
        similarityThreshold: Double
    ): Flow<DocStoreEntry> = queryFlow(
        constructQuery(tags, query, tagsClauseOperator, whereClauseOperator, null, 0, similarityThreshold),
        tags + listOfNotNull(query),
        fetchSize
    ) { rs -> toEntry(rs) }

    private fun constructQuery(
        tags: List<String>,
        query: String?,
        tagsClauseOperator: BooleanOperator,
        whereClauseOperator: BooleanOperator,
        limit: Int?,
        offset: Int,
        similarityThreshold: Double
    ): String {
        val rankSelect = if (!query.isNullOrBlank()) {
            ", similarity(text, '${query.sanitizeInputForDB()}') AS rank"
        } else ""
        val whereClause = if (tags.isEmpty() && query.isNullOrBlank()) {
            ""
        } else {
            "WHERE " + listOfNotNull(
                tags.takeIf { it.isNotEmpty() }?.joinToString(" $tagsClauseOperator ") { "? = ANY(tags)" }
                    ?.let { "($it)" },
                query?.takeIf { it.isNotBlank() }?.let { "similarity(text, ?) > $similarityThreshold" }
            ).joinToString(" $whereClauseOperator ")
        }
        val limitClause = limit?.let { " LIMIT $it" + if (offset > 0) " OFFSET $offset" else "" } ?: ""
        val orderClause =
            if (query.isNullOrBlank()) "ORDER BY updated_at DESC" else "ORDER BY rank DESC, updated_at DESC"
        return "SELECT *$rankSelect FROM $tableName $whereClause $orderClause$limitClause"
    }

    private fun <R> queryFlow(
        query: String,
        params: List<Any>,
        fetchSize: Int,
        rowProcessor: (java.sql.ResultSet) -> R
    ): Flow<R> = channelFlow {
        withDocStoreConnection { connection ->
            connection.prepareStatement(query).use { ps ->
                ps.fetchSize = fetchSize
                params.forEachIndexed { index, param ->
                    ps.setObject(index + 1, param)
                }
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        send(rowProcessor(rs))
                    }
                }
            }
        }
    }

    override suspend fun reExtract() {
        bulkInsert(documentsByRecencyScrolling(emptyList(), null, BooleanOperator.AND, BooleanOperator.AND, 0.5, 100))
    }
}

private fun setQueryParams(statement: java.sql.PreparedStatement, tags: List<String>, query: String?) {
    var index = 1
    tags.forEach { tag ->
        statement.setString(index++, tag)
    }
    query?.let {
        statement.setString(index, it)
    }
}

private fun toEntry(rs: java.sql.ResultSet): DocStoreEntry {
    return DocStoreEntry(
        id = rs.getString("id"),
        createdAt = Instant.fromEpochMilliseconds(rs.getTimestamp("created_at").time),
        updatedAt = Instant.fromEpochMilliseconds(rs.getTimestamp("updated_at").time),
        json = rs.getString("json"),
        tags = rs.getArray("tags")?.array?.let { it as Array<*> }?.map { it.toString() },
        text = rs.getString("text"),
        similarity = try {
            rs.getObject("rank")?.let { (it as? Number)?.toFloat() }
        } catch (_: Exception) {
            null
        }
    )
}

