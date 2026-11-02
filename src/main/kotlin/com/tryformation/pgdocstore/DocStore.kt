package com.tryformation.pgdocstore

import com.jillesvangurp.serializationext.DEFAULT_JSON
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Timestamp
import javax.sql.DataSource
import kotlin.reflect.KProperty
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Instant
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import io.github.oshai.kotlinlogging.KotlinLogging


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
    /** used to normalize extracted text before it is stored */
    private val indexNormalizer: (String)->String = {it},
    /** used to normalize extracted text before it is queried; defaults to doing the same thing as [indexNormalizer] */
    private val queryNormalizer: (String)->String = indexNormalizer,
    private val idExtractor: (T) -> String = { d ->
        val p = d::class.members.find { it.name == "id" } as KProperty<*>?
        p?.getter?.call(d)?.toString() ?: error("property id not found")
    },
    private val logging: Boolean = false,
    /** Not null if in a transaction.  */
    private val connection: Connection? = null,

    ) : IDocStore<T> {
    private suspend fun <R> withDocStoreConnection(block: suspend (Connection) -> R): R =
        if (connection != null) onDatabaseDispatcher {
            currentCoroutineContext().ensureActive()
            block(connection)
        } else dataSource.withConnection(block)

    private suspend fun <R> withDocStoreTransaction(block: suspend (Connection) -> R): R =
        if (connection != null) withDocStoreConnection(block) else dataSource.withTransaction(block)

    override suspend fun <R> transact(block: suspend (IDocStore<T>) -> R): R =
        if (connection != null) block(this) else dataSource.withTransaction { conn ->
            block(
                DocStore(
                    dataSource = dataSource,
                    serializationStrategy = serializationStrategy,
                    tableName = tableName,
                    json = json,
                    tagExtractor = tagExtractor,
                    textExtractor = textExtractor,
                    indexNormalizer = indexNormalizer,
                    queryNormalizer = queryNormalizer,
                    idExtractor = idExtractor,
                    logging = logging,
                    connection = conn
                )
            )
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
                statement.setString(6, text?.let { indexNormalizer(it) })
                if (logging) logger.info { "SQL: ${statement}" }
                statement.withCancellation { statement.executeUpdate() }
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
                if (logging) logger.info { "SQL: ${statement}" }

                statement.query { resultSet ->
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

                statement.query { resultSet ->
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
                statement.query { resultSet ->
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

                statement.query { resultSet ->
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

        return withDocStoreTransaction { conn ->
            // Fetch the existing document
            val existing = conn.prepareStatement(
                "SELECT json FROM $tableName WHERE id = ? FOR UPDATE"
            ).use { stmt ->
                stmt.setString(1, id)
                stmt.query { rs ->
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
                stmt.setString(4, text?.let { indexNormalizer(it) })
                stmt.setString(5, id)
                if (logging) logger.info { "SQL: ${stmt}" }
                check(stmt.withCancellation { stmt.executeUpdate() } == 1) { "Document disappeared during update: $id" }
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
                val deleted = statement.withCancellation { statement.executeUpdate() }
                if (logging) {
                    if (deleted > 0) logger.info { "$tableName delete $id: deleted" }
                    else logger.info { "$tableName delete $id: not found" }
                }
            }
        }
    }

    override suspend fun deleteByIds(ids: Collection<String>): Long {
        if (ids.isEmpty()) return 0L

        return withDocStoreConnection { connection ->
            val array = connection.createArrayOf("text", ids.toTypedArray())
            try {
                connection.prepareStatement("DELETE FROM $tableName WHERE id = ANY (?::text[])").use { statement ->
                    statement.setArray(1, array)
                    statement.withCancellation { statement.executeLargeUpdate() }
                }
            } finally {
                array.free()
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
            sequence = iterable.asSequence().map { d ->
                idExtractor.invoke(d) to d
            },
            chunkSize = chunkSize
        )
    }

    override suspend fun bulkInsertWithId(flow: Flow<Pair<String, T>>, chunkSize: Int, delayMillis: Duration) {
        if (logging) logger.info { "$tableName start bulk inserting" }

        flow.chunked(chunkSize = chunkSize, delayMillis = delayMillis).collect { chunk ->
            insertList(chunk)
        }
        if (logging) logger.info { "$tableName done bulk inserting" }

    }

    override suspend fun bulkInsertWithId(iterable: Iterable<Pair<String, T>>, chunkSize: Int) {
        bulkInsertWithId(iterable.asSequence(), chunkSize)
    }

    override suspend fun bulkInsertWithId(sequence: Sequence<Pair<String, T>>, chunkSize: Int) {
        sequence.chunked(chunkSize).forEach { chunk ->
            insertList(chunk)
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
                    statement.setString(paramIndex++, text?.let { indexNormalizer(it) })
                }
                if (logging) logger.info { "SQL: ${statement}" }

                statement.withCancellation { statement.executeUpdate() }
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
                statement.query { resultSet ->
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
        similarityThreshold: Double,
        updatedAtAfter: Instant?,
        updatedAtBefore: Instant?,
    ): List<T> {
        val sql =
            constructQuery(
                tags = tags,
                query = query,
                tagsClauseOperator = tagsClauseOperator,
                whereClauseOperator = whereClauseOperator,
                limit = limit,
                offset = offset,
                similarityThreshold = similarityThreshold,
                updatedAtAfter = updatedAtAfter,
                updatedAtBefore = updatedAtBefore
            )
        return withQueryConnection(sql) { connection ->
            connection.prepareStatement(sql.sql).use { statement ->
                setQueryParams(statement, sql.params)
                statement.query { rs ->
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
        similarityThreshold: Double,
        updatedAtAfter: Instant?,
        updatedAtBefore: Instant?,
    ): List<DocStoreEntry> {
        val sql =
            constructQuery(
                tags = tags,
                query = query,
                tagsClauseOperator = tagsClauseOperator,
                whereClauseOperator = whereClauseOperator,
                limit = limit,
                offset = offset,
                similarityThreshold = similarityThreshold,
                updatedAtAfter = updatedAtAfter,
                updatedAtBefore = updatedAtBefore
            )
        return withQueryConnection(sql) { connection ->
            connection.prepareStatement(sql.sql).use { statement ->
                setQueryParams(statement, sql.params)
                statement.query { rs ->
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
        fetchSize: Int,
        updatedAtAfter: Instant?,
        updatedAtBefore: Instant?,
    ): Flow<T> = queryFlow(
        constructQuery(
            tags = tags,
            query = query,
            tagsClauseOperator = tagsClauseOperator,
            whereClauseOperator = whereClauseOperator,
            limit = null,
            offset = 0,
            similarityThreshold = similarityThreshold,
            updatedAtAfter = updatedAtAfter,
            updatedAtBefore = updatedAtBefore
        ),
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
        similarityThreshold: Double,
        updatedAtAfter: Instant?,
        updatedAtBefore: Instant?,
    ): Flow<DocStoreEntry> = queryFlow(
        constructQuery(
            tags = tags,
            query = query,
            tagsClauseOperator = tagsClauseOperator,
            whereClauseOperator = whereClauseOperator,
            limit = null,
            offset = 0,
            similarityThreshold = similarityThreshold,
            updatedAtAfter = updatedAtAfter,
            updatedAtBefore = updatedAtBefore
        ),
        fetchSize
    ) { rs -> toEntry(rs) }

    private data class Query(val sql: String, val params: List<Any>, val similarityThreshold: Double?)

    private fun constructQuery(
        tags: List<String>,
        query: String?,
        tagsClauseOperator: BooleanOperator,
        whereClauseOperator: BooleanOperator,
        limit: Int?,
        offset: Int,
        similarityThreshold: Double,
        updatedAtAfter: Instant? = null,
        updatedAtBefore: Instant? = null,
    ): Query {
        require(similarityThreshold.isFinite() && similarityThreshold in 0.0..1.0) {
            "similarityThreshold must be between 0 and 1"
        }
        require(limit == null || limit >= 0) { "limit must not be negative" }
        require(offset >= 0) { "offset must not be negative" }
        val normalizedQuery = query?.takeIf { it.isNotBlank() }?.let(queryNormalizer)
        val fuzzy = normalizedQuery != null && similarityThreshold != 1.0
        val params = mutableListOf<Any>()
        val rankSelect = if (fuzzy) {
            params += normalizedQuery
            ", similarity(text, ?) AS rank"
        } else ""
        val clauses = mutableListOf<String>()
        if (tags.isNotEmpty()) {
            params.add(tags)
            clauses += "tags ${if (tagsClauseOperator == BooleanOperator.AND) "@>" else "&&"} ?::text[]"
        }
        if (normalizedQuery != null) {
            params += normalizedQuery
            clauses += if (fuzzy) {
                // The operator enables GIN; the exact comparison preserves the Double threshold contract.
                params += normalizedQuery
                "(text % ? AND similarity(text, ?) >= $similarityThreshold)"
            } else "text LIKE ?"
        }
        val dateClauses = mutableListOf<String>()
        updatedAtAfter?.let { dateClauses += "updated_at >= ?"; params += it.toSqlTimestamp() }
        updatedAtBefore?.let { dateClauses += "updated_at <= ?"; params += it.toSqlTimestamp() }
        if (dateClauses.isNotEmpty()) clauses += dateClauses.joinToString(" AND ", "(", ")")
        val where = if (clauses.isEmpty()) "" else "WHERE " + clauses.joinToString(" $whereClauseOperator ")
        val order = if (fuzzy) "rank DESC, updated_at DESC, id" else "updated_at DESC, id"
        val pagination = limit?.let { " LIMIT $it OFFSET $offset" } ?: ""
        return Query("SELECT *$rankSelect FROM $tableName $where ORDER BY $order$pagination", params,
            similarityThreshold.takeIf { fuzzy }).also {
            if (logging) logger.info { it.sql }
        }
    }

    private suspend fun <R> withQueryConnection(
        query: Query,
        cursor: Boolean = false,
        block: suspend (Connection) -> R
    ): R {
        suspend fun execute(conn: Connection): R {
            val threshold = query.similarityThreshold ?: return block(conn)
            suspend fun setThreshold(value: String) {
                conn.prepareStatement("SELECT set_config('pg_trgm.similarity_threshold', ?, true)").use { ps ->
                    ps.setString(1, value)
                    ps.query { rs -> rs.next() }
                }
            }
            // For an owned transaction SET LOCAL is automatically reset by commit/rollback.
            if (connection == null) {
                setThreshold(threshold.toFloat().toString())
                return block(conn)
            }
            val previous = conn.prepareStatement("SELECT current_setting('pg_trgm.similarity_threshold', true)").use { ps ->
                ps.query { rs ->
                    rs.next()
                    rs.getString(1) ?: "0.3"
                }
            }
            setThreshold(threshold.toFloat().toString())
            var failure: Throwable? = null
            try {
                return block(conn)
            } catch (e: Throwable) {
                failure = e
                throw e
            } finally {
                // Also restore after short-circuiting a flow inside the caller's still-active transaction.
                try {
                    withContext(NonCancellable) { setThreshold(previous) }
                } catch (e: Throwable) {
                    if (failure == null) throw e else failure.addSuppressed(e)
                }
            }
        }
        return if (cursor || query.similarityThreshold != null) withDocStoreTransaction { execute(it) }
        else withDocStoreConnection { execute(it) }
    }

    private fun <R> queryFlow(
        query: Query,
        fetchSize: Int,
        rowProcessor: (java.sql.ResultSet) -> R
    ): Flow<R> {
        require(fetchSize > 0) { "fetchSize must be positive" }
        return flow {
            withQueryConnection(query, cursor = true) { conn ->
                conn.prepareStatement(query.sql).use { ps ->
                    ps.fetchSize = fetchSize
                    setQueryParams(ps, query.params)
                    ps.query { rs ->
                        while (rs.next()) emit(rowProcessor(rs))
                    }
                }
            }
        }.flowOn(dbScope)
    }

    override suspend fun reExtract() {
        var afterId: String? = null
        while (true) {
            val page = withDocStoreConnection { conn ->
                val where = if (afterId == null) "" else "WHERE id > ?"
                conn.prepareStatement("SELECT id, json FROM $tableName $where ORDER BY id LIMIT 100").use { ps ->
                    afterId?.let { ps.setString(1, it) }
                    ps.query { rs ->
                        buildList { while (rs.next()) add(rs.getString("id") to rs.getString("json")) }
                    }
                }
            }
            if (page.isEmpty()) break
            withDocStoreConnection { conn ->
                conn.prepareStatement("UPDATE $tableName SET tags = ?, text = ?, updated_at = ? WHERE id = ? AND json = ?").use { ps ->
                    for ((id, originalJson) in page) {
                        currentCoroutineContext().ensureActive()
                        val doc = json.decodeFromString(serializationStrategy, originalJson)
                        ps.setArray(1, conn.createArrayOf("text", tagExtractor(doc)?.toTypedArray()))
                        ps.setString(2, textExtractor(doc)?.let(indexNormalizer))
                        ps.setTimestamp(3, Clock.System.now().toSqlTimestamp())
                        ps.setString(4, id)
                        ps.setString(5, originalJson)
                        // Concurrent updates/deletes win; never write back the scanned JSON or insert missing rows.
                        ps.withCancellation { ps.executeUpdate() }
                    }
                }
            }
            afterId = page.last().first
        }
    }

    private fun setQueryParams(statement: PreparedStatement, params: List<Any>) {
        params.forEachIndexed { index, param ->
            when (param) {
                is List<*> -> statement.setArray(index + 1, statement.connection.createArrayOf("text", param.toTypedArray()))
                is String -> statement.setString(index + 1, param)
                is Timestamp -> statement.setTimestamp(index + 1, param)
                else -> statement.setObject(index + 1, param)
            }
        }
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

