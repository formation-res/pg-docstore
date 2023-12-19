package com.jillesvangurp.pgdocstore

import com.github.jasync.sql.db.QueryResult
import com.github.jasync.sql.db.SuspendingConnection
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlin.random.Random
import kotlin.random.nextULong
import kotlin.reflect.KProperty
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class DocStore<T : Any>(
    val connection: SuspendingConnection,
    val serializationStrategy: KSerializer<T>,
    val tableName: String,
    val json: Json = DEFAULT_JSON,
    val tagExtractor: (T) -> List<String> = { listOf() },
    val idExtractor: (T) -> String = { d ->
        val p = d::class.members.find { it.name == "id" } as KProperty<*>?
        p?.getter?.call(d)?.toString() ?: error("property id not found")
    },

    ) {

    suspend fun create(
        doc: T,
        timestamp: Instant = Clock.System.now()
    ) {
        create(idExtractor.invoke(doc), doc, timestamp)
    }

    suspend fun create(id: String, doc: T, timestamp: Instant = Clock.System.now()) {
        val txt = json.encodeToString(serializationStrategy, doc)
        val tags = tagExtractor.invoke(doc)
        connection.inTransaction { c ->
            c.sendPreparedStatement(
                """
                INSERT INTO $tableName (id, created_at, updated_at, json, tags)
                VALUES (?,?,?,?,?)
            """.trimIndent(), listOf(id, timestamp, timestamp, txt, tags)
            )
        }
    }

    suspend fun getById(id: String): T? {
        return connection.sendPreparedStatement(
            """
            select * from $tableName where id = ?
        """.trimIndent(), listOf(id)
        ).let {
            if (it.rows.isNotEmpty()) {
                it.rows.first().getString("json")?.let { str ->
                    json.decodeFromString(serializationStrategy, str)
                }
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
        val newValue = connection.inTransaction { c ->
            c.sendPreparedStatement(
                """
            select json from $tableName where id = ?
        """.trimIndent(), listOf(id)
            ).let {
                if (it.rows.isNotEmpty()) {
                    val firstRow = it.rows.first()

                    firstRow.getString("json")?.let { str ->
                        json.decodeFromString(serializationStrategy, str).let { original ->
                            val updated = updateFunction.invoke(original)
                            val tags = tagExtractor.invoke(updated)
                            val txt = json.encodeToString(serializationStrategy, updated)
                            c.sendPreparedStatement(
                                """
                                UPDATE $tableName
                                SET 
                                    updated_at = ?,
                                    json= ?,
                                    tags = ?
                                WHERE id = ?    
                            """.trimIndent(), listOf(timestamp, txt, tags, id)
                            )
                            updated
                        }
                    } ?: error("row has no json")
                } else {
                    // FIXME exception handling
                    error("not found")
                }
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
                    INSERT INTO $tableName (id, json, tags, created_at, updated_at)
                    VALUES 
                """.trimIndent()

        // Constructing the placeholders part of the SQL
        val placeholders = chunk.joinToString(", ") { " (?, ?, ?, ?, ?)" }
        val conflictAction = """
                    ON CONFLICT (id) DO UPDATE SET
                    json = EXCLUDED.json,
                    tags = EXCLUDED.tags,
                    updated_at = EXCLUDED.updated_at
                """.trimIndent()

        val sql = baseSql + placeholders + conflictAction

        // Flatten the list of lists into a single list of parameters
        val params = chunk.map { (id, doc) ->
            val now = Clock.System.now()
            val tags = tagExtractor.invoke(doc)
            listOf(id, json.encodeToString(serializationStrategy, doc), tags, now, now)
        }.flatten()

        // Create and execute the PreparedStatement
        connection.inTransaction { c ->
            c.sendPreparedStatement(sql, params)
        }
    }

    suspend fun count(): Long {
        return connection.sendQuery("SELECT count(id) FROM $tableName").let {
            it.rows.first()[0]!! as Long
        }
    }

    suspend fun documentsByRecency() = queryFlow("SELECT * FROM $tableName ORDER BY updated_at DESC", listOf())

    suspend fun queryFlow(query: String, params: List<Any>, fetchSize: Int = 100) : Flow<T> {
        // use channelFlow for thread safety
        return channelFlow {
            val producer = this
            connection.inTransaction {c ->
                val cursorId = "cursor_${Random.nextULong()}"
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
                            val doc = json.decodeFromString(serializationStrategy,row.getString("json")?: error("empty json"))
                            producer.send(doc)
                        }
                    }

                } finally {
                    c.sendQuery("CLOSE $cursorId;")
                }
            }
        }
    }
}

