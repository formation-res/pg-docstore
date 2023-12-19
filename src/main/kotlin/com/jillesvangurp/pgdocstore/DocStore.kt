package com.jillesvangurp.pgdocstore

import com.github.jasync.sql.db.SuspendingConnection
import kotlinx.coroutines.flow.Flow
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class DocStore<T>(
    val connection: SuspendingConnection,
    val serializationStrategy: KSerializer<T>,
    val json: Json = DEFAULT_JSON,
    val tagExtractor : (T)-> List<String> = { listOf() }
) {

    suspend fun create(id: String, doc: T, timestamp: Instant = Clock.System.now()) {
        val txt = json.encodeToString(serializationStrategy, doc)
        val tags = tagExtractor.invoke(doc)
        connection.inTransaction { c ->
            c.sendPreparedStatement(
                """
                INSERT INTO docstore (id, created_at, updated_at, json, tags)
                VALUES (?,?,?,?,?)
            """.trimIndent(), listOf(id, timestamp, timestamp, txt, tags)
            )
        }
    }

    suspend fun getById(id: String): T? {
        return connection.sendPreparedStatement(
            """
            select * from docstore where id = ?
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
        id: String,
        timestamp: Instant = Clock.System.now(),
        updateFunction: (T) -> T
    ): T {
        val newValue = connection.inTransaction { c ->
            c.sendPreparedStatement(
                """
            select json from docstore where id = ?
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
                                UPDATE docstore
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

    suspend fun delete(id: String) {
        connection.sendPreparedStatement(
            """DELETE FROM docstore WHERE id = ?""", listOf(id)
        )
    }

    suspend fun bulkInsert(flow: Flow<Pair<String, T>>, chunkSize: Int = 100, delayMillis: Duration = 1.seconds) {
        flow.chunked(chunkSize = chunkSize, delayMillis = delayMillis).collect { chunk ->
            insertList(chunk)
        }
    }
    suspend fun bulkInsert(iterable: Iterable<Pair<String,T>>, chunkSize: Int = 100) {
        bulkInsert(iterable.asSequence(), chunkSize)
    }

    suspend fun bulkInsert(sequence: Sequence<Pair<String,T>>, chunkSize: Int = 100) {
        sequence.chunked(chunkSize).forEach { chunk ->
            insertList(chunk)
        }
    }

    suspend fun insertList(chunk: List<Pair<String, T>>) {
        // Base SQL for INSERT
        val baseSql = """
                    INSERT INTO docstore (id, json, tags, created_at, updated_at)
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
        return connection.sendQuery("SELECT count(id) FROM docstore").let {
            it.rows.first()[0]!! as Long
        }
    }
}

