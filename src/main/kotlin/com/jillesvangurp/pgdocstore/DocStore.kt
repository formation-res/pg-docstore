package com.jillesvangurp.pgdocstore

import com.github.jasync.sql.db.SuspendingConnection
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json

class DocStore<T>(val connection: SuspendingConnection, val serializationStrategy: KSerializer<T>, val json: Json = DEFAULT_JSON) {

    suspend fun create(id:String, doc: T, tags: List<String> = listOf(), timestamp: Instant = Clock.System.now()) {
        val txt = json.encodeToString(serializationStrategy, doc)

        connection.inTransaction {c->
            c.sendPreparedStatement("""
                INSERT INTO docstore (id, created_at, updated_at, json, tags)
                VALUES (?,?,?,?,?)
            """.trimIndent(), listOf(id,timestamp,timestamp,txt,tags)
            )
        }
    }
    suspend fun getById(id: String) : T? {
        return connection.sendPreparedStatement("""
            select * from docstore where id = ?
        """.trimIndent(), listOf(id)
        ).let {
            if(it.rows.isNotEmpty()) {
                it.rows.first().getString("json")?.let { str ->
                    json.decodeFromString(serializationStrategy, str)
                }
            } else {
                null
            }
        }
    }

    suspend fun update(id: String, tagUpdateFunction: (List<String>) -> List<String> = {it}, timestamp: Instant = Clock.System.now(), updateFunction: (T)-> T): T {
        val newValue = connection.inTransaction { c->
            c.sendPreparedStatement("""
            select * from docstore where id = ?
        """.trimIndent(), listOf(id)
            ).let {
                if(it.rows.isNotEmpty()) {
                    val firstRow = it.rows.first()
                    @Suppress("UNCHECKED_CAST") val oldTags = firstRow["tags"] as List<String>
                    val newTags = tagUpdateFunction.invoke(oldTags)
                    firstRow.getString("json")?.let { str ->
                        json.decodeFromString(serializationStrategy, str).let {original ->
                            val updated = updateFunction.invoke(original)
                            val txt = json.encodeToString(serializationStrategy, updated)
                            c.sendPreparedStatement("""
                                UPDATE docstore
                                SET 
                                    updated_at = ?,
                                    json= ?,
                                    tags = ?
                                WHERE id = ?    
                            """.trimIndent(), listOf(timestamp,txt,newTags,id)
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
}

