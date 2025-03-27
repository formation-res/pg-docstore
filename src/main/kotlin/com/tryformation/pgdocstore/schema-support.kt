package com.tryformation.pgdocstore

import java.sql.Connection
import javax.sql.DataSource
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.withContext
import mu.KotlinLogging

private val logger = KotlinLogging.logger {  }

suspend fun DataSource.dropDocStoreTable(tableName: String = "docstore") {
    withContext(virtualThreadDispatcher + CoroutineName("database")) {
        this@dropDocStoreTable.connection.use { conn ->
            conn.dropDocStoreTable(tableName)
        }
    }
}

fun Connection.dropDocStoreTable(tableName: String = "docstore") {
    prepareStatement("""
        DROP INDEX IF EXISTS idx_${tableName}_created_at;
        DROP INDEX IF EXISTS idx_${tableName}_updated_at;
        DROP INDEX IF EXISTS idx_${tableName}_tags;
        DROP INDEX IF EXISTS idx_${tableName}_text;
        DROP TABLE IF EXISTS $tableName;
    """.trimIndent()).use { it.execute() }.also {
        logger.info { "dropped $tableName" }
    }
}


suspend fun DataSource.createDocStoreTable(tableName: String = "docstore") {
    withContext(virtualThreadDispatcher + CoroutineName("database")) {
        this@createDocStoreTable.connection.use { conn ->
            conn.createDocStoreTable(tableName)
        }
    }
}

fun Connection.createDocStoreTable(tableName: String = "docstore") {
    prepareStatement("CREATE EXTENSION IF NOT EXISTS pg_trgm;").use {
        try {
            it.execute()
        } catch (_: Exception) {
            // harmless if it fails (e.g. permissions or already created)
        }
    }

    prepareStatement("""
        CREATE TABLE IF NOT EXISTS "$tableName" (
            id         TEXT PRIMARY KEY,
            created_at TIMESTAMPTZ DEFAULT current_timestamp,
            updated_at TIMESTAMPTZ DEFAULT current_timestamp,
            json       TEXT,
            tags       TEXT[],
            text       TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_${tableName}_created_at ON $tableName (created_at);
        CREATE INDEX IF NOT EXISTS idx_${tableName}_updated_at ON $tableName (updated_at);
        CREATE INDEX IF NOT EXISTS idx_${tableName}_tags ON $tableName USING gin (tags);
        CREATE INDEX IF NOT EXISTS idx_${tableName}_text ON $tableName USING gin (text gin_trgm_ops);
    """.trimIndent()).use {
        it.executeUpdate()
    }.also {
        logger.info { "created $tableName" }
    }
}
