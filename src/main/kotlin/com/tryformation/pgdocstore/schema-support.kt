package com.tryformation.pgdocstore

import com.github.jasync.sql.db.SuspendingConnection


suspend fun SuspendingConnection.dropDocStoreTable(tableName: String = "docstore") {
    inTransaction { c ->
        c.sendQuery(
            """
                drop index if exists idx_${tableName}_created_at;
                drop index if exists idx_${tableName}_updated_at;
                drop index if exists idx_${tableName}_tags;
                
                drop table if exists $tableName;
            """.trimIndent()
        )
    }
}

suspend fun SuspendingConnection.createDocStoreTable(tableName: String) {
    inTransaction { c ->
        c.sendQuery(
            """
                CREATE TABLE IF NOT EXISTS "$tableName" (
                  id       text PRIMARY KEY,
                  created_at timestamptz DEFAULT current_timestamp,
                  updated_at timestamptz DEFAULT current_timestamp,            
                  json     text,
                  tags     text[],
                  text     tsvector
                );
                
                CREATE INDEX IF NOT EXISTS idx_${tableName}_created_at ON ${tableName} (created_at);
                CREATE INDEX IF NOT EXISTS idx_${tableName}_updated_at ON ${tableName} (updated_at);
                CREATE INDEX IF NOT EXISTS idx_${tableName}_tags ON ${tableName} USING gin (tags);
                CREATE INDEX IF NOT EXISTS idx_${tableName}_text ON ${tableName} USING GIN (text);
            """.trimIndent()
        )
    }
}

suspend fun SuspendingConnection.reCreateDocStoreTable(tableName: String) {
    inTransaction { c ->
        c.dropDocStoreTable(tableName)
        c.createDocStoreTable(tableName)
    }
}