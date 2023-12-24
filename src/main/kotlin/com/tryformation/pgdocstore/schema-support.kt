package com.tryformation.pgdocstore

import com.github.jasync.sql.db.SuspendingConnection


suspend fun SuspendingConnection.dropTable(tableName: String = "docstore") {
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

suspend fun SuspendingConnection.reCreateDocStoreSchema(tableName: String = "docstore") {
    inTransaction { c ->
        c.sendQuery(
            """
                drop index if exists idx_${tableName}_created_at;
                drop index if exists idx_${tableName}_updated_at;
                drop index if exists idx_${tableName}_tags;
                drop index if exists idx_${tableName}_text;
                
                drop table if exists $tableName;
                
                CREATE TABLE "$tableName" (
                  id       text PRIMARY KEY,
                  created_at timestamptz DEFAULT current_timestamp,
                  updated_at timestamptz DEFAULT current_timestamp,            
                  json     text,
                  tags     text[],
                  text     tsvector
                );
                
                CREATE INDEX idx_${tableName}_created_at ON ${tableName} (created_at);
                CREATE INDEX idx_${tableName}_updated_at ON ${tableName} (updated_at);
                CREATE INDEX idx_${tableName}_tags ON ${tableName} USING gin (tags);
                CREATE INDEX idx_${tableName}_text ON ${tableName} USING GIN (text);
            """.trimIndent()
        )
    }
}