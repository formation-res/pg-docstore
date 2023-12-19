package com.jillesvangurp.pgdocstore

import com.github.jasync.sql.db.SuspendingConnection

suspend fun SuspendingConnection.reCreateDocStoreSchema() {
    inTransaction { c ->
        c.sendQuery(
            """
                drop index if exists idx_docstore_created_at;
                drop index if exists idx_docstore_updated_at;
                drop index if exists idx_docstore_tags;
                
                drop table if exists docstore;
                
                CREATE TABLE "docstore" (
                  id       varchar(255) PRIMARY KEY,
                  created_at timestamptz DEFAULT current_timestamp,
                  updated_at timestamptz DEFAULT current_timestamp,            
                  json     text,
                  tags     text[]
                );
                
                CREATE INDEX idx_docstore_created_at ON docstore (created_at);
                CREATE INDEX idx_docstore_updated_at ON docstore (updated_at);
                CREATE INDEX idx_docstore_tags ON docstore USING gin (tags);
            """.trimIndent()
        )
    }
}