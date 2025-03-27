package com.tryformation.pgdocstore

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

fun connectionPool(
    jdbcUrl: String = "jdbc:postgresql://localhost:5432/docstore",
    username: String = "postgres",
    password: String = "secret",
    maximumPoolSize: Int = 200,
    minimumIdle: Int = 5,
    idleTimeout: Long = 30000,
    maxLifetime: Long = 60000,
    connectionTimeout: Long = 10000
) = HikariConfig().apply {
    driverClassName = "org.postgresql.Driver"
    this.jdbcUrl = jdbcUrl
    this.username = username
    this.password = password

    this.maximumPoolSize = maximumPoolSize
    this.minimumIdle = minimumIdle
    this.idleTimeout = idleTimeout
    this.maxLifetime = maxLifetime
    this.connectionTimeout = connectionTimeout

    // PostgreSQL specific performance tweaks
    addDataSourceProperty("cachePrepStmts", "true")
    addDataSourceProperty("prepStmtCacheSize", "250")
    addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    addDataSourceProperty("stringtype", "unspecified")
}.let {
    HikariDataSource(it)
}

