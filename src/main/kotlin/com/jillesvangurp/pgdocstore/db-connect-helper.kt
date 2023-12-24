package com.jillesvangurp.pgdocstore

import com.github.jasync.sql.db.ConnectionPoolConfiguration
import com.github.jasync.sql.db.SuspendingConnection
import com.github.jasync.sql.db.asSuspending
import com.github.jasync.sql.db.interceptor.LoggingInterceptorSupplier
import com.github.jasync.sql.db.postgresql.PostgreSQLConnectionBuilder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.Executors

/**
 * For simple applications this should be fine. For production applications, you probably want to
 * carefully configure the connection pool.
 *
 * Note, jasync connections are non blocking but not thread safe. That's why you want a pool.
 */
fun suspendingConnection(
    host: String = "localhost",
    port: Int = 5432,
    db: String = "docstore",
    user: String = "postgres",
    password: String = "secret"
): SuspendingConnection {
    return PostgreSQLConnectionBuilder.createConnectionPool(
        ConnectionPoolConfiguration(
            host = host,
            port = port,
            database = db,
            username = user,
            password = password,
            interceptors = listOf(LoggingInterceptorSupplier()),
            coroutineDispatcher = Dispatchers.Default
        )
    ).asSuspending
}