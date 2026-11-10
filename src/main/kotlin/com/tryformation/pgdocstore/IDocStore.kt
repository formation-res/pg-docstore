package com.tryformation.pgdocstore

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.Flow
import kotlin.time.Clock
import kotlin.time.Instant
/**
 * Specifies how multiple conditions are combined in queries.
 * Use [AND] for logical AND, [OR] for logical OR.
 */
enum class BooleanOperator {
    OR,
    AND
}

/**
 * Interface for the docstore to facilitate alternate implementations. For different
 * databases or using different database libraries to connect (an earlier version actually used jasync postgresql)
 */
interface IDocStore<T : Any> {
    /**
     * Executes the given [block] within a database transaction.
     *
     * Returns the result of [block]. If an exception is thrown, the transaction is rolled back.
     * If already inside a transaction, reuses the current one (without a savepoint).
     * Use the supplied store for every operation that should participate in the transaction.
     * Use a transactional store sequentially; do not share it between concurrent coroutines or retain it after the block.
     * Collect flows from it before the block returns.
     *
     * JDBC statements are cancelled when the coroutine is cancelled, and cancellation is checked before committing.
     * A cancellation or connection failure during commit can still leave the outcome uncertain; retries must be idempotent.
     *
     * @param block A suspending function that takes a transactional version of [IDocStore].
     * @return The result of the block.
     */
    suspend fun <R> transact(block: suspend (IDocStore<T>) -> R): R

    /**
     * Create a document. The configured id extractor will be used to extract an id from the document.
     *
     * Optionally, you can override the [timestamp] and specify that the document should be
     * overwritten if it already exists with [onConflictUpdate].
     *
     * Returns the created [DocStoreEntry]
     */
    suspend fun create(
        doc: T,
        timestamp: Instant = Clock.System.now(),
        onConflictUpdate: Boolean = false
    ): DocStoreEntry

    /**
     * Create a document and use the specified [id] as the id.
     *
     * Optionally, you can override the [timestamp] and specify that the document should be
     * overwritten if it already exists with [onConflictUpdate]
     *
     * Returns the created [DocStoreEntry]
     *
     */
    suspend fun create(
        id: String,
        doc: T,
        timestamp: Instant = Clock.System.now(),
        onConflictUpdate: Boolean = false
    ): DocStoreEntry

    /**
     * Retrieve a document by [id].
     *
     * Returns the deserialized document or null if it doesn't exist.
     */
    suspend fun getById(id: String): T?

    /**
     * Retrieve multiple documents by their [ids].
     */
    suspend fun multiGetById(ids: List<String>): List<T>

    /**
     * Retrieve a [DocStoreEntry] by its [id]. Returns null if there is no matching entry.
     */
    suspend fun getEntryById(id: String): DocStoreEntry?

    /**
     * Retrieve multiple [DocStoreEntry] by their [ids]
     */
    suspend fun multiGetEntryById(ids: List<String>): List<DocStoreEntry>

    /**
     * Update a document. The document will be retrieved from the database and then the [updateFunction]
     * is applied to it to modify it.
     *
     * You can optionally specify a different [timestamp] than now.
     *
     * The row is locked until the transaction ends, so concurrent updates read the latest committed version.
     * The callback runs once while holding that lock; keep suspending work inside it short.
     */
    suspend fun update(
        doc: T,
        timestamp: Instant = Clock.System.now(),
        updateFunction: suspend (T) -> T
    ): T

    suspend fun update(
        id: String,
        timestamp: Instant = Clock.System.now(),
        updateFunction: suspend (T) -> T
    ): T

    /**
     * Deletes a [document].
     */
    suspend fun delete(
        document: T,
    )

    /**
     * Delete a document by its [id].
     */
    suspend fun delete(id: String)

    /**
     * Deletes documents with the given [ids] and returns the number of rows deleted.
     * Missing ids are ignored; duplicate ids do not increase the count.
     * Empty input returns zero without opening a connection.
     *
     * Uses one atomic DELETE statement with a bound PostgreSQL array, including for large batches.
     * The array is held in memory and must fit PostgreSQL's array and message size limits.
     * The database may reject requests with too many ids or excessively long ids. Use your discretion when
     * choosing a batch size: memory use, execution time, and lock duration depend on your data and workload.
     * Joins the enclosing [transact] transaction and responds to coroutine cancellation.
     */
    suspend fun deleteByIds(ids: Collection<String>): Long

    /**
     * Bulk insert documents from a [flow].
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Each chunk commits independently unless you surround this with [transact].
     * An upstream failure leaves earlier committed chunks in place.
     *
     * Useful in combination with [documentsByRecencyScrolling], which returns a flow of documents.
     */
    suspend fun bulkInsert(
        flow: Flow<T>,
        chunkSize: Int = 100, delayMillis: Duration = 1.seconds
    )

    /**
     * Bulk insert a [sequence].
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Each chunk commits independently unless you surround this with [transact].
     * An upstream failure leaves earlier committed chunks in place.
     */
    suspend fun bulkInsert(
        sequence: Sequence<T>,
        chunkSize: Int = 100
    )

    /**
     * Bulk insert any [iterable] (List, Set, etc.).
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Each chunk commits independently unless you surround this with [transact].
     * An upstream failure leaves earlier committed chunks in place.
     */
    suspend fun bulkInsert(
        iterable: Iterable<T>,
        chunkSize: Int = 100
    )

    /**
     * Bulk insert documents from a [flow].
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Each chunk commits independently unless you surround this with [transact].
     * An upstream failure leaves earlier committed chunks in place.
     *
     * Useful in combination with [documentsByRecencyScrolling], which returns a flow of documents.
     */
    suspend fun bulkInsertWithId(flow: Flow<Pair<String, T>>, chunkSize: Int = 100, delayMillis: Duration = 1.seconds)

    /**
     * Bulk insert documents from an [iterable].
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Each chunk commits independently unless you surround this with [transact].
     * An upstream failure leaves earlier committed chunks in place.
     */
    suspend fun bulkInsertWithId(iterable: Iterable<Pair<String, T>>, chunkSize: Int = 100)

    /**
     * Bulk insert documents from a [sequence].
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Each chunk commits independently unless you surround this with [transact].
     * An upstream failure leaves earlier committed chunks in place.
     */
    suspend fun bulkInsertWithId(sequence: Sequence<Pair<String, T>>, chunkSize: Int = 100)

    /**
     * Insert a list of id, document pairs. Used by the various bulk inserts to insert documents.
     *
     * This falls back to overwriting the document with ON CONFLICT (id) DO UPDATE in case
     * the document already exists.
     */
    suspend fun insertList(chunk: List<Pair<String, T>>, timestamp: Instant = Clock.System.now())

    suspend fun count(): Long

    /**
     * Reads up to [limit] documents after applying [filter]. The limit must be nonnegative.
     * Ordering uses storage columns. Update timestamp ties are ordered by storage ID.
     */
    suspend fun find(filter: DocFilter, limit: Int = 100, order: DocOrder = DocOrder.IdAscending): List<T> =
        throw UnsupportedOperationException("This store does not support composable filters")

    /** Counts matching rows in PostgreSQL without loading their documents. */
    suspend fun count(filter: DocFilter): Long =
        throw UnsupportedOperationException("This store does not support composable filters")

    /**
     * Reads up to [limit] matching storage IDs ordered by update timestamp and then storage ID.
     * Pass the previous page's cursor as [after]. The limit must be positive.
     * Each call observes the current transaction's view. Separate calls can see concurrent changes.
     */
    suspend fun pageIds(filter: DocFilter, after: DocCursor? = null, limit: Int = 100): DocPage<String> =
        throw UnsupportedOperationException("This store does not support composable filters")

    /** Same cursor and ordering as [pageIds], returning deserialized documents. */
    suspend fun pageDocuments(filter: DocFilter, after: DocCursor? = null, limit: Int = 100): DocPage<T> =
        throw UnsupportedOperationException("This store does not support composable filters")

    /**
     * Returns a list of documents ordered by recency. This fetches a maximum of [limit] document
     * from the specified [offset]
     *
     * You can optionally constrain
     * the query with [tags]. If you set [whereClauseOperator] to OR,
     * a logical OR will be used instead of the default AND.
     *
     * You can also specify a text [query]. In that case the results will be ordered by
     * their ranking. [similarityThreshold] must be between 0 and 1; 1 uses LIKE matching.
     * [limit] and [offset] must not be negative.
     */
    suspend fun documentsByRecency(
        tags: List<String> = emptyList(),
        query: String? = null,
        tagsClauseOperator: BooleanOperator = BooleanOperator.AND,
        whereClauseOperator: BooleanOperator = BooleanOperator.AND,
        limit: Int = 100,
        offset: Int = 0,
        similarityThreshold: Double = 0.1,
        updatedAtAfter: Instant? = null,
        updatedAtBefore: Instant? = null,

        ): List<T>

    /**
     * Similar to [documentsByRecency] but returns a list of the [DocStoreEntry] with the full row data.
     */
    suspend fun entriesByRecency(
        tags: List<String> = emptyList(),
        query: String? = null,
        tagsClauseOperator: BooleanOperator = BooleanOperator.AND,
        whereClauseOperator: BooleanOperator = BooleanOperator.AND,
        limit: Int = 100,
        offset: Int = 0,
        similarityThreshold: Double = 0.1,
        updatedAtAfter: Instant? = null,
        updatedAtBefore: Instant? = null,

        ): List<DocStoreEntry>

    /**
     * Returns a flow of documents ordered by recency. This will scroll through the results
     * and fetch pages of results [fetchSize] documents at the time. Each document is emitted
     * to the flow as it comes in.
     *
     * You can use this to efficiently process all documents in your store.
     * [fetchSize] must be positive. Collection holds a connection and a transaction until it completes or is cancelled.
     * An existing transaction is reused; collect the flow inside its [transact] block.
     *
     * You can optionally constrain
     * the query with [tags]. If you set [whereClauseOperator] to OR,
     * a logical OR will be used instead of the default AND.
     *
     * You can also specify a text [query]. In that case the results will be ordered by
     * their ranking. [similarityThreshold] must be between 0 and 1; 1 uses LIKE matching.
     */
    suspend fun documentsByRecencyScrolling(
        tags: List<String> = emptyList(),
        query: String? = null,
        tagsClauseOperator: BooleanOperator = BooleanOperator.AND,
        whereClauseOperator: BooleanOperator = BooleanOperator.AND,
        similarityThreshold: Double = 0.1,
        fetchSize: Int = 100,
        updatedAtAfter: Instant? = null,
        updatedAtBefore: Instant? = null,

        ): Flow<T>

    /**
     * Similar to [documentsByRecencyScrolling] but returns a flow of the [DocStoreEntry] with the full row data.
     */
    suspend fun entriesByRecencyScrolling(
        tags: List<String> = emptyList(),
        query: String? = null,
        tagsClauseOperator: BooleanOperator = BooleanOperator.AND,
        whereClauseOperator: BooleanOperator = BooleanOperator.AND,
        fetchSize: Int = 100,
        similarityThreshold: Double = 0.1,
        updatedAtAfter: Instant? = null,
        updatedAtBefore: Instant? = null,

        ): Flow<DocStoreEntry>

    /**
     * Refreshes the extracted tags and text in bounded pages, preserving JSON, row ids and creation timestamps.
     * Successfully refreshed rows receive a new updated timestamp.
     *
     * This is useful when you change the extract functions. Concurrent updates and deletions win:
     * rows whose JSON changed after being read are skipped. Run again to refresh any skipped rows.
     * Each derived-column update commits independently unless called inside [transact].
     */
    suspend fun reExtract()
}
