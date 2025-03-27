package com.tryformation.pgdocstore

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.Flow
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant

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
     * If already inside a transaction, reuses the current one.
     *
     * @param block A suspending function that takes a transactional version of [IDocStore].
     * @return The result of the block.
     */
    suspend fun <R> transact(block: suspend (IDocStore<T>) -> R): R

    /**
     * Create a document. The [idExtractor] will be used to extract an id from the document.
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
     * Note,
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
     * Bulk insert documents from a [flow].
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Note, unless you surround this with transact,
     * no transaction is used.
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
     * are inserted for each page. Note, unless you surround this with transact,
     * no transaction is used.
     */
    suspend fun bulkInsert(
        sequence: Sequence<T>,
        chunkSize: Int = 100
    )

    /**
     * Bulk insert any [iterable] (List, Set, etc.).
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Note, unless you surround this with transact,
     * no transaction is used.
     */
    suspend fun bulkInsert(
        iterable: Iterable<T>,
        chunkSize: Int = 100
    )

    /**
     * Bulk insert documents from a [flow].
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Note, unless you surround this with transact,
     * no transaction is used.
     *
     * Useful in combination with [documentsByRecencyScrolling], which returns a flow of documents.
     */
    suspend fun bulkInsertWithId(flow: Flow<Pair<String, T>>, chunkSize: Int = 100, delayMillis: Duration = 1.seconds)

    /**
     * Bulk insert documents from an [iterable].
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Note, unless you surround this with transact,
     * no transaction is used.
     */
    suspend fun bulkInsertWithId(iterable: Iterable<Pair<String, T>>, chunkSize: Int = 100)

    /**
     * Bulk insert documents from a [sequence].
     *
     * Specify [chunkSize] to control how many documents
     * are inserted for each page. Note, unless you surround this with transact,
     * no transaction is used.
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
     * Returns a list of documents ordered by recency. This fetches a maximum of [limit] document
     * from the specified [offset]
     *
     * You can optionally constrain
     * the query with [tags]. If you set [whereClauseOperator] to OR,
     * a logical OR will be used instead of the default AND.
     *
     * You can also specify a text [query]. In that case the results will be ordered by
     * their ranking. You can use [similarityThreshold] to control how strict it matches.
     */
    suspend fun documentsByRecency(
        tags: List<String> = emptyList(),
        query: String? = null,
        tagsClauseOperator: BooleanOperator = BooleanOperator.AND,
        whereClauseOperator: BooleanOperator = BooleanOperator.AND,
        limit: Int = 100,
        offset: Int = 0,
        similarityThreshold: Double = 0.1,
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
    ): List<DocStoreEntry>

    /**
     * Returns a flow of documents ordered by recency. This will scroll through the results
     * and fetch pages of results [fetchSize] documents at the time. Each document is emitted
     * to the flow as it comes in.
     *
     * You can use this to efficiently process all documents in your store.
     *
     * You can optionally constrain
     * the query with [tags]. If you set [whereClauseOperator] to OR,
     * a logical OR will be used instead of the default AND.
     *
     * You can also specify a text [query]. In that case the results will be ordered by
     * their ranking. You can use [similarityThreshold] to control how strict it matches.
     */
    suspend fun documentsByRecencyScrolling(
        tags: List<String> = emptyList(),
        query: String? = null,
        tagsClauseOperator: BooleanOperator = BooleanOperator.AND,
        whereClauseOperator: BooleanOperator = BooleanOperator.AND,
        similarityThreshold: Double = 0.1,
        fetchSize: Int = 100,
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
    ): Flow<DocStoreEntry>

    /**
     * Updates all documents in the store and re-runs all the extract functions.
     *
     * This is useful when you change the way those functions work.
     *
     * Note, this may take a while to run on a big database.
     * Also, while documentsByRecencyScrolling necessarily has its
     * own transaction (scrolling has to be done in a transaction), bulkInsert does not.
     * You can wrap the bulkInsert with a separate transaction but it will be a separate one.
     */
    suspend fun reExtract()
}