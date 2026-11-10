package com.tryformation.pgdocstore

import kotlin.time.Instant

enum class DocOrder {
    IdAscending,
    UpdatedAtAscending,
}

/** The storage update timestamp and ID of the last row in a page. Null timestamps sort last. */
data class DocCursor(val updatedAt: Instant?, val id: String)

/** Pass [nextCursor] to the next call with the same filter. A null cursor marks the end. */
data class DocPage<out T>(val items: List<T>, val nextCursor: DocCursor?)
