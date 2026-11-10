package com.tryformation.pgdocstore

import java.sql.Timestamp
import kotlin.time.Instant
import kotlin.time.toJavaInstant

/**
 * A predicate over extracted tags, storage IDs, and storage update timestamps.
 * Use the factory functions to compose filters. Values are bound as JDBC parameters.
 */
sealed class DocFilter {
    /** Combines child filters with AND or OR. */
    class Group internal constructor(val filters: List<DocFilter>, val operator: BooleanOperator) : DocFilter()

    /** Matches rows that do not match the child filter. */
    class Negated internal constructor(val filter: DocFilter) : DocFilter()

    /** Tests all or any of the given exact tags. */
    class Tags internal constructor(val values: List<String>, val operator: BooleanOperator) : DocFilter()

    /** Matches any of the given storage IDs. */
    class Ids internal constructor(val values: List<String>) : DocFilter()

    /** Restricts the storage update time to the given inclusive bounds. */
    class UpdatedAt internal constructor(val fromInclusive: Instant?, val throughInclusive: Instant?) : DocFilter()
}

/** Matches when every child matches. An empty group matches every row. */
fun allOf(vararg filters: DocFilter): DocFilter = allOf(filters.asList())

fun allOf(filters: Collection<DocFilter>): DocFilter = DocFilter.Group(filters.toList(), BooleanOperator.AND)

/** Matches when at least one child matches. An empty group matches no rows. */
fun anyOf(vararg filters: DocFilter): DocFilter = anyOf(filters.asList())

fun anyOf(filters: Collection<DocFilter>): DocFilter = DocFilter.Group(filters.toList(), BooleanOperator.OR)

/** Matches rows for which [filter] does not match, including rows missing the tested value. */
fun not(filter: DocFilter): DocFilter = DocFilter.Negated(filter)

/** Matches rows containing all the given tags. An empty list matches every row. */
fun tagsAll(vararg tags: String): DocFilter = tagsAll(tags.asList())

fun tagsAll(tags: Collection<String>): DocFilter = DocFilter.Tags(tags.toList(), BooleanOperator.AND)

/** Matches rows containing any of the given tags. An empty list matches no rows. */
fun tagsAny(vararg tags: String): DocFilter = tagsAny(tags.asList())

fun tagsAny(tags: Collection<String>): DocFilter = DocFilter.Tags(tags.toList(), BooleanOperator.OR)

/** Matches the storage ID against the given IDs. An empty list matches no rows. */
fun idsIn(ids: Collection<String>): DocFilter = DocFilter.Ids(ids.toList())

/**
 * Matches the storage update timestamp within inclusive bounds.
 * A null bound leaves that side open. Two null bounds match every row.
 */
fun updatedAt(fromInclusive: Instant? = null, throughInclusive: Instant? = null): DocFilter =
    DocFilter.UpdatedAt(fromInclusive, throughInclusive)

internal fun DocFilter.toSql(params: MutableList<Any>): String = when (this) {
    is DocFilter.Group -> {
        if (filters.isEmpty()) {
            if (operator == BooleanOperator.AND) "TRUE" else "FALSE"
        } else {
            filters.joinToString(" $operator ", "(", ")") { it.toSql(params) }
        }
    }
    is DocFilter.Negated -> "(${filter.toSql(params)}) IS NOT TRUE"
    is DocFilter.Tags -> when {
        values.isEmpty() -> if (operator == BooleanOperator.AND) "TRUE" else "FALSE"
        else -> {
            params.add(values)
            if (operator == BooleanOperator.AND) "tags @> ?::text[]" else "tags && ?::text[]"
        }
    }
    is DocFilter.Ids -> {
        if (values.isEmpty()) "FALSE"
        else {
            params.add(values)
            "id = ANY (?::text[])"
        }
    }
    is DocFilter.UpdatedAt -> when {
        fromInclusive != null && throughInclusive != null -> {
            params.add(Timestamp.from(fromInclusive.toJavaInstant()))
            params.add(Timestamp.from(throughInclusive.toJavaInstant()))
            "updated_at BETWEEN ? AND ?"
        }
        fromInclusive != null -> {
            params.add(Timestamp.from(fromInclusive.toJavaInstant()))
            "updated_at >= ?"
        }
        throughInclusive != null -> {
            params.add(Timestamp.from(throughInclusive.toJavaInstant()))
            "updated_at <= ?"
        }
        else -> "TRUE"
    }
}
