package com.tryformation.pgdocstore

import kotlinx.serialization.json.JsonPrimitive
import kotlin.time.Instant

/** Values to compare. A `*` path segment expands a JSON array; other segments are literal keys. */
sealed interface DocStringSource {
    data object StorageId : DocStringSource
    data object ExtractedTags : DocStringSource
    data object ExtractedText : DocStringSource
    data class Document(val path: List<String>) : DocStringSource {
        init { require(path.isNotEmpty()) }
    }
}

enum class DocStringMatch { Exact, Prefix, Wildcard }
enum class DocComparison(internal val sql: String) { GreaterThan(">"), AtLeast(">="), LessThan("<"), AtMost("<=") }

/**
 * Matches any value from [source]. Wildcards use `*` and `?`; a backslash escapes the next character.
 * Exact extracted-tag matching uses the tags GIN operators; [tagsAll] requires all values.
 * Case-insensitive extracted-text matching uses ILIKE so the existing trigram index remains eligible.
 * To use text as a candidate filter for tag matching, callers must guarantee that their text extractor
 * includes the queried tags and combine it with a per-tag predicate. Storage IDs should use [idsIn].
 * Document paths preserve payload semantics but need a suitable index or a selective enclosing filter.
 * [excludedPrefixes] applies to individual values, not to the whole document.
 * [matchMissing] includes null source values, for example extracted text on legacy rows.
 * A document path producing no values still does not match.
 */
fun stringsMatch(
    source: DocStringSource,
    values: Collection<String>,
    match: DocStringMatch = DocStringMatch.Exact,
    ignoreCase: Boolean = false,
    excludedPrefixes: Collection<String> = emptyList(),
    matchMissing: Boolean = false,
): DocFilter = DocFilter.Strings(source, values.toList(), match, ignoreCase, excludedPrefixes.toList(), matchMissing)

/** Compares a timestamp in the payload, independently of the store's modification timestamp. */
fun documentTime(path: List<String>, comparison: DocComparison, value: Instant): DocFilter {
    require(path.isNotEmpty())
    return DocFilter.DocumentTime(path.toList(), comparison, value)
}

internal fun stringPredicateSql(filter: DocFilter.Strings, params: MutableList<Any>): String {
    if (filter.values.isEmpty()) return "FALSE"
    if (filter.source == DocStringSource.ExtractedTags && filter.match == DocStringMatch.Exact &&
        !filter.ignoreCase && !filter.matchMissing) {
        val visible = filter.values.filterNot { value -> filter.excludedPrefixes.any(value::startsWith) }
        if (visible.isEmpty()) return "FALSE"
        params.add(visible)
        return "tags && ?::text[]"
    }
    val scalar = when (filter.source) {
        DocStringSource.StorageId -> "id"
        DocStringSource.ExtractedText -> "text"
        else -> null
    }
    val source = when (val value = filter.source) {
        DocStringSource.StorageId -> "(SELECT id AS v) AS values_source"
        DocStringSource.ExtractedTags -> "unnest(tags) AS values_source(v)"
        DocStringSource.ExtractedText -> "(SELECT text AS v) AS values_source"
        is DocStringSource.Document -> {
            val path = "$" + value.path.joinToString("") {
                if (it == "*") "[*]" else ".${JsonPrimitive(it)}"
            }
            params.add(path)
            "(SELECT value #>> '{}' AS v FROM jsonb_path_query(json::jsonb, ?::jsonpath) AS values_json(value)) AS values_source"
        }
    }
    val patterns = filter.values.map { value ->
        when (filter.match) {
            DocStringMatch.Exact -> if (filter.ignoreCase) escapeLike(value) else value
            DocStringMatch.Prefix -> escapeLike(value) + "%"
            DocStringMatch.Wildcard -> wildcardPattern(value)
        }
    }
    val valueExpression = scalar ?: "v"
    val like = filter.match != DocStringMatch.Exact || filter.ignoreCase
    val operator = if (filter.ignoreCase) "ILIKE" else "LIKE"
    val checks = when {
        like && patterns.size == 1 -> {
            params.add(patterns.single())
            "$valueExpression $operator ? ESCAPE E'\\\\'"
        }
        like -> {
            params.add(patterns)
            // LIKE/ILIKE ANY uses PostgreSQL's default backslash escape, matching the single-pattern form.
            "$valueExpression $operator ANY (?::text[])"
        }
        else -> {
            params.add(patterns)
            "$valueExpression = ANY (?::text[])"
        }
    }
    val exclusions = filter.excludedPrefixes.joinToString("") {
        params.add(it)
        " AND NOT starts_with($valueExpression, ?)"
    }
    val missing = if (filter.matchMissing) "$valueExpression IS NULL OR " else ""
    val predicate = "($missing$checks)$exclusions"
    return if (scalar != null) predicate else "EXISTS (SELECT 1 FROM $source WHERE $predicate)"
}

private fun escapeLike(value: String) = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
private fun wildcardPattern(value: String): String = buildString {
    var escaped = false
    for (character in value) {
        if (escaped) { append(escapeLike(character.toString())); escaped = false }
        else when (character) {
            '\\' -> escaped = true
            '*' -> append('%')
            '?' -> append('_')
            else -> append(escapeLike(character.toString()))
        }
    }
    if (escaped) append("\\\\")
}

/** Ordering for filtered reads. Timestamp paths reference the payload, not storage metadata. */
sealed interface DocSort {
    data class StorageId(val ascending: Boolean = true) : DocSort
    data class DocumentTime(val path: List<String>, val ascending: Boolean = true) : DocSort {
        init { require(path.isNotEmpty()) }
    }
}

internal fun docSortSql(order: List<DocSort>, params: MutableList<Any>): String =
    (order.map { field ->
        when (field) {
            is DocSort.StorageId -> "id ${if (field.ascending) "ASC" else "DESC"} NULLS LAST"
            is DocSort.DocumentTime -> {
                params.add(field.path)
                "(json::jsonb #>> ?::text[])::timestamptz ${if (field.ascending) "ASC" else "DESC"} NULLS LAST"
            }
        }
    } + "id ASC").joinToString()
