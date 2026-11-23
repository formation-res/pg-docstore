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
 * Exact indexed tags should use [tagsAny] or [tagsAll], and storage IDs should use [idsIn].
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
            DocStringMatch.Exact -> value
            DocStringMatch.Prefix -> escapeLike(value) + "%"
            DocStringMatch.Wildcard -> wildcardPattern(value)
        }
    }
    val valueExpression = scalar ?: "v"
    val left = if (filter.ignoreCase) "lower($valueExpression)" else valueExpression
    val checks = when {
        // Keep a single LIKE directly on the extracted text column so its trigram index remains usable.
        patterns.size == 1 && filter.match != DocStringMatch.Exact -> {
            params.add(patterns.single())
            val right = if (filter.ignoreCase) "lower(?)" else "?"
            "$left LIKE $right ESCAPE E'\\\\'"
        }
        filter.match == DocStringMatch.Exact && !filter.ignoreCase -> {
            params.add(patterns)
            "$left = ANY (?::text[])"
        }
        else -> {
            // One array parameter avoids JDBC's parameter limit for large alternative groups.
            params.add(patterns)
            val right = if (filter.ignoreCase) "lower(pattern)" else "pattern"
            val comparison = if (filter.match == DocStringMatch.Exact) "$left = $right"
                else "$left LIKE $right ESCAPE E'\\\\'"
            "EXISTS (SELECT 1 FROM unnest(?::text[]) AS patterns(pattern) WHERE $comparison)"
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
