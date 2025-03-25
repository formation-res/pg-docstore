package com.tryformation.pgdocstore

fun String.sanitizeInputForDB(): String {
    // Define a regular expression for disallowed characters or strings
    // For example, this regex will remove single quotes, double quotes, semicolons, and SQL comment syntax
    val disallowedPattern = """['";]|(--)|(/\*)|(\*/)|(#\s)""".toRegex()

    // Replace disallowed characters with a space
    return disallowedPattern.replace(this, " ")
}

fun String.camelCase2SnakeCase(): String {
    val re = "(?<=[a-z0-9])[A-Z]".toRegex()
    return re.replace(this) { m -> "_${m.value}" }.lowercase()
}
