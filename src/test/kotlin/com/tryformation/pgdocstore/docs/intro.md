[![Process Pull Request](https://github.com/formation-res/pg-docstore/actions/workflows/pr_master.yaml/badge.svg)](https://github.com/formation-res/pg-docstore/actions/workflows/pr_master.yaml)

Pg-docstore is a kotlin library that allows you to use postgres as a json document store from Kotlin.

## Why

Document stores are very useful in some applications and while popular in the nosql world, sql databases like
postgres provide a lot of nice functionality ant performance and is therefore a popular choice for data storage.
Using postgres as a document store makes a lot of sense. Transactionality ensures that your data is safe. You can use any
of a wide range of managed and cloud based service providers or just host it yourself.

At FORMATION, we use pg-docstore to store millions of documents of different types. We use Elasticsearch for querying, aggregations, and other functionality and therefore have no need
for the elaborate querying support in databases. But we do like to keep our data safe, which is why we like postgres.
Additionally, we like having a clear separation between what we store and what we query on. So, our architecture includes
an ETL pipeline that builds our search index from the raw data in pg-docstore.

## Gradle

Add the `maven.tryformation.com` repository:

```kotlin
repositories {
    mavenCentral()
    maven("https://maven.tryformation.com/releases") {
        content {
            includeGroup("com.tryformation")
        }
    }
}
```

And then the dependency to commonsMain or main:

```kotlin
    // check the latest release tag for the latest version
    implementation("com.tryformation:pg-docstore:1.x.y")
```

## Features

- document store with crud operations for storing and retrieving json documents
- update function that retrieves, applies your lambda to the retrieved document, and then stores in a transaction.
- serialization is done using kotlinx.serialization
- efficient bulk insert/updates
- efficient querying and dumping of the content of the store with database scrolls. We use this for our ETL pipeline.
- nice Kotlin API with suspend functions, flows, strong typing, etc.

This library builds on JDBC and the postgresql driver for that together with HikariCP and Java's virtual threads.

