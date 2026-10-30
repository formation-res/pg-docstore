# Pg Doc Store 

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

## Building and testing

Java 25 or newer and Docker Compose are required. The library targets Java 25;
local builds can run on JDK 26. CI tests JDK 25 and 26 with PostgreSQL 18,
and JDK 25 with PostgreSQL 16.

```sh
./gradlew build dokkaJar
```

Tests start a temporary PostgreSQL 18 container on `localhost:55432` and stop it
afterward. To use a database you started yourself:

```sh
./gradlew check -PuseExistingPostgres=true -PtestJdbcUrl=jdbc:postgresql://localhost:55432/docstore
```

The test database must be named `docstore`, with user `postgres` and password
`secret`. Tests create and drop tables; use a dedicated test database.
Dependency and plugin versions are in `gradle/libs.versions.toml`.

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


## Usage

### Connecting to the database

Pg-docstore uses the jdbc posgresql driver to connect to Postgresql and virtual threads to make that non blocking.                 

```kotlin
// uses HikariCP but should work with any postgres datasource
val connectionPool = connectionPool(
  jdbcUrl = "jdbc:postgresql://localhost:5432/docstore",
  username = "postgres",
  password = "secret",
)
connectionPool.dropDocStoreTable(tableName)
connectionPool.createDocStoreTable(tableName)

```

The `reCreateDocStoreSchema` call applies the docstore table schema to a docs table and re-creates that.

### Creating a store

We'll use the following simple data class as our model. It's annotated with `@Serializable`. This enables
model serialization using kotlinx.serialization.

```kotlin
@Serializable
data class MyModel(
  val title: String,
  val description: String? = null,
  val categories: List<String> = listOf(),
  val id: String = UUID.randomUUID().toString(),
)
```

Using this data class, we can now create a store.

```kotlin
val store = DocStore(
  dataSource = connectionPool,
  serializationStrategy = MyModel.serializer(),
  tableName = tableName,
  idExtractor = MyModel::id,
  // optional, used for text search
  textExtractor = {
    "${it.title} ${it.description}"
  },
  // optional, used for tag search
  tagExtractor = {
    it.categories
  }
)
```

The call includes three optional arguments. The optional `idExtractor` extracts an id from your model,
the default implementation simply looks for an `id` property by name but using a property reference is 
slightly more efficient.

To facilitate querying by plain text (using the text search facilities in postgresql) or tags, 
there are the textExtractor and tagExtractor fields. This allows you to search across your 
models without requiring a lot of knowledge about the internal structure of the json. The default 
implementations simply return `null`. 

In the example, we simply concatenate the title and description to enable text search and we use the 
content of the categories field to enable tag search.

### Create, read, update, and delete (CRUD) and querying

```kotlin

// do some crud
val doc1 = MyModel("Number 1", "a first document", categories = listOf("foo"))
store.create(doc1)

store.getById(doc1.id)?.let {
  println("Retrieved ${it.title}")
}

// you can only create the same id once
try {
  store.create(doc1)
} catch (e: Exception) {
  // fails
  println("id already exists")
}

// you can force the conflict be handled
// by overwriting the original
store.create(
  doc1.copy(title = "Numero Uno"),
  onConflictUpdate = true
)

// now it is changed
store.getById(doc1.id)?.let {
  println("Changed title ${it.title}")
}

// This is a better way to do updates
// update by id and modify the retrieved original
store.update(doc1.id) { original ->
  // modify the original
  original.copy(title = "Number One")
}

// you can also do it like this
store.update(doc1) { original ->
  // still fetches the original
  original.copy(title = "This is the one")
}

store.getById(doc1.id)?.let {
  println("Now it is ${it.title}")
}

// delete a document
store.delete(doc1)
println("now it's gone: ${store.getById(doc1.id)}")

```

This prints:

```text
Retrieved Number 1
id already exists
Changed title Numero Uno
Now it is This is the one
now it's gone: null
```

### Multi-get

You can get multiple documents in one go like this:

```kotlin
store.create(MyModel(id = "1", title = "Foo"))
store.create(MyModel(id = "2", title = "Bar"))
val docs =
  store.multiGetById(listOf("1", "2"))
println(docs.map { it.title })
```

This prints:

```text
[Foo, Bar]
```

### Bulk inserting documents

Inserting documents one at the time is not
very efficient. If you have large amounts
of documents, you should use bulkInsert.

This works with lists or flows. If you use
scrolling searches, which return a flow,
you can efficiently update large amounts
of documents in one go.

```kotlin

// you can also do bulk inserts using flows or lists
flow {
  repeat(200) { index ->
    emit(
      MyModel(
        title = "Bulk $index",
        description = "A bulk inserted doc #$index",
        categories = listOf("bulk")
      )
    )
    // ensure the documents don't have the same timestamp
    delay(1.milliseconds)
  }
}.let { f ->
  // bulk insert 40 at a time
  store.bulkInsert(flow = f, chunkSize = 40)
}
```

### Querying

Querying is a bit limited in pg-docstore; it's meant to 
be used in combination with things like opensearch or
elasticsearch.

However, there are a few ways in which you can get documents
out of the store.

```kotlin

// documents are sorted by recency
println(
  "five most recent documents: ${
    store.documentsByRecency(limit = 5).map { it.title }
  }")
// we can also scroll through the entire table
// and count the number of documents in the flow
println(
  "Total documents: ${
    // returns a Flow<T>
    store.documentsByRecencyScrolling(
      // fetch pages of 5 rows
      fetchSize = 5
    ).count()
  }"
)
// of course you can also do a select count(*) ...
println(store.count())

// The search can be restricted using tags
// you need to set up the tagExtractor functino
// in the DocStore params for this to work
println(
  "Just the bulk tagged documents: ${
    store.documentsByRecencyScrolling(
      // filters on the extracted tags
      tags = listOf("bulk")
    ).count()
  }"
)

```

This prints:

```text
five most recent documents: [Bulk 170, Bulk 178, Bulk 163, Bulk 194, Bulk 167]
Total documents: 202
202
Just the bulk tagged documents: 200
```

While no substitute for a proper search engine, postgres has some
text search facilities. We use a simple trigram index which is great
for common use cases for things like matching and ranking on ids,
email addresses, parts of words, etc.

```kotlin

store.create(MyModel("The quick brown fox"))
// or search on the extracted text
println("Found:")
store.documentsByRecency(
  query = "brown fox",
  // will produce a lot of hits
  // increase for stricter match
  similarityThreshold = 0.01,
  limit = 5
).forEach {
  println("-${it.title}")
}
```

This prints:

```text
Found:
-The quick brown fox
-Foo
-Bar
-Bulk 3
-Bulk 1
```

### Transactions

Single-statement writes commit automatically. Use `transact` to group operations atomically.
Updates lock the document row before running your callback and join the enclosing transaction.
Bulk inserts commit each chunk independently unless explicitly wrapped in `transact`.

The `transact` function supplies a store bound to one connection. Use that store for all operations
in the transaction, sequentially, and collect any flows before returning. Nested calls reuse the
transaction without creating savepoints. Keep update callbacks short because they hold a row lock.

JDBC statements respond to coroutine cancellation, and transactions check cancellation before committing.
Cancellation or a connection failure during commit can still leave the outcome uncertain; retries need
to be idempotent.

```kotlin
store.transact { tStore ->
  // everything you do with tStore is
  // one big transaction
  tStore.create(
    MyModel(
      title = "One more",
      categories = listOf("transaction1")
    )
  )
  tStore.create(
    MyModel(
      title = "And this one too",
      categories = listOf("transaction1")
    )
  )
}
println(
  "Both docs exist: ${
    store.documentsByRecency(tags = listOf("transaction1")).count()
  } after the transaction"
)
```

This prints:

```text
Both docs exist: 2 after the transaction
```

In case of an exception, there is a rollback.

```kotlin
// rollbacks happen if there are exceptions
val another = MyModel(
  title = "Transactional",
  description = "yet another document",
  categories = listOf("foo")
)
runCatching {
  store.transact { tStore ->
    tStore.create(another)

    println(
      "in the transaction it exists as: ${
        tStore.getById(another.id)?.title
      }"
    )
    // simulate an error
    // causes a rollback and rethrows
    error("oopsie")
  }
}
// prints null because the transaction was rolled back
println("after the rollback ${store.getById(another.id)?.title}")
```

This prints:

```text
in the transaction it exists as: Transactional
after the rollback null
```

## Scrolling and re-extraction

Scrolling uses a PostgreSQL cursor with a positive fetch size. The flow holds a connection and transaction
for the duration of collection, including time spent waiting for a slow consumer. Completion and cancellation
close the cursor and release an owned connection. Inside `transact`, the flow reuses the transaction's connection.

`reExtract()` refreshes tags and searchable text in bounded pages. It preserves document JSON, explicit row ids,
and creation timestamps. Refreshed rows receive a new `updated_at` timestamp. Concurrent updates and deletions
take precedence; rows whose JSON changed after being read are skipped.
Run it again if those rows also need refreshing. Wrap it in `transact` when the entire refresh must be atomic.

## Future work

As FORMATION grows, we will no doubt need more features. Some features that come to mind are sharding, utilizing some of
the json features in postgres, geospatial features, or other featutes.

## Development status

This is a relatively new project; so there may be some bugs, design flaws, etc. However, I've implemented similar
stores many times before in past projects and I think I know what I'm doing. If it works for us, it might also work 
for you.

Give it a try!

## License and contributing

The code is provided as is under the [MIT](LICENSE.md). If you are planning to make a contribution, please
reach out via the issue tracker first.

This readme is generated with [kotlin4example](https://github.com/jillesvangurp/kotlin4example)

