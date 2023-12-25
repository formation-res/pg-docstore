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

## Features

- document store with crud operations for storing and retrieving json documents
- update function that retrieves, applies your lambda to the retrieved document, and then stores in a transaction.
- serialization is done using kotlinx.serialization
- efficient bulk insert/updates
- efficient querying and dumping of the content of the store with database scrolls. We use this for our ETL pipeline.
- nice Kotlin API with suspend functions, flows, strong typing, etc.

This library builds on jasync-postgresql, which is one of the few database drivers out there that is written in Kotlin
and that uses non blocking IO. 

## Usage

### Connecting to the database

Pg-docstore uses jasync-postgresql to connect to Postgresql. Unlike many other
database frameworks, this framework uses non blocking IO and is co-routine friendly;
and largely written in Kotlin too. This makes it a perfect choice for pg-docstore.

```kotlin
val connection = PostgreSQLConnectionBuilder
  .createConnectionPool(
    ConnectionPoolConfiguration(
      host = "localhost",
      port = 5432,
      database = "docstore",
      username = "postgres",
      password = "secret",
    )
  ).asSuspending

// recreate the docs table
connection.reCreateDocStoreSchema("docs")
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
  connection = connection,
  serializationStrategy = MyModel.serializer(),
  tableName = "docs",
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
// update by id
store.update(doc1.id) {
  it.copy(title = "Number One")
}
// or just pass in the document
store.update(doc1) {
  it.copy(title = "Numero Uno")
}
// Retrieve it again
store.getById(doc1.id)?.let {
  println("Retrieved ${it.title}")
}

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

// and of course we can query in all sorts of ways
println("five most recent documents: ${
  store.documentsByRecency(limit = 5).map { it.title }
}")
// we can scroll through the entire table
// and count the number of documents in the flow
println(
  "Total documents: ${
    store.documentsByRecencyScrolling().count()
  }"
)

// and we can restrict the search using tags
println(
  "Just the bulk tagged documents: ${
    store
      .documentsByRecencyScrolling(
        // filters on the extracted tags
        tags = listOf("bulk")
      )
      .count()
  }"
)

// delete a document
store.delete(doc1)
println("now it's gone: ${store.getById(doc1.id)}")
```

Captured Output:

```
Retrieved Number 1
Retrieved Numero Uno
five most recent documents: [Bulk 199, Bulk 198, Bulk 197, Bulk 196, Bulk 195]
Total documents: 201
Just the bulk tagged documents: 200
now it's gone: null

```

### Text search

```kotlin

store.create(MyModel("The quick brown fox"))
// or search on the extracted text
println(
  "Found for 'fox': ${
    store.documentsByRecency(query = "fox").first().title
  }"
)
```

Captured Output:

```
Found for 'fox': The quick brown fox

```

### Transactions

Most operations in the docstore are single sql statements and don't require a transaction.

But of course we are using a proper database here and you can group operations in a transaction. 
The `transact` function creates a new docstore with it's own isolated connection that is used for the duration of 
the transaction. This ensures that it succeeds or rolls back as a whole and prevents other threads 
from sending sql commands on the same connection. We need this because connections are shared and queries
are non blocking.

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

Captured Output:

```
Both docs exist: 2 after the transaction

```

In case of an exception, there is a rollback.

```kotlin

// rollbacks happen if there are exceptions
val another = MyModel(
  title = "Another",
  description = "yet another document",
  categories = listOf("foo")
)
runCatching {
  store.transact { tStore ->
    tStore.create(another)
    tStore.update(another) {
      another.copy(title = "Modified")
    }
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

Captured Output:

```
in the transaction it exists as: Modified
after the rollback null

```

```kotlin

// if you like sql, just use the connection
store.connection
  .sendQuery("SELECT COUNT(*) as total FROM docs")
  .let { res ->
    res.rows.first().let { row ->
      println("Count query total: ${row["total"]}")
    }
  }

```

Captured Output:

```
Count query total: 203

```

This shows off most of the features. And more importantly, it shows how 
simple interactions with the database are. Mostly, it's just simple idiomatic Kotlin; nice
little one liners.

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

