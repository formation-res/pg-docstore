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
