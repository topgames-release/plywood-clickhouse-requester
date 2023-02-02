# plywood-clickhouse-requester

<b>Non-Plywood official!!!!</b>

This is the [ClickHouse](https://clickhouse.com/) requester making abstraction layer for [plywood](https://github.com/implydata/plywood).

Given a ClickHouse query and an optional context it return a promise that resolves to the data table.

## Installation

To install run:

```
npm install @topgames/plywood-clickhouse-requester
```

## Usage

In the raw you could use this library like so:

```
import { clickHouseRequesterFactory, TZRowResultHandler } from 'plywood-clickhouse-requester';

clickHouseRequester = clickHouseRequesterFactory({
  host: 'my.clickhouse.host',
  database: 'all_my_data',
  user: 'default',
  password: 'password',
  rowResultHandler: TZRowResultHandler
})

clickHouseRequester({
  query: 'SELECT COUNT(*) AS `__VALUE__` FROM `trips` GROUP BY `__VALUE__`'
})
  .then(function(stream) {
    // readable stream
  })
```

`TZRowResultHandler` is optional. it can help you convert DateTime values to T/Z format (it's a string). clickHouse can't output time values in T/Z format by default.

## Tests

Currently the tests run against a real ClickHouse database that should be configured (database, user, password) the same as
what is indicated in `test/info.js`.
