# lami

lami is a fork of [Egor Kovetskiy](https://github.com/kovetskiy)[poke](https://github.com/kovetskiy/poke) MySQL/MariaDB slow query log parser, lami can converts slow query log into JSON format.

lami can detects following features:

- time when query was started (using `Time:` and `Query_time` fields), 
- query type: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `DROP`
- query length

### How to use
```
lami -f /pat/to/slow-query.log
```

lami will print the JSON output to stdout output

```
[
    {
        "lock_time": 0.000048,
        "query": "SET timestamp=1480443944;DELETE [...]",
        "query_length": 110,
        "query_time": 36.083807,
        "query_type": "",
        "rows_affected": 0,
        "rows_examined": 7342175,
        "rows_sent": 1,
        "schema": "dbmu",
        "time": "2019-05-16 16:06:30.00000000",
        "time_start": "2019-05-16 16:05:53.91619300"
    }
]
```

## License
MIT.
