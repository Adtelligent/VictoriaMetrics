See vmalert docs [here](https://docs.victoriametrics.com/victoriametrics/vmalert/).

vmalert docs can be edited at [docs/vmalert.md](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/docs/victoriametrics/vmalert.md).

## SQL datasource

`vmalert` can execute rules with `type: "sql"` against an HTTP endpoint configured via `-datasource.sql.url`.

This integration is primarily designed for ClickHouse and HTTP proxies built in front of it. `vmalert` doesn't initialize or use native database drivers directly. Instead, it sends SQL queries over HTTP and expects a compatible JSON response format.


### Query requirements

The SQL query must return exactly one numeric value column prefixed with `c_`. All other returned columns are treated as labels.

Example:

```sql
SELECT host, count() AS c_value
FROM demo.events
GROUP BY host
FORMAT JSONCompact
```
### Query requirements

```yml
groups:
  - name: sql-rules
    type: "sql"
    rules:
      - record: demo:events_total
        expr: SELECT host, count() AS c_value FROM demo.events GROUP BY host FORMAT JSONCompact
```