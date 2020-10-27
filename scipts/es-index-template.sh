curl -XPUT -H 'Content-Type: application/json' "${ES_HOST:-localhost}:${ES_PORT:-9200}/_template/statsd-template" -d '
{
    "index_patterns" : ["statsd-*"],
    "settings" : {
        "number_of_shards" : 1
    },
    "mappings" : {
        "_source" : { "enabled" : true },
        "properties" : {
            "type": {
                "type": "keyword"
            },
            "@timestamp": {
                "type": "date"
            },
            "val": {
                "type": "double",
                "index": "true"
            },
            "name": {
                "type": "keyword",
                "index": "true"
            },
            "count_ps": {
                "type": "float",
                "index": "true"
            },
            "count": {
                "type": "float",
                "index": "true"
            },
            "upper": {
                "type": "float",
                "index": "true"
            },
            "lower": {
                "type": "float",
                "index": "true"
            },
            "mean": {
                "type": "float",
                "index": "true"
            },
            "median": {
                "type": "float",
                "index": "true"
            },
            "std": {
                "type": "float",
                "index": "true"
            },
             "sum": {
                "type": "float",
                "index": "true"
            },
            "mean_90": {
                "type": "float",
                "index": "true"
            },
            "upper_90": {
                "type": "float",
                "index": "true"
            },
            "sum_90": {
                "type": "float",
                "index": "true"
            },
            "bin_100": {
                "type": "integer",
                "index": "true"
            },
            "bin_500": {
                "type": "integer",
                "index": "true"
            },
            "bin_1000": {
                "type": "integer",
                "index": "true"
            },
            "bin_inf": {
                "type": "integer",
                "index": "true"
            }
        }
    }
}
'
