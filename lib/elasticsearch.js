/*
 * Flush stats to ElasticSearch (http://www.elasticsearch.org/)
 *
 * To enable this backend, include 'elastic' in the backends
 * configuration array:
 *
 *   backends: ['statsd-elasticsearch7-backend']
 *  (if the config file is in the statsd folder)
 *
 * This backend supports the following config options:
 *
 *   host:            hostname or IP of ElasticSearch server
 *   port:            port of Elastic Search Server
 *   path:            http path of Elastic Search Server (default: '/')
 *   indexPrefix:     Prefix of the dynamic index to be created (default: 'statsd')
 *   indexTimestamp:  Timestamping format of the index, either "year", "month", "day", or "hour"
 */

const http = require('http'),
      https = require('https'),
      fs = require('fs');

// this will be instantiated to the logger
let lg;
let debug;
let flushInterval;
let elasticHost;
let elasticPort;
let elasticPath;
let elasticIndex;
let elasticIndexTimestamp;
let elasticCountType;
let elasticTimerType;
let elasticUsername;
let elasticPassword;
let elasticHttps;
let elasticCertCa;

let elasticStats = {};

function transform(stats, index, statsdType) {
    const payload = [];

    for (let i = 0; i < stats.length; i++) {
        payload.push(JSON.stringify({
            index: {
                "_index": index
            }
        }));
        stats[i].type = statsdType;
        payload.push(JSON.stringify(stats[i]));
    }

    return payload;
}

function insert(listCounters, listTimers, listTimerData, listGaugeData) {
    const indexDate = new Date();
    let statsdIndex = elasticIndex + '-' + indexDate.getUTCFullYear();

    if (elasticIndexTimestamp === 'month' || elasticIndexTimestamp === 'day' || elasticIndexTimestamp === 'hour') {
        let indexMo = indexDate.getUTCMonth() + 1;
        if (indexMo < 10) {
            indexMo = '0' + indexMo;
        }
        statsdIndex += '.' + indexMo;
    }

    if (elasticIndexTimestamp === 'day' || elasticIndexTimestamp === 'hour') {
        let indexDt = indexDate.getUTCDate();
        if (indexDt < 10) {
            indexDt = '0' + indexDt;
        }
        statsdIndex += '.' + indexDt;
    }

    if (elasticIndexTimestamp === 'hour') {
        let indexDt = indexDate.getUTCHours();
        if (indexDt < 10) {
            indexDt = '0' + indexDt;
        }
        statsdIndex += '.' + indexDt;
    }

    let payload = transform(listCounters, statsdIndex, elasticCountType);
    payload = payload.concat(transform(listTimers, statsdIndex, elasticTimerType));
    payload = payload.concat(transform(listTimerData, statsdIndex, elasticTimerType));
    payload = payload.concat(transform(listGaugeData, statsdIndex, elasticGaugeDataType));

    if (payload.length === 0) {
        // No work to do
        return;
    }

    payload = payload.join("\n") + "\n";

    const optionsPost = {
        host: elasticHost,
        port: elasticPort,
        path: `${elasticPath}${statsdIndex}/_bulk`,
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': payload.length
        }
    };

    if (elasticUsername && elasticPassword) {
        optionsPost.auth = elasticUsername + ':' + elasticPassword;
    }

    let httpClient = http;
    if (elasticHttps) {
        httpClient = https;

        if (elasticCertCa) {
            optionsPost.ca = fs.readFileSync(elasticCertCa);
            optionsPost.agent = new https.Agent(optionsPost);
        }
    }

    const req = httpClient.request(optionsPost, function (res) {
        res.on('data', (d) => {
            lg.log(`ES responded with ${res.statusCode}`);
            if (res.statusCode >= 400) {
                let errdata = "HTTP " + res.statusCode + ": " + d;
                lg.log('error', errdata);
            }
        });
    }).on('error', function (err) {
        lg.log('error', 'Error with HTTP request, no stats flushed.');
        console.log(err);
    });

    if (debug) {
        lg.log('ES payload:');
        lg.log(payload);
    }

    req.write(payload);
    req.end();
}

const flush_stats = function elastic_flush(ts, metrics) {
  let numStats = 0;
  let key;
  let array_counts = new Array();
  let array_timers = new Array();
  let array_timer_data = new Array();
  let array_gauges = new Array();

  ts = ts * 1000;

  for (key in metrics.counters) {
    numStats += fm.counters(key, metrics.counters[key], ts, array_counts);
  }

  for (key in metrics.timers) {
    numStats += fm.timers(key, metrics.timers[key], ts, array_timers);
  }

  if (array_timers.length > 0) {
    for (key in metrics.timer_data) {
      fm.timer_data(key, metrics.timer_data[key], ts, array_timer_data);
    }
  }

  for (key in metrics.gauges) {
    numStats += fm.gauges(key, metrics.gauges[key], ts, array_gauges);
  }

  if (debug) {
    lg.log('metrics:');
    lg.log(JSON.stringify(metrics));
  }

  insert(array_counts, array_timers, array_timer_data, array_gauges);

  if (debug) {
    lg.log("debug", "flushed " + numStats + " stats to ES");
  }
};

const elastic_backend_status = function (writeCb) {
  for (let stat in elasticStats) {
    writeCb(null, 'elastic', stat, elasticStats[stat]);
  }
};

exports.init = function (startup_time, config, events, logger) {

    debug = config.debug;
    lg = logger;

    let configEs = config.elasticsearch || {};

    elasticHost = configEs.host || process.env.ES_HOST || 'localhost';
    elasticPort = configEs.port || process.env.ES_PORT || 9200;
    elasticPath = configEs.path || process.env.ES_PATH || '/';
    elasticIndex = configEs.indexPrefix || process.env.ES_INDEX_PREFIX || 'statsd';
    elasticIndexTimestamp = configEs.indexTimestamp || process.env.ES_INDEX_TIMESTAMP || 'day';
    elasticCountType = configEs.countType || process.env.ES_COUNT_TYPE || 'counter';
    elasticTimerType = configEs.timerType || process.env.ES_TIMER_TYPE || 'timer';
    elasticTimerDataType = configEs.timerDataType || process.env.ES_TIMER_DATATYPE || elasticTimerType + '_stats';
    elasticGaugeDataType = configEs.gaugeDataType  || process.env.ES_GAUGE_DATATYPE || 'gauge';
    elasticFormatter = configEs.formatter || process.env.ES_FORMATTER || 'default_format';
    elasticUsername = configEs.username  || process.env.ES_USERNAME || undefined;
    elasticPassword = configEs.password || process.env.ES_PASSWORD || undefined;
    elasticHttps = configEs.secure || process.env.ES_SECURE || false;
    elasticCertCa = configEs.ca || undefined;

    fm = require('./' + elasticFormatter + '.js');
    if (debug) {
        lg.log("debug", "loaded formatter " + elasticFormatter);
    }

    if (fm.init) {
        fm.init(configEs);
    }
    flushInterval = config.flushInterval;

    elasticStats.last_flush = startup_time;
    elasticStats.last_exception = startup_time;


    events.on('flush', flush_stats);
    events.on('status', elastic_backend_status);

    return true;
};
