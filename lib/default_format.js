
var counters = function (key, value, ts, bucket) {
  bucket.push({
		"name": key
		"val":value,
		"@timestamp": ts
	});
	return 1;
}

var timers = function (key, series, ts, bucket) {
    for (keyTimer in series) {
      bucket.push({
    		"name": key,
    		"val":series[keyTimer],
    		"@timestamp": ts
    	});
    }
	return series.length;
}

var timer_data = function (key, value, ts, bucket) {
    value["@timestamp"] = ts;
    value["name"]  = key;
    if (value['histogram']) {
      for (var keyH in value['histogram']) {
        value[keyH] = value['histogram'][keyH];
      }
      delete value['histogram'];
    }
    bucket.push(value);
}

exports.counters   = counters;
exports.timers     = timers;
exports.timer_data = timer_data;
exports.gauges     = counters;
