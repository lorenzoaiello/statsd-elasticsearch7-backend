const counters = function (key, value, ts, bucket) {
  if (value) {
    bucket.push({
  		"name": key,
  		"val":value,
  		"@timestamp": ts
  	});
  	return 1;
  }

  return 0;
}

const timers = function (key, series, ts, bucket) {
  let counter = 0;
  for (keyTimer in series) {
    if (series[keyTimer]) {
      bucket.push({
    		"name": key,
    		"val":series[keyTimer],
    		"@timestamp": ts
    	});
      counter ++;
    }
  }
	return counter;
}

const timer_data = function (key, value, ts, bucket) {
  value["@timestamp"] = ts;
  value["name"]  = key;
  let shouldPush = false;
  if (value['histogram']) {
    for (var keyH in value['histogram']) {
      shouldPush = shouldPush || !!value['histogram'][keyH];
      value[keyH] = value['histogram'][keyH];
    }
    delete value['histogram'];
  }

  if (shouldPush) {
    bucket.push(value);
  }
}

exports.counters   = counters;
exports.timers     = timers;
exports.timer_data = timer_data;
exports.gauges     = counters;
