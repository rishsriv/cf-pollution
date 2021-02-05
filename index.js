// use a format like the one below to handle cronjobs
// addEventListener("scheduled", event => {
//   event.waitUntil(handleScheduled(event))
// })

addEventListener('fetch', event => {
  const request = event.request;
  if (request.method === "OPTIONS") {
    //handle CORS
    event.respondWith(handleOptions(request));
  } else if(request.method === "GET" || request.method === "HEAD" || request.method === "POST") {
    // Handle requests to the API server
    event.respondWith(handleRequest(request));
  } else {
    event.respondWith(
      new Response(null, {status: 405, statusText: "Method Not Allowed"}),
    )
  }
})

async function readRequestJSON(request) {
  const headers = request.headers;
  const contentType = headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    return request.json();
  }
  else {}
}

/**
 * Returns the months between two dates
 * @param {startDate} int  A date in the YYYYMMDD format
 * @param {endDate} int A date in the YYYYMMDD format
 * @return {[string]} an array of strings in the YYYYMM format
 */
const getMonthsInBetween = function(startDate, endDate) {
  const startYear = parseInt(startDate/10000);
  const endYear = parseInt(endDate/10000);
  const startMonth = parseInt((startDate%10000)/100);
  const endMonth = parseInt((endDate%10000)/100);
  let dates = [];

  for(let year = startYear; year <= endYear; year++) {
    let startAtMonth = (year == startYear) ? startMonth : 1;
    let endAtMonth = (year == endYear) ? endMonth : 12;

    for(let month = startAtMonth; month <= endAtMonth; month++) {
      let displayMonth = (month < 10) ? "0" + String(month) : String(month);
      dates.push(String(year) + displayMonth);
    }
  }
  return dates;
}

/**
 * Returns the mean data for a given set of values, grouped by a date resolution
 * @param {values} [[timestamp, value], ... ]  An array of values in the [timestamp, value] format
 * @param {resolution} str one of ["h", "d", or "m"] TODO maybe add week in the future
 * @return {[[timestamp, pollution_val], ... ] } an array of arrays
 */
const aggregateDateTime = function(values, resolution) {
  //First, convert all timestamps into the right resolution of data
  if (resolution == "d") { //transform the timestamp in values to the first day
    values = values.map(
      item => [new Date(item[0]).setUTCHours(0,0,0,0), item[1]]);
  } else if (resolution == "m") { //transform the timestamp in values to the first of the month
    values = values.map(
      item => [new Date(item[0]).setUTCHours(0,0,0,0), item[1]]
    ).map(
      item => [new Date(item[0]).setDate(1), item[1]]
    );
  } else {
    return values;
  }

  //Then, aggregate all the data
  //TODO really figure out why this works. I wrote this, but I don't really understand it
  let result = values.reduce(function(accumulator, item) {
    if (!accumulator[item[0]]) {
      accumulator[item[0]] = [0, 0];
    }
    accumulator[item[0]][0] += item[1];
    accumulator[item[0]][1] += 1;
    return accumulator
  }, {});

  result = Object.keys(result).map(key => [parseInt(key), parseInt(result[key][0]/result[key][1])]);
  return result;
}

/**
 * Returns the requested pollution data for a city
 * @param {city} str  The city name
 * @param {metric} str Metric name ("PM2.5", "PM10", "NO2", "O3", or "CO")
 * @param {timeFrom} int Time from which the data should start in the YYYYMMDD format
 * @param {timeTo} int Time till which the data should go on to in the YYYYMMDD format
 * @param {resolution} str Resolution of results. One of "h" (hour), "d" (day), or "m" (month)
 * @return {[city, [[timestamp, pollution_val], ... ]]} an array of arrays
 */
async function getCityDets(cities, keys, timeFrom, timeTo, resolution) {
  //then, get the keys -- TODO can this be parallelized?
  let kvValues = [];
  for (const key of keys) {
    kvValues.push(POLLUTION_CITY.get(key, "text"));
  }
  kvValues = await Promise.all(kvValues);
  for (idx in kvValues) {
    if (kvValues[idx] != null) {
      kvValues[idx] = kvValues[idx].split(";").map(val => val.split(","));
    } else {
      kvValues[idx] = [];
    }
  }
  //kvValues = kvValues.map(item => item.split(";").map(val => val.split(",")));

  let cityDets = {};
  for (const city of cities) {
    let finDets = [];
    for (const idx in keys) {
      if (keys[idx].split("_")[0] == city) {
        let det = kvValues[idx];
        if (det != null) {
          finDets = finDets.concat(det.map(item => [1000*parseInt(item[0]), parseInt(item[1]) ]));
        }
      }
    }

    if (finDets.length > 0) {
      finDets = finDets.filter(item => parseInt(item[0]) <= timeTo + 24*3600*1000 && parseInt(item[0]) >= timeFrom);
      finDets = aggregateDateTime(finDets, resolution);
    }
    cityDets[city] = finDets;
  }
  return cityDets;
}

async function isValidRequest(userEmail, hash) {
  try {
    const userDets = await USERS.get(userEmail, "json");
    if (userDets.hash === hash) {
      return true;
    } else {
      return false;
    }
  } catch (err) {
    console.log("Error connecting with the USERS table");
    return false;
  }
}

/**
 * Respond to the request
 * @param {Request} request
 */
async function handleRequest(request) {
  const headers = request.headers;
  //before doing anything, check if the request had valid credentials
  //check for X-Auth-Token and X-Auth-Email in requests. If they're absent or invalid, return a 403 forbidden
  //else, continue to process the request as you would do otherwise
  
  const userEmail = headers.get("x-auth-email");
  const userHash = headers.get("x-auth-token");
  const isAuthenticated = await isValidRequest(userEmail, userHash);
  if (isAuthenticated === false) {
    let response = new Response("Invalid Credentials", {
      status: 403,
      headers: {
        'Content-Type': "application/text",
        'Access-Control-Allow-Origin': '*'
      }
    });
    return response;
  }

  const reqBody = await readRequestJSON(request);
  
  //first, get all necessary variables
  let {
    cities, //an array of cities
    timeFrom, //in the YYYYMMDD format
    timeTo, //in the YYYYMMDD format
    resolution, //one of "h", "d", "w", or "m"
    metric, //one of "PM2.5", "PM10", "NO2", "O3", or "CO"
    reloadMonth, //optional. Only there for requests where we want to reload the month
    metrics //optional.  Only there for requests where we want to reload the month
  } = reqBody;
  
  // let cities = reqBody.cities;  //an array of cities
  // const timeFrom = reqBody.timeFrom; //in the YYYYMMDD format
  // const timeTo = reqBody.timeTo; //in the YYYYMMDD format
  // const resolution = reqBody.resolution; //one of "h", "d", "w", or "m"
  // const metric = reqBody.metric; //one of "PM2.5", "PM10", "NO2", "O3", or "CO"
  // const reloadMonth = reqBody.reloadMonth; //optional. Only there for requests where we want to reload the month
  // const metrics = reqBody.metrics;//optional.  Only there for requests where we want to reload the month

  if (reloadMonth !== undefined) {
    //this is a request asking us to update the month data
    const formatDate = d => [d.getFullYear(), (d.getMonth() + 1).toString().padStart(2, '0')].join('');
    const curMonth = formatDate(new Date);

    for (const city of cities) {
      for (const metric of metrics) {
        const key = city + "_" + curMonth + "_" + metric;
        let hourlyValue = await POLLUTION_CITY.get(key, "text");
        hourlyValue = hourlyValue.split(";").map(val => [1000*parseInt(val.split(",")[0]), parseInt(val.split(",")[1])]);
        let monthlyValue = aggregateDateTime(hourlyValue, "d");
        monthlyValue = monthlyValue.map(val => [String(val[0]/1000), String(val[1])].join(","));
        monthlyValue = monthlyValue.join(";");
        const newKey = key + "_d";
        POLLUTION_CITY.put(newKey, monthlyValue);
      }
    }
    let response = new Response("addin yer data", {
      status: 200,
      headers: {
        'Content-Type': "application/json",
        'Access-Control-Allow-Origin': '*'
      }
    });
    return response;
  } else {
    //this is a normal request
    cities = cities.map(item => item.toLowerCase());
    //second, determine what time periods to get values from KV for. Then, determine the keys to query
    const dates = getMonthsInBetween(parseInt(timeFrom), parseInt(timeTo));
    let keysToQuery = [];
    for (const city of cities) {
      for (const date of dates) {
        if (resolution == "h") {
          keysToQuery.push(city + "_" + date + "_" + metric);
        } else {
          keysToQuery.push(city + "_" + date + "_" + metric + "_d");
        }
      }
    }

    //third, convert timeFrom and timeTo into epoch times
    const fromYear = parseInt(timeFrom/10000);
    const fromMonth = parseInt((timeFrom%10000)/100);
    const fromDate = timeFrom%100;

    const toYear = parseInt(timeTo/10000);
    const toMonth = parseInt((timeTo%10000)/100);
    const toDate = timeTo%100;

    const timeFromEpoch = parseInt(Date.UTC(fromYear, fromMonth - 1, fromDate));
    const timeToEpoch = parseInt(Date.UTC(toYear, toMonth - 1, toDate));

    let cityDets = await getCityDets(cities, keysToQuery, timeFromEpoch, timeToEpoch, resolution);
    
    //finally, send a request back
    let respData = JSON.stringify(cityDets);
    let response = new Response(respData, {
      status: 200,
      headers: {
        'Content-Type': "application/json",
        'Access-Control-Allow-Origin': '*'
      }
    });
    return response;
  }

  
}

async function handleOptions(request) {
  let headers = request.headers;
  if (
    headers.get("Origin") !== null &&
    headers.get("Access-Control-Request-Method") !== null &&
    headers.get("Access-Control-Request-Headers") !== null
  ) {
    let respHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": request.headers.get("Access-Control-Request-Headers"),
    }

    return new Response(null, {
      headers: respHeaders,
    })
  }
  else {
    return new Response(null, {
      headers: {
        "Allow": "POST, OPTIONS",
      },
    })
  }
}