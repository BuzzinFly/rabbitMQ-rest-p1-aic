// Scripted REST: SEARCH / QUERY script
// Purpose: poll facade and return event objects for reconciliation

var baseUrl = configuration.facadeBaseUrl;     // e.g. "https://facade.example.com"
var apiKey  = configuration.facadeApiKey;
var limit   = configuration.limit || 100;
var lease   = configuration.leaseSeconds || 60;

function http(method, path, body) {
  var req = {
    method: method,
    uri: baseUrl + path,
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": apiKey
    }
  };
  if (body !== null && body !== undefined) {
    req.entity = JSON.stringify(body);
  }

  var resp = request(req);

  if (resp.status === 204) {
    return { status: 204, json: null };
  }
  if (resp.status < 200 || resp.status >= 300) {
    throw "HTTP " + resp.status + " calling " + path + " body=" + (resp.entity || "");
  }
  return { status: resp.status, json: resp.entity ? JSON.parse(resp.entity) : null };
}

// Poll facade
var poll = http("GET", "/v1/events/users?limit=" + limit + "&leaseSeconds=" + lease, null);

if (poll.status === 204) {
  // No results in this cycle
  // Return an empty list / empty result set (connector framework will accept this)
  [];
} else {
  var batchId = poll.json.batchId;
  var events = poll.json.events || [];

  // Return “accounts” to IDM
  // _id MUST be unique; use facade event id (receipt id)
  var results = events.map(function(evt) {
    return {
      _id: evt.id,
      batchId: batchId,
      data: evt.data
    };
  });

  results;
}