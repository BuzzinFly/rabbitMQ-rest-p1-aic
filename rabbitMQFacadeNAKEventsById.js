// Scripted REST: UPDATE script (optional)
// Purpose: NACK/requeue event

var baseUrl = configuration.facadeBaseUrl;
var apiKey  = configuration.facadeApiKey;

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
  if (resp.status < 200 || resp.status >= 300) {
    throw "HTTP " + resp.status + " calling " + path + " body=" + (resp.entity || "");
  }
  return resp.entity ? JSON.parse(resp.entity) : null;
}

var eventId = id;

// Depending on how your connector passes the object content into the update script,
// you may have `object` or `newObject`. We'll assume `newObject`.
var batchId = (newObject && newObject.batchId) ? newObject.batchId : null;
if (!batchId) {
  throw "Missing batchId; cannot NACK";
}

http("POST", "/v1/events/nacks", {
  batchId: batchId,
  eventIds: [eventId],
  requeue: true
});

({});
