// Scripted REST: DELETE script
// Purpose: ACK event in facade once processed

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

// In many connector scripts, the id comes in as `id` (or `uid` depending on toolkit).
// Use whatever your script receives; common patterns are `id` or `uid`.
var eventId = id;

http("POST", "/v1/events/ack-by-id", { eventId: eventId });

// Return something truthy/empty depending on connector expectations
({});
