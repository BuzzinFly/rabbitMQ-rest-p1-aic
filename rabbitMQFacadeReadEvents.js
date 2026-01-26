/**
 * Poll facade for events, upsert managed/user correlated by mail, then ACK/NACK.
 *
 * Assumes facade returns:
 * {
 *   batchId: "...",
 *   events: [ { id: "<receiptId>", data: { ...external schema... } }, ... ]
 * }
 */

// Connector configuration properties you define
var FACADE_BASE_URL = configuration.facadeBaseUrl;   // e.g. "https://facade.example.com"
var API_KEY         = configuration.facadeApiKey;    // store in ESV and reference it
var LIMIT           = configuration.limit || 100;
var LEASE_SECONDS   = configuration.leaseSeconds || 60;

function facadeRequest(method, path, body) {
  var req = {
    method: method,
    uri: FACADE_BASE_URL + path,
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": API_KEY
    }
  };

  if (body !== null && body !== undefined) {
    req.entity = JSON.stringify(body);
  }

  // Depending on your AIC scripting runtime, this might be `request(...)` or a provided http client.
  // Keep this as `request(...)` if that's what your Scripted REST connector supports.
  var resp = request(req);

  if (resp.status === 204) {
    return { status: 204, json: null };
  }
  if (resp.status < 200 || resp.status >= 300) {
    throw "Facade call failed: " + method + " " + path +
          " status=" + resp.status + " body=" + (resp.entity || "");
  }

  var json = resp.entity ? JSON.parse(resp.entity) : null;
  return { status: resp.status, json: json };
}

function ackById(eventId) {
  facadeRequest("POST", "/v1/events/ack-by-id", { eventId: eventId });
}

function nackBatch(batchId, eventIds, requeue) {
  facadeRequest("POST", "/v1/events/nacks", {
    batchId: batchId,
    eventIds: eventIds,
    requeue: requeue
  });
}

function escapeQueryValue(v) {
  // Basic escaping for quotes inside values in _queryFilter strings
  return ("" + v).replace(/"/g, '\\"');
}

function upsertUserByEmail(data) {
  var email = data.Email;
  if (!email) {
    throw "Missing Email in payload; cannot correlate";
  }

  var loginName = null;
  if (data.LoginNames && data.LoginNames.length > 0) {
    loginName = data.LoginNames[0];
  }
  if (!loginName) {
    loginName = email;
  }

  // Correlate by mail
  var q = 'mail eq "' + escapeQueryValue(email) + '"';
  var result = openidm.query("managed/user", { "_queryFilter": q });

  var userObj = {
    userName: loginName,
    givenName: data.FirstName,
    sn: data.Surname,
    mail: email,
    telephoneNumber: data.MobileNumber,

    // Use your indexed strings for externally-managed fields
    frIndexedString1: data.IdentityDocument,
    frIndexedString2: data.IdentityDocumentType,
    frIndexedString3: data.AlumnoIdIntegracion,
    frIndexedString4: data.TrabajadorIdIntegracion
  };

  // Create if missing
  if (!result || !result.result || result.result.length === 0) {
    // If you want to ensure mail/userName exist:
    if (!userObj.userName) userObj.userName = email;

    openidm.create("managed/user", null, userObj);
    return;
  }

  // Patch if exists (safer than full update)
  var existing = result.result[0];
  var patch = [];

  Object.keys(userObj).forEach(function(k) {
    if (userObj[k] !== undefined && userObj[k] !== null) {
      patch.push({ operation: "replace", field: "/" + k, value: userObj[k] });
    }
  });

  openidm.patch("managed/user/" + existing._id, null, patch);
}

function runOnce() {
  // 1) Poll
  var poll = facadeRequest(
    "GET",
    "/v1/events/users?limit=" + LIMIT + "&leaseSeconds=" + LEASE_SECONDS,
    null
  );

  if (poll.status === 204) {
    return { processed: 0, failed: 0 };
  }

  var batchId = poll.json.batchId;
  var events = poll.json.events || [];

  var processed = 0;
  var failedIds = [];

  // 2) Process each event
  for (var i = 0; i < events.length; i++) {
    var evt = events[i];
    var receiptId = evt.id;   // facade internal id for ACK/NACK
    var data = evt.data;      // raw payload (external schema)

    try {
      upsertUserByEmail(data);
      // 3) ACK on success
      ackById(receiptId);
      processed++;
    } catch (e) {
      logger.error("Failed processing receiptId=" + receiptId + " error=" + e);
      failedIds.push(receiptId);
    }
  }

  // 4) NACK failures (requeue for retry)
  if (failedIds.length > 0) {
    nackBatch(batchId, failedIds, true);
  }

  return { processed: processed, failed: failedIds.length };
}

// Execute once per invocation
runOnce();
