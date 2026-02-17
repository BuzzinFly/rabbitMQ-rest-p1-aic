// DeleteScript.groovy (DROP-IN)
// ACK-by-id: POST /v1/events/ack-by-id { "eventId": "<_id>" }
// Fix: uid may be an Attribute; extract real value safely.

import groovy.json.JsonOutput
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection

import org.identityconnectors.framework.common.objects.Uid
import org.identityconnectors.framework.common.objects.Attribute

def getApiKey() {
    def gs = configuration.password
    if (gs == null) {
        throw new IllegalStateException("configuration.password (GuardedString API key) is required")
    }
    def apiKey = null
    gs.access({ chars -> apiKey = new String(chars) } as org.identityconnectors.common.security.GuardedString.Accessor)
    return apiKey
}

// Extract the actual id string from various possible types (Uid, Attribute, String, etc.)
def extractId(Object v) {
    if (v == null) return null

    if (v instanceof Uid) {
        return ((Uid) v).getUidValue()
    }

    if (v instanceof Attribute) {
        def vals = ((Attribute) v).getValue()
        if (vals != null && !vals.isEmpty() && vals[0] != null) {
            return vals[0].toString()
        }
        return null
    }

    // Some runtimes pass a Map like [__UID__: "..."] or similar
    if (v instanceof Map) {
        def m = (Map) v
        def maybe = m.get("__UID__") ?: m.get("uid") ?: m.get("id")
        return maybe != null ? maybe.toString() : null
    }

    return v.toString()
}

def sanitizeId(String s) {
    if (s == null) return null
    return s.trim().replace("\r", "").replace("\n", "").replace("\t", "")
}

def isSafeId(String s) {
    if (s == null || s.isEmpty()) return false
    return s.toList().every { ch ->
        (ch >= 'A' && ch <= 'Z') ||
        (ch >= 'a' && ch <= 'z') ||
        (ch >= '0' && ch <= '9') ||
        ch == '.' || ch == '_' || ch == ':' || ch == '-'
    }
}

def httpPostJson(String urlStr, Map<String, String> headers, String jsonBody) {
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.setRequestMethod("POST")
    conn.setDoOutput(true)
    conn.setConnectTimeout(15000)
    conn.setReadTimeout(30000)

    headers.each { k, v -> conn.setRequestProperty(k, v) }
    conn.setRequestProperty("Content-Type", "application/json")

    conn.getOutputStream().withWriter(StandardCharsets.UTF_8.name()) { it << jsonBody }

    int code = conn.getResponseCode()
    def err = conn.getErrorStream()
    def errBody = err ? err.getText(StandardCharsets.UTF_8.name()) : ""

    return [code: code, errBody: errBody]
}

def baseUrl = (configuration.serviceAddress ?: "").toString().replaceAll("/+\$", "")
if (!baseUrl) {
    throw new IllegalStateException("configuration.serviceAddress is required")
}

def apiKey = getApiKey()

// Prefer uid, fallback to id, fallback to __UID__ if present in binding
def raw =
        (uid != null ? uid : null) ?:
        (id != null ? id : null) ?:
        (this.binding?.hasVariable("__UID__") ? this.binding.getVariable("__UID__") : null)

def eventId = sanitizeId(extractId(raw))

if (!eventId) {
    throw new IllegalArgumentException("Missing event id for ACK (uid/id/__UID__)")
}

// If your ids can include other characters, relax this check.
// For your ids like "test-cuenta-modificada-0006" this is correct.
if (!isSafeId(eventId)) {
    throw new IllegalArgumentException("Suspicious eventId value after sanitization: '${eventId}'")
}

def url = "${baseUrl}/v1/events/ack-by-id"
def body = JsonOutput.toJson([eventId: eventId])

def resp = httpPostJson(url, [
    "X-API-Key": apiKey,
    "Accept": "application/json",
    "X-Debug-EventId": eventId
], body)

// Idempotent ACK (recommended)
if (resp.code == 404) {
    return null
}

if (resp.code < 200 || resp.code >= 300) {
    throw new RuntimeException("ACK failed status=${resp.code} eventId=${eventId} body=${resp.errBody}")
}

return null
