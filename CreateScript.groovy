// CreateScript.groovy (LEGACY-SHAPE PUBLISH)
// Publishes: POST /v1/publish
// Uses configuration.serviceAddress as base URL
// Defaults routingKey from configuration.propertyBag.queueName
// Transforms incoming envelope (eventName/data) into legacy bus shape (Name/Content string)

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import org.identityconnectors.framework.common.objects.AttributeUtil
import org.identityconnectors.framework.common.objects.Uid

def getApiKey() {
    def gs = configuration.password
    if (gs == null) {
        throw new IllegalStateException("configuration.password (GuardedString API key) is required")
    }
    def apiKey = null
    gs.access({ chars -> apiKey = new String(chars) }
        as org.identityconnectors.common.security.GuardedString.Accessor)
    return apiKey
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
    if (code < 200 || code >= 300) {
        def err = conn.getErrorStream()
        def errBody = err ? err.getText(StandardCharsets.UTF_8.name()) : ""
        throw new RuntimeException("HTTP ${code} POST ${urlStr} failed: ${errBody}")
    }
    return true
}

// --- Attribute helpers (attributes is Set<Attribute>) ---
def attrValue(String name) {
    def a = AttributeUtil.find(name, attributes)
    if (a == null) return null
    def v = a.getValue()
    if (v == null || v.isEmpty()) return null
    return v[0]
}
def attrString(String name) {
    def v = attrValue(name)
    return (v == null) ? null : v.toString()
}
def attrBoolean(String name) {
    def a = AttributeUtil.find(name, attributes)
    if (a == null) return null
    return AttributeUtil.getBooleanValue(a)
}

// --- configuration ---
def baseUrl = (configuration.serviceAddress ?: "").toString().replaceAll("/+\$", "")
if (!baseUrl) throw new IllegalStateException("configuration.serviceAddress is required")

def apiKey = getApiKey()

def bag = configuration.propertyBag ?: [:]
def defaultQueue = (bag.queueName ?: "")?.toString()
def configuredEventName = (bag.eventName ?: "")?.toString()

// --- read incoming attributes ---
def routingKey = (attrString("routingKey") ?: defaultQueue ?: "")?.toString()
if (!routingKey) throw new IllegalArgumentException("routingKey is required")

def exchange    = (attrString("exchange") ?: "")
def contentType = (attrString("contentType") ?: "application/json")

def persistent = attrBoolean("persistent")
persistent = (persistent == null ? true : persistent)

// accept payload OR data
def incoming = attrValue("payload")
if (incoming == null) incoming = attrValue("data")
if (incoming == null) throw new IllegalArgumentException("payload (or data) is required")

// parse if JSON string
if (incoming instanceof String) {
    try { incoming = new JsonSlurper().parseText(incoming as String) } catch (Exception ignore) { }
}

// must be a Map to transform
if (!(incoming instanceof Map)) {
    throw new IllegalArgumentException("payload/data must be an object (Map)")
}
def inMap = (Map) incoming

// --- derive event name (what facade expects as payload.Name) ---
def derivedEventName =
    (inMap.eventName ?: inMap.EventName ?: inMap.Name ?: inMap.name ?: inMap.type ?: inMap.Type ?: configuredEventName)

if (!derivedEventName) {
    throw new IllegalArgumentException("eventName is required (payload.eventName or configuration.propertyBag.eventName)")
}
derivedEventName = derivedEventName.toString()

def nowIso = OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

// --- build legacy bus payload ---
def legacyPayload = [:]

// Keep provided legacy fields, but normalize blanks
legacyPayload.Action      = ((inMap.Action ?: "UPDATE")?.toString() ?: "UPDATE")
legacyPayload.DateTime    = ((inMap.DateTime ?: inMap.timestamp ?: nowIso)?.toString() ?: nowIso)
legacyPayload.Key         = ((inMap.Key ?: inMap.eventId ?: java.util.UUID.randomUUID().toString())?.toString())
legacyPayload.Name        = ((inMap.Name?.toString()?.trim()) ? inMap.Name.toString() : derivedEventName)
legacyPayload.Application = ((inMap.Application ?: "PingOneAIC")?.toString() ?: "PingOneAIC")
legacyPayload.Sender      = ((inMap.Sender ?: "PingOneAIC")?.toString() ?: "PingOneAIC")
legacyPayload.AccountId   = ((inMap.AccountId ?: (inMap.data instanceof Map ? (inMap.data.idAlumno ?: inMap.data.AccountId) : null) ?: "unknown")?.toString() ?: "unknown")

// Content must be a STRING containing JSON
def contentObj = inMap.Content
if (contentObj == null) {
    if (inMap.data instanceof Map) {
        contentObj = inMap.data
    } else {
        def tmp = new LinkedHashMap(inMap)
        ["eventName","EventName","timestamp","eventId","source","type","Type"].each { tmp.remove(it) }
        contentObj = tmp
    }
}

if (contentObj instanceof String) {
    legacyPayload.Content = (String) contentObj
} else {
    legacyPayload.Content = JsonOutput.toJson(contentObj)
}

// Optional: preserve batchId if present
if (inMap.batchId != null) legacyPayload.batchId = inMap.batchId

def req = [
    exchange    : exchange,
    routingKey  : routingKey,
    payload     : legacyPayload,
    contentType : contentType,
    persistent  : persistent
]

def headersVal = attrValue("headers")
if (headersVal != null) req.headers = headersVal

def url = "${baseUrl}/v1/publish"
httpPostJson(url, [
    "X-API-Key": apiKey,
    "Accept": "application/json"
], JsonOutput.toJson(req))

return new Uid(java.util.UUID.randomUUID().toString())
