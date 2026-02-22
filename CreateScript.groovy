// CreateScript.groovy (PUBLISH - CLIENT BUS FORMAT)
// Publishes: POST /v1/publish
// Message body is FLAT JSON payload (no legacy envelope)
// Rabbit headers are passed through (publishHeaders preferred, headers fallback)

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection

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
    return (apiKey ?: "").trim()
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
def baseUrl = (configuration.serviceAddress ?: "").toString()
while (baseUrl.endsWith("/")) {
    baseUrl = baseUrl.substring(0, baseUrl.length() - 1)
}
if (!baseUrl) throw new IllegalStateException("configuration.serviceAddress is required")

def apiKey = getApiKey()
if (!apiKey) throw new IllegalStateException("API key is empty in configuration.password")

// propertyBag is unreliable in some builds; still use if present (backward compatibility)
def bag = null
try { bag = configuration.propertyBag } catch (ignored) { bag = null }
def defaultQueue = ""
try {
    if (bag instanceof Map && bag.queueName != null) defaultQueue = bag.queueName.toString()
} catch (ignored) { defaultQueue = "" }

// --- read incoming attributes ---
def routingKey = (attrString("routingKey") ?: defaultQueue ?: "")?.toString()
if (!routingKey) throw new IllegalArgumentException("routingKey is required (attribute routingKey or configuration.propertyBag.queueName)")

def exchange    = (attrString("exchange") ?: "")?.toString()
def contentType = (attrString("contentType") ?: "application/json")?.toString()

def persistent = attrBoolean("persistent")
persistent = (persistent == null ? true : persistent)

// optional messageId (used by facade as AMQP message_id)
def messageId = (attrString("messageId") ?: "")?.toString()
if (!messageId) {
    def ph = attrValue("publishHeaders")
    if (ph instanceof Map && ph.key != null) {
        messageId = ph.key.toString()
    }
}

// accept payload OR data (flat JSON object)
def incoming = attrValue("payload")
if (incoming == null) incoming = attrValue("data")
if (incoming == null) throw new IllegalArgumentException("payload (or data) is required")

// parse if JSON string
if (incoming instanceof String) {
    try { incoming = new JsonSlurper().parseText(incoming as String) } catch (Exception ignore) { }
}

// must be a Map/object
if (!(incoming instanceof Map)) {
    throw new IllegalArgumentException("payload/data must be an object (Map)")
}
def payloadMap = (Map) incoming

// headers: publishHeaders preferred, fallback to headers
def publishHeaders = attrValue("publishHeaders")
if (!(publishHeaders instanceof Map)) {
    publishHeaders = attrValue("headers")
}
if (publishHeaders != null && !(publishHeaders instanceof Map)) {
    if (publishHeaders instanceof String) {
        try { publishHeaders = new JsonSlurper().parseText(publishHeaders as String) } catch (ignored) { }
    }
}

// build facade request
def req = [
    exchange    : exchange,
    routingKey  : routingKey,
    payload     : payloadMap,
    contentType : contentType,
    persistent  : persistent
]
if (messageId) req.messageId = messageId
if (publishHeaders instanceof Map) req.headers = publishHeaders

def url = "${baseUrl}/v1/publish"
httpPostJson(url, [
    "X-API-Key": apiKey,
    "Accept": "application/json"
], JsonOutput.toJson(req))

// Return UID: prefer messageId if available
def returnedId = (messageId ?: java.util.UUID.randomUUID().toString())
return new Uid(returnedId)
