// SearchScript.groovy (DROP-IN, UPDATED FOR DEDICATED QUEUES)
// Uses /v1/events/users with queue=... so existing connector behavior stays consistent.

import groovy.json.JsonSlurper
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection
import java.net.URLEncoder
import groovy.util.ConfigSlurper

def getApiKey() {
    def gs = configuration.password  // GuardedString
    if (gs == null) throw new IllegalStateException("configuration.password (GuardedString API key) is required")
    def apiKey = null
    gs.access({ chars -> apiKey = new String(chars) } as org.identityconnectors.common.security.GuardedString.Accessor)
    return apiKey
}

def httpGet(String urlStr, Map<String, String> headers = [:]) {
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.setRequestMethod("GET")
    conn.setConnectTimeout(15000)
    conn.setReadTimeout(30000)
    headers.each { k, v -> conn.setRequestProperty(k, v) }

    int code = conn.getResponseCode()
    String body = null
    if (code >= 200 && code < 300) {
        body = conn.getInputStream().getText(StandardCharsets.UTF_8.name())
    } else if (code == 204) {
        body = null
    } else {
        def err = conn.getErrorStream()
        def errBody = err ? err.getText(StandardCharsets.UTF_8.name()) : ""
        throw new RuntimeException("HTTP ${code} GET ${urlStr} failed: ${errBody}")
    }
    return [code: code, body: body]
}

def baseUrl = (configuration.serviceAddress ?: "").toString()
while (baseUrl.endsWith("/")) {
    baseUrl = baseUrl.substring(0, baseUrl.length() - 1)
}
if (!baseUrl) throw new IllegalStateException("configuration.serviceAddress is required")

// ---- customConfiguration (script-consumable) ----
def customText = (configuration.customConfiguration ?: "").toString()
def cfg = new ConfigSlurper().parse(customText)

def limit = ((cfg.get("limit") ?: 100) as int)
def leaseSeconds = ((cfg.get("leaseSeconds") ?: 10) as int)
def eventName = (cfg.get("eventName") ?: "CuentaModificada").toString()
def includeData = (cfg.get("includeData") != null) ? (cfg.get("includeData") as boolean) : true

def queueName = (cfg.get("queueName") ?: "").toString()
if (!queueName) {
    throw new IllegalStateException("customConfiguration.queueName is required. customConfiguration=" + customText)
}

def apiKey = getApiKey()

def qs =
    "limit=${limit}" +
    "&leaseSeconds=${leaseSeconds}" +
    "&eventName=${URLEncoder.encode(eventName, 'UTF-8')}" +
    "&includeData=${includeData}" +
    "&queue=${URLEncoder.encode(queueName, 'UTF-8')}"

def url = "${baseUrl}/v1/events/users?${qs}"

def resp = httpGet(url, [
    "X-API-Key": apiKey,
    "Accept": "application/json"
])

if (resp.code == 204 || resp.body == null || resp.body.trim().isEmpty()) return null

def parsed = new JsonSlurper().parseText(resp.body)
def items = (parsed?.result instanceof List) ? parsed.result : []

// Keep emitting only a known safe set of attributes
def allowed = [
  "batchId","_id","eventType","sourceQueue","receivedAt",
  "headers","rawContent","data"
] as Set

items.each { ev ->
    if (!(ev instanceof Map)) return

    def uidVal = (ev._id ?: ev.__UID__ ?: ev.id)?.toString()
    if (!uidVal) return

    def nameVal = (ev.__NAME__ ?: uidVal)?.toString()

    handler {
        uid uidVal
        id  nameVal

        ev.each { k, v ->
            def key = k?.toString()
            if (key && v != null && allowed.contains(key)) {
                attribute key, v
            }
        }

        // convenience
        attribute "_id", uidVal
    }
}

return null
