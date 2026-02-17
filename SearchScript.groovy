// SearchScript.groovy (DROP-IN)
// Uses the OpenICF "handler" callback (Closure) to emit results one-by-one.

import groovy.json.JsonSlurper
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection
import java.net.URLEncoder

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

def baseUrl = (configuration.serviceAddress ?: "").toString().replaceAll("/+\$", "")
if (!baseUrl) throw new IllegalStateException("configuration.serviceAddress is required")

def bag = configuration.propertyBag ?: [:]
def limit = ((bag.limit ?: 100) as int)
def leaseSeconds = ((bag.leaseSeconds ?: 10) as int)
def eventName = (bag.eventName ?: "CuentaModificada").toString()
def includeData = (bag.includeData != null) ? (bag.includeData as boolean) : true

def apiKey = getApiKey()

def qs = "limit=${limit}&leaseSeconds=${leaseSeconds}&eventName=${URLEncoder.encode(eventName, 'UTF-8')}&includeData=${includeData}"
def url = "${baseUrl}/v1/events/users?${qs}"

def resp = httpGet(url, [
    "X-API-Key": apiKey,
    "Accept": "application/json"
])

if (resp.code == 204 || resp.body == null || resp.body.trim().isEmpty()) return null

def parsed = new JsonSlurper().parseText(resp.body)
def items = (parsed?.result instanceof List) ? parsed.result : []

// Only emit business attributes (not __UID__/__NAME__ identifiers)
def allowed = [
  "AccountId","Action","Application","Content","DateTime","ExceptionMessage",
  "Key","Name","Sender","batchId","contentType","exchange","routingKey","persistent","data","_id"
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

        attribute "_id", uidVal
    }
}

return null
