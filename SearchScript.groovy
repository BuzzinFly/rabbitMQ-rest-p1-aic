// SearchScript.groovy (DROP-IN)
// Supports BOTH:
//  1) Polling list: GET /v1/events/users?...&queue=...
//  2) Read-by-id (when IDM routes READ as SEARCH): GET /v1/events/by-id/{id}

import groovy.json.JsonSlurper
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection
import java.net.URLEncoder
import groovy.util.ConfigSlurper

import org.identityconnectors.framework.common.objects.Uid
import org.identityconnectors.framework.common.objects.Attribute

// ----------------------------
// Helpers
// ----------------------------
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
    } else if (code == 204 || code == 404) {
        body = null
    } else {
        def err = conn.getErrorStream()
        def errBody = err ? err.getText(StandardCharsets.UTF_8.name()) : ""
        throw new RuntimeException("HTTP ${code} GET ${urlStr} failed: ${errBody}")
    }
    return [code: code, body: body]
}

def extractId(Object v) {
    if (v == null) return null
    if (v instanceof Uid) return ((Uid) v).getUidValue()
    if (v instanceof Attribute) {
        def vals = ((Attribute) v).getValue()
        if (vals != null && !vals.isEmpty() && vals[0] != null) return vals[0].toString()
        return null
    }
    if (v instanceof Map) {
        def m = (Map) v
        def maybe = m.get("__UID__") ?: m.get("_id") ?: m.get("id") ?: m.get("uid")
        return maybe != null ? maybe.toString() : null
    }
    return v.toString()
}

def sanitizeId(String s) {
    if (s == null) return null
    return s.trim().replace("\r", "").replace("\n", "").replace("\t", "")
}

// ----------------------------
// Base URL
// ----------------------------
def baseUrl = (configuration.serviceAddress ?: "").toString()
while (baseUrl.endsWith("/")) {
    baseUrl = baseUrl.substring(0, baseUrl.length() - 1)
}
if (!baseUrl) throw new IllegalStateException("configuration.serviceAddress is required")

// ----------------------------
// customConfiguration (script-consumable)
// ----------------------------
def customText = (configuration.customConfiguration ?: "").toString()
def cfg = new ConfigSlurper().parse(customText)

def limit = ((cfg.get("limit") ?: 100) as int)
def leaseSeconds = ((cfg.get("leaseSeconds") ?: 10) as int)
def eventName = (cfg.get("eventName") ?: "CuentaModificada").toString()
def includeDataFlag = (cfg.get("includeData") != null) ? (cfg.get("includeData") as boolean) : true

def queueName = (cfg.get("queueName") ?: "").toString()
if (!queueName) {
    throw new IllegalStateException("customConfiguration.queueName is required. customConfiguration=" + customText)
}

def apiKey = getApiKey()

// IMPORTANT: capture into a final variable so handler closure doesn't try to resolve it on its delegate
final Set allowedKeysFinal = ([
  "batchId","_id","eventType","sourceQueue","receivedAt",
  "headers","rawContent","data"
] as Set)

// Emit helper that avoids unqualified lookups inside handler closure
def emitEvent = { Map ev ->
    def uidVal = (ev.get("_id") ?: ev.get("__UID__") ?: ev.get("id"))?.toString()
    if (!uidVal) return

    def nameVal = (ev.get("__NAME__") ?: uidVal)?.toString()

    // Collect attributes outside handler scope to avoid delegate/property resolution issues
    Map<String, Object> attrs = [:]
    ev.each { k, v ->
        def key = k?.toString()
        if (key && v != null && allowedKeysFinal.contains(key)) {
            attrs[key] = v
        }
    }
    attrs["_id"] = uidVal

    handler {
        uid uidVal
        id  nameVal
        attrs.each { k, v ->
            attribute k, v
        }
    }
}

// ----------------------------
// Detect "read-by-id" requests
// ----------------------------
def requestedId = null
try {
    requestedId = sanitizeId(extractId(this.binding?.hasVariable("uid") ? this.binding.getVariable("uid") : null))
    if (!requestedId) {
        requestedId = sanitizeId(extractId(this.binding?.hasVariable("id") ? this.binding.getVariable("id") : null))
    }

    // Reflectively scan filter for Equals(__UID__|_id)
    if (!requestedId && this.binding?.hasVariable("filter")) {
        def f = this.binding.getVariable("filter")

        def findIdInFilter
        findIdInFilter = { obj ->
            if (obj == null) return null
            try {
                def mName = obj.metaClass.getMetaMethod("getAttributeName")
                def mVal  = obj.metaClass.getMetaMethod("getAttributeValue")
                if (mName && mVal) {
                    def an = obj.getAttributeName()?.toString()
                    if (an == "__UID__" || an == "_id") {
                        def av = obj.getAttributeValue()
                        return sanitizeId(av != null ? av.toString() : null)
                    }
                }

                def mFilters = obj.metaClass.getMetaMethod("getFilters")
                if (mFilters) {
                    def parts = obj.getFilters()
                    if (parts instanceof Collection) {
                        for (p in parts) {
                            def r = findIdInFilter(p)
                            if (r) return r
                        }
                    }
                }

                def mComps = obj.metaClass.getMetaMethod("getComponents")
                if (mComps) {
                    def parts = obj.getComponents()
                    if (parts instanceof Collection) {
                        for (p in parts) {
                            def r = findIdInFilter(p)
                            if (r) return r
                        }
                    }
                }
            } catch (ignored) { }
            return null
        }

        requestedId = findIdInFilter(f)
    }
} catch (ignored) {
    requestedId = null
}

// ----------------------------
// If single-id requested -> call /v1/events/by-id/{id}
// ----------------------------
if (requestedId) {
    def urlById = "${baseUrl}/v1/events/by-id/${URLEncoder.encode(requestedId, 'UTF-8')}"
    def respById = httpGet(urlById, [
        "X-API-Key": apiKey,
        "Accept": "application/json"
    ])

    if (respById.code == 404 || respById.body == null || respById.body.trim().isEmpty()) {
        return null
    }

    def ev = new JsonSlurper().parseText(respById.body)
    if (ev instanceof Map) {
        emitEvent((Map) ev)
    }
    return null
}

// ----------------------------
// Otherwise -> normal polling list via /v1/events/users
// ----------------------------
def qs =
    "limit=${limit}" +
    "&leaseSeconds=${leaseSeconds}" +
    "&eventName=${URLEncoder.encode(eventName, 'UTF-8')}" +
    "&includeData=${includeDataFlag}" +
    "&queue=${URLEncoder.encode(queueName, 'UTF-8')}"

def url = "${baseUrl}/v1/events/users?${qs}"

def resp = httpGet(url, [
    "X-API-Key": apiKey,
    "Accept": "application/json"
])

if (resp.code == 204 || resp.body == null || resp.body.trim().isEmpty()) return null

def parsed = new JsonSlurper().parseText(resp.body)
def items = (parsed?.result instanceof List) ? parsed.result : []

items.each { ev ->
    if (ev instanceof Map) {
        emitEvent((Map) ev)
    }
}

return null
