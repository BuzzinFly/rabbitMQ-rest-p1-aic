// ReadScript.groovy (DROP-IN, read-by-id)
// Uses: GET /v1/events/by-id/{id}
// No poll/lease side effects.

import groovy.json.JsonSlurper
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection
import java.net.URLEncoder

import org.identityconnectors.framework.common.objects.ObjectClass
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder
import org.identityconnectors.framework.common.objects.Uid

if (objectClass != ObjectClass.ACCOUNT) {
    return null
}

// --- resolve id ---
def id = null
if (uid instanceof Uid) {
    id = uid.getUidValue()
} else if (uid != null) {
    id = uid.toString()
}
if (!id || id.trim().isEmpty()) {
    throw new RuntimeException("Missing uid for READ")
}
id = id.trim()

// --- baseUrl ---
def baseUrl = (configuration.serviceAddress ?: "").toString()
while (baseUrl.endsWith("/")) {
    baseUrl = baseUrl.substring(0, baseUrl.length() - 1)
}
if (!baseUrl) {
    throw new RuntimeException("configuration.serviceAddress is required")
}

// --- api key ---
def apiKey = null
def pw = configuration.password
if (pw != null) {
    pw.access { chars -> apiKey = new String(chars) }
}
apiKey = (apiKey ?: "").trim()
if (!apiKey) {
    throw new RuntimeException("API key is empty in configuration.password")
}

// --- call facade ---
def urlStr = "${baseUrl}/v1/events/by-id/${URLEncoder.encode(id, 'UTF-8')}"

HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
conn.setRequestMethod("GET")
conn.setConnectTimeout(15000)
conn.setReadTimeout(30000)
conn.setRequestProperty("Accept", "application/json")
conn.setRequestProperty("X-API-Key", apiKey)

int status = conn.getResponseCode()
if (status == 404 || status == 204) {
    return null
}

String bodyText = null
if (status >= 200 && status < 300) {
    bodyText = conn.getInputStream().getText(StandardCharsets.UTF_8.name())
} else {
    def err = conn.getErrorStream()
    def errBody = err ? err.getText(StandardCharsets.UTF_8.name()) : ""
    throw new RuntimeException("Facade READ-by-id failed status=${status} body=${errBody}")
}

if (!bodyText || bodyText.trim().isEmpty()) return null

def ev = new JsonSlurper().parseText(bodyText)
if (!(ev instanceof Map)) return null

def evId = (ev._id ?: ev.__UID__ ?: id)?.toString()
if (!evId) evId = id

def cob = new ConnectorObjectBuilder()
cob.setObjectClass(ObjectClass.ACCOUNT)
cob.setUid(evId)
cob.setName(evId)

// Emit only what your schema supports (current approach)
def allowed = ["_id","batchId","eventType","sourceQueue","receivedAt","headers","rawContent","data"]
allowed.each { k ->
    if (ev.containsKey(k) && ev[k] != null) {
        cob.addAttribute(k, ev[k])
    }
}

// Convenience: always set _id
cob.addAttribute("_id", evId)

return cob.build()
