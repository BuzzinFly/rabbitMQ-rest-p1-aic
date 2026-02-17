// ReadScript.groovy (DROP-IN, read-by-id)
// Uses: GET /v1/user-events/{id}
// Avoids poll+lease side effects.

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

def baseUrl = (configuration.serviceAddress ?: "").toString().replaceAll("/+\$", "")
if (!baseUrl) {
    throw new RuntimeException("configuration.serviceAddress is required")
}

def apiKey = null
def pw = configuration.password
if (pw != null) {
    pw.access { chars -> apiKey = new String(chars) }
}
apiKey = (apiKey ?: "").trim()
if (!apiKey) {
    throw new RuntimeException("API key is empty in configuration.password")
}

def urlStr = "${baseUrl}/v1/user-events/${URLEncoder.encode(id, 'UTF-8')}"

HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
conn.setRequestMethod("GET")
conn.setConnectTimeout(15000)
conn.setReadTimeout(30000)
conn.setRequestProperty("Accept", "application/json")
conn.setRequestProperty("X-API-Key", apiKey)

int status = conn.getResponseCode()
if (status == 204 || status == 404) {
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

def cob = new ConnectorObjectBuilder()
cob.setObjectClass(ObjectClass.ACCOUNT)
cob.setUid(evId)
cob.setName(evId)

// Copy attributes your schema exposes
["Action","DateTime","Key","Name","Application","Sender","AccountId","Content","ExceptionMessage","batchId"].each { k ->
    if (ev.containsKey(k) && ev[k] != null) {
        cob.addAttribute(k, ev[k])
    }
}
if (ev.containsKey("data") && ev["data"] != null) {
    cob.addAttribute("data", ev["data"])
}

return cob.build()
