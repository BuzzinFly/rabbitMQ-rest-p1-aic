import groovy.json.JsonSlurper
import org.identityconnectors.framework.common.objects.ObjectClass
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder

import org.apache.http.client.methods.HttpGet
import org.apache.http.util.EntityUtils

import java.net.URLEncoder

def logger = log

// Only ACCOUNT supported
if (objectClass != ObjectClass.ACCOUNT) {
    return
}

def limit = (configuration.propertyBag?.limit ?: 100) as Integer
def leaseSeconds = (configuration.propertyBag?.leaseSeconds ?: 60) as Integer

def base = (configuration.serviceAddress ?: "").toString().trim()
if (!base) {
    throw new RuntimeException("serviceAddress is empty in configurationProperties")
}
if (base.endsWith("/")) {
    base = base.substring(0, base.length() - 1)
}

// Build URL with query string
def qs = "limit=" + URLEncoder.encode(limit.toString(), "UTF-8") +
        "&leaseSeconds=" + URLEncoder.encode(leaseSeconds.toString(), "UTF-8")
def url = base + "/v1/events/users?" + qs

def req = new HttpGet(url)
req.setHeader("Accept", "application/json")

// API key from configuration.password (GuardedString)
def apiKey = ""
def pw = configuration.password
if (pw != null) {
    pw.access { chars -> apiKey = new String(chars) }
}
apiKey = (apiKey ?: "").trim()
if (!apiKey) {
    throw new RuntimeException("API key is empty in configuration.password")
}
req.setHeader("x-api-key", apiKey)

// Execute request using Apache HttpClient
def resp = connection.execute(req)
def status = resp.getStatusLine().getStatusCode()

if (status == 204) {
    EntityUtils.consumeQuietly(resp.getEntity())
    return
}

def entity = resp.getEntity()
def bodyText = entity != null ? EntityUtils.toString(entity, "UTF-8") : ""
EntityUtils.consumeQuietly(entity)

if (status < 200 || status >= 300) {
    throw new RuntimeException("Facade returned status=" + status + " body=" + bodyText)
}
if (!bodyText) {
    return
}

def body = new JsonSlurper().parseText(bodyText)
def batchId = body?.batchId
def events = body?.events ?: []

// Emit connector objects back to IDM via the injected handler (Closure in this runtime)
def rh = handler

events.each { evt ->
    def id = (evt.id ?: evt.eventId)?.toString()
    if (!id) {
        return
    }

    def cob = new ConnectorObjectBuilder()
    cob.setObjectClass(ObjectClass.ACCOUNT)
    cob.setUid(id)
    cob.setName(id)

    cob.addAttribute("batchId", batchId)
    cob.addAttribute("data", evt.data)

    // handler is a Closure; call it. Some runtimes return false to stop iteration.
    def keepGoing = rh.call(cob.build())
    if (keepGoing == false) {
        return
    }
}

return
