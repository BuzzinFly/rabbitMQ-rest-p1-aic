import groovy.json.JsonSlurper
import org.identityconnectors.framework.common.objects.ObjectClass
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder

import org.apache.http.client.methods.HttpGet
import org.apache.http.util.EntityUtils

import java.net.URLEncoder

def logger = log

// Use injected bindings directly:
// - connection (Apache HttpClient)
// - configuration (ScriptedRESTConfiguration)
// - objectClass (ObjectClass)
// - handler (ResultsHandler)

if (objectClass != ObjectClass.ACCOUNT) {
    logger.info("[rabbitmqFacade][SEARCH] Unsupported objectClass=" + objectClass)
    return
}

def limit = (configuration.propertyBag?.limit ?: 100) as Integer
def leaseSeconds = (configuration.propertyBag?.leaseSeconds ?: 60) as Integer

logger.info("[rabbitmqFacade][SEARCH] Polling facade limit=" + limit + " leaseSeconds=" + leaseSeconds)

def base = (configuration.serviceAddress ?: "").toString()
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

logger.info("[rabbitmqFacade][SEARCH] Request URL=" + url)

def req = new HttpGet(url)
req.setHeader("Accept", "application/json")

// Extract API key from configuration.password safely (GuardedString)
def apiKey = ""
def pw = configuration.password

if (pw != null) {
    try {
        pw.access { chars ->
            apiKey = new String(chars)
        }
    } catch (Exception e) {
        apiKey = pw.toString()
    }
}

apiKey = apiKey.replaceAll("\\s+", "")

logger.info("[rabbitmqFacade][SEARCH] apiKey present=" + (apiKey.length() > 0))
logger.info("[rabbitmqFacade][SEARCH] apiKey length=" + apiKey.length())

if (!apiKey) {
    logger.warn("[rabbitmqFacade][SEARCH] API key is empty; request will likely get 401")
} else {
    req.setHeader("X-API-Key", apiKey)
    logger.info("[rabbitmqFacade][SEARCH] X-API-Key header set")
}

// Execute request using Apache HttpClient
def resp = connection.execute(req)
def status = resp.getStatusLine().getStatusCode()
logger.info("[rabbitmqFacade][SEARCH] Facade status=" + status)

if (status == 204) {
    EntityUtils.consumeQuietly(resp.getEntity())
    logger.info("[rabbitmqFacade][SEARCH] No events (204)")
    return
}

def entity = resp.getEntity()
def bodyText = entity != null ? EntityUtils.toString(entity, "UTF-8") : ""
EntityUtils.consumeQuietly(entity)

if (status < 200 || status >= 300) {
    throw new RuntimeException("Facade returned status=" + status + " body=" + bodyText)
}

if (!bodyText) {
    throw new RuntimeException("Facade returned empty body with status=" + status)
}

def body = new JsonSlurper().parseText(bodyText)
def batchId = body?.batchId
def events = body?.events ?: []

logger.info("[rabbitmqFacade][SEARCH] batchId=" + batchId + " events=" + events.size())

// Emit connector objects back to IDM via the injected handler
def rh = handler

events.each { evt ->
    def id = (evt.id ?: evt.eventId)?.toString()
    if (!id) {
        logger.warn("[rabbitmqFacade][SEARCH] Skipping event missing id. Keys=" + evt?.keySet())
        return
    }

    def cob = new ConnectorObjectBuilder()
    cob.setObjectClass(ObjectClass.ACCOUNT)
    cob.setUid(id)
    cob.setName(id)

    cob.addAttribute("batchId", batchId)
    cob.addAttribute("data", evt.data)

    rh.handle(cob.build())
}

return
