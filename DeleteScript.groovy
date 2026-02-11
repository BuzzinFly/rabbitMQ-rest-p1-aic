import groovy.json.JsonOutput
import org.identityconnectors.framework.common.objects.ObjectClass
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils

def logger = log

if (objectClass != ObjectClass.ACCOUNT) {
    logger.info("[rabbitmqFacade][DELETE] Unsupported objectClass=" + objectClass)
    return
}

def eventId = uid?.uidValue?.toString()
if (!eventId) {
    throw new RuntimeException("Missing uid.uidValue for DELETE")
}

def base = (configuration.serviceAddress ?: "").toString()
if (!base) {
    throw new RuntimeException("serviceAddress is empty in configurationProperties")
}
if (base.endsWith("/")) {
    base = base.substring(0, base.length() - 1)
}

// Extract API key from configuration.password (GuardedString-safe)
def apiKey = ""
def pw = configuration.password
if (pw != null) {
    try {
        pw.access { chars -> apiKey = new String(chars) }
    } catch (Exception e) {
        apiKey = pw.toString()
    }
}
apiKey = apiKey.replaceAll("\\s+", "")

logger.info("[rabbitmqFacade][DELETE] apiKey present=" + (apiKey.length() > 0))
logger.info("[rabbitmqFacade][DELETE] apiKey length=" + apiKey.length())

def url = base + "/v1/events/ack-by-id"
logger.info("[rabbitmqFacade][DELETE] ACK url=" + url + " eventId=" + eventId)

def req = new HttpPost(url)
req.setHeader("Accept", "application/json")
req.setHeader("Content-Type", "application/json")
if (apiKey) {
    req.setHeader("X-API-Key", apiKey)
}

def jsonBody = JsonOutput.toJson([eventId: eventId])
req.setEntity(new StringEntity(jsonBody, "UTF-8"))

def resp = connection.execute(req)
def status = resp.getStatusLine().getStatusCode()
def entity = resp.getEntity()
def bodyText = entity != null ? EntityUtils.toString(entity, "UTF-8") : ""
EntityUtils.consumeQuietly(entity)

logger.info("[rabbitmqFacade][DELETE] ACK status=" + status + " eventId=" + eventId)

if (status < 200 || status >= 300) {
    throw new RuntimeException("ACK failed status=" + status + " body=" + bodyText + " eventId=" + eventId)
}

return
