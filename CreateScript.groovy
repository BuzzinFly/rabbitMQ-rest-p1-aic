import groovy.json.JsonOutput
import org.identityconnectors.framework.common.objects.ObjectClass
import org.identityconnectors.framework.common.objects.Uid
import org.identityconnectors.framework.common.objects.AttributeUtil

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils

import java.util.UUID

def logger = log

if (objectClass != ObjectClass.ACCOUNT) {
    throw new RuntimeException("Only ACCOUNT supported for CREATE")
}

// ----- Read attributes -----
def routingKey = AttributeUtil.getStringValue(AttributeUtil.find("routingKey", attributes))
def exchange = AttributeUtil.getStringValue(AttributeUtil.find("exchange", attributes)) ?: ""
def contentType = AttributeUtil.getStringValue(AttributeUtil.find("contentType", attributes)) ?: "application/json"

def persistentAttr = AttributeUtil.getBooleanValue(AttributeUtil.find("persistent", attributes))
def persistent = (persistentAttr == null) ? true : persistentAttr

def payloadAttr = AttributeUtil.find("payload", attributes)
def payload = null
if (payloadAttr != null) {
    def vals = payloadAttr.value
    if (vals != null && !vals.isEmpty()) {
        payload = vals[0]
    }
}

if (!routingKey) {
    throw new RuntimeException("Missing required attribute: routingKey")
}
if (payload == null) {
    throw new RuntimeException("Missing required attribute: payload")
}

// ----- serviceAddress (base URL) -----
def base = (configuration.serviceAddress ?: "").toString()
if (!base) {
    throw new RuntimeException("serviceAddress is empty in configurationProperties")
}
if (base.endsWith("/")) {
    base = base.substring(0, base.length() - 1)
}

// ----- API key from configuration.password (GuardedString-safe) -----
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

logger.info("[rabbitmqFacade][CREATE] apiKey present=" + (apiKey.length() > 0))
logger.info("[rabbitmqFacade][CREATE] apiKey length=" + apiKey.length())

// ----- Call facade publish endpoint -----
def url = base + "/v1/publish"
logger.info("[rabbitmqFacade][CREATE] Publish url=" + url + " exchange='" + exchange + "' routingKey='" + routingKey + "' persistent=" + persistent + " contentType=" + contentType)

def req = new HttpPost(url)
req.setHeader("Accept", "application/json")
req.setHeader("Content-Type", "application/json")
if (apiKey) {
    req.setHeader("X-API-Key", apiKey)
}

def publishReq = [
    exchange: exchange,
    routingKey: routingKey,
    contentType: contentType,
    persistent: persistent,
    payload: payload
]

def jsonBody = JsonOutput.toJson(publishReq)
req.setEntity(new StringEntity(jsonBody, "UTF-8"))

def resp = connection.execute(req)
def status = resp.getStatusLine().getStatusCode()
def entity = resp.getEntity()
def bodyText = entity != null ? EntityUtils.toString(entity, "UTF-8") : ""
EntityUtils.consumeQuietly(entity)

logger.info("[rabbitmqFacade][CREATE] Publish status=" + status + " routingKey='" + routingKey + "'")

if (status < 200 || status >= 300) {
    throw new RuntimeException("Publish failed status=" + status + " body=" + bodyText)
}

return new Uid(UUID.randomUUID().toString())
