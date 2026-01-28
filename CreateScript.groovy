import groovy.json.JsonOutput
import org.identityconnectors.framework.common.objects.ObjectClass
import org.identityconnectors.framework.common.objects.Uid
import org.identityconnectors.framework.common.objects.AttributeUtil
import java.util.UUID

def logger = log

def connection = connection
def objectClass = objectClass
def attributes = attributes

if (objectClass != ObjectClass.ACCOUNT) {
    throw new RuntimeException("Only ACCOUNT supported")
}

// Required attributes
def payload = AttributeUtil.getAsObjectValue(AttributeUtil.find("payload", attributes))
def routingKey = AttributeUtil.getStringValue(AttributeUtil.find("routingKey", attributes))

if (!routingKey) {
    throw new RuntimeException("Missing required attribute: routingKey")
}
if (payload == null) {
    throw new RuntimeException("Missing required attribute: payload")
}

// Optional attributes
def exchange = AttributeUtil.getStringValue(AttributeUtil.find("exchange", attributes)) ?: ""
def contentType = AttributeUtil.getStringValue(AttributeUtil.find("contentType", attributes)) ?: "application/json"
def persistentAttr = AttributeUtil.getBooleanValue(AttributeUtil.find("persistent", attributes))
def persistent = (persistentAttr == null) ? true : persistentAttr

def payloadKeys = (payload instanceof Map) ? payload.keySet().toString() : payload.getClass().getName()

logger.info("[rabbitmqFacade][CREATE] Publish exchange='" + exchange +
        "' routingKey='" + routingKey +
        "' persistent=" + persistent +
        " contentType=" + contentType +
        " payloadKeys=" + payloadKeys)

def req = [
    exchange: exchange,
    routingKey: routingKey,
    contentType: contentType,
    persistent: persistent,
    payload: payload
]

def resp = connection.post(
    path: "/v1/publish",
    body: JsonOutput.toJson(req),
    requestContentType: "application/json"
)

logger.info("[rabbitmqFacade][CREATE] Publish status=" + resp.status + " routingKey='" + routingKey + "'")

if (resp.status < 200 || resp.status >= 300) {
    throw new RuntimeException("Publish failed status=" + resp.status)
}

return new Uid(UUID.randomUUID().toString())
