import groovy.json.JsonOutput
import org.identityconnectors.framework.common.objects.ObjectClass

def connection = connection
def objectClass = objectClass
def uid = uid

def logger = log

if (objectClass != ObjectClass.ACCOUNT) {
logger.info("[rabbitmqFacade][DELETE] Unsupported objectClass=" + objectClass)
    return
}

def eventId = uid.uidValue
logger.info("[rabbitmqFacade][DELETE] ACK eventId=" + eventId)

def resp = connection.post(
    path: "/v1/events/ack-by-id",
    body: JsonOutput.toJson([eventId: eventId]),
    requestContentType: "application/json"
)

logger.info("[rabbitmqFacade][DELETE] ACK status=" + resp.status + " eventId=" + eventId)

if (resp.status < 200 || resp.status >= 300) {
    throw new RuntimeException("ACK failed status=" + resp.status + " eventId=" + eventId)
}

return
