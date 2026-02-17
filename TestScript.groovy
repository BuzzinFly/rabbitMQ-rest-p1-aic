// TestScript.groovy (DROP-IN)
// Fix: base URL from serviceAddress (no configuration.baseUrl)

import groovy.json.JsonSlurper
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection

def getApiKey() {
    def gs = configuration.password
    if (gs == null) {
        throw new IllegalStateException("configuration.password (GuardedString API key) is required")
    }
    def apiKey = null
    gs.access({ chars -> apiKey = new String(chars) } as org.identityconnectors.common.security.GuardedString.Accessor)
    return apiKey
}

def httpGet(String urlStr, Map<String, String> headers = [:]) {
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.setRequestMethod("GET")
    conn.setConnectTimeout(8000)
    conn.setReadTimeout(15000)
    headers.each { k, v -> conn.setRequestProperty(k, v) }

    int code = conn.getResponseCode()
    String body = null
    if (code >= 200 && code < 300) {
        body = conn.getInputStream().getText(StandardCharsets.UTF_8.name())
    } else {
        def err = conn.getErrorStream()
        def errBody = err ? err.getText(StandardCharsets.UTF_8.name()) : ""
        throw new RuntimeException("HTTP ${code} GET ${urlStr} failed: ${errBody}")
    }
    return [code: code, body: body]
}

def baseUrl = (configuration.serviceAddress ?: "").toString().replaceAll("/+\$", "")
if (!baseUrl) {
    throw new IllegalStateException("configuration.serviceAddress is required")
}

def apiKey = getApiKey()
def url = "${baseUrl}/health"

def resp = httpGet(url, [
    "X-API-Key": apiKey,
    "Accept": "application/json"
])

def parsed = new JsonSlurper().parseText(resp.body ?: "{}")
if (!(parsed.ok == true || parsed.status?.toString()?.equalsIgnoreCase("ok"))) {
    throw new RuntimeException("Facade health check did not return ok=true. Response: ${resp.body}")
}

return true
