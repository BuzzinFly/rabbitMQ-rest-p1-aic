/**
 * Scripted REST Connector Customizer for 1.5.20.15
 *
 * Important: inside customize/init/decorate closures, the delegate is ScriptedRESTConfiguration,
 * so unqualified 'log' is resolved against the configuration and fails.
 * Capture it outside as 'logger'.
 */
def logger = log

customize {
  init { httpClientBuilder ->
    logger.info("[rabbitmqFacade][CUSTOMIZER] init() called")
  }

  decorate { httpClient ->
    logger.info("[rabbitmqFacade][CUSTOMIZER] decorate() called; returning provided httpClient")
    return httpClient
  }
}