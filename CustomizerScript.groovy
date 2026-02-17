/**
 * Scripted REST Connector Customizer
 *
 * Make logging safe: do not assume 'log' is always bound.
 */
def logger = null
try {
  logger = this.binding?.hasVariable('log') ? this.binding.getVariable('log') : null
} catch (Exception ignore) {
  logger = null
}

customize {
  init { httpClientBuilder ->
    try { logger?.info("[rabbitmqFacade][CUSTOMIZER] init() called") } catch (Exception ignore) {}
  }

  decorate { httpClient ->
    try { logger?.info("[rabbitmqFacade][CUSTOMIZER] decorate() called; returning provided httpClient") } catch (Exception ignore) {}
    return httpClient
  }
}
