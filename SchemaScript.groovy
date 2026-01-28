import org.identityconnectors.framework.common.objects.ObjectClass

builder.schema {
  objectClass {
    type ObjectClass.ACCOUNT_NAME

    attributes {
      // Core identifiers
      __UID__  String.class
      __NAME__ String.class

      // Returned by SEARCH
      batchId String.class
      data    Map.class

      // Inputs for CREATE (publish)
      routingKey  String.class
      exchange    String.class
      contentType String.class
      persistent  Boolean.class
      payload     Map.class
    }
  }
}
