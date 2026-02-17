// SchemaScript.groovy
// NOTE: This script is unchanged for issue #1 (propertyBag access).
// Your current runtime error "SchemaScript must return with Schema object" is issue #2 and requires a different fix.
// If you keep schemaScriptFileName enabled, you must update SchemaScript to return an ICF Schema object.

return [
  [
    objectType: "__ACCOUNT__",
    nativeType: "__ACCOUNT__",
    type: "object",
    properties: [
      "__UID__":          [ type: "string", nativeName: "__UID__",  required: true ],
      "__NAME__":         [ type: "string", nativeName: "__NAME__", required: true ],

      "_id":              [ type: "string", nativeName: "_id" ],
      "batchId":          [ type: "string", nativeName: "batchId" ],

      "Action":           [ type: "string" ],
      "DateTime":         [ type: "string" ],
      "Key":              [ type: "string" ],
      "Name":             [ type: "string" ],
      "Application":      [ type: "string" ],
      "Sender":           [ type: "string" ],
      "AccountId":        [ type: "string" ],
      "Content":          [ type: "string" ],
      "ExceptionMessage": [ type: "string" ],

      "data":             [ type: "object" ],

      "exchange":         [ type: "string" ],
      "routingKey":       [ type: "string" ],
      "payload":          [ type: "object" ],
      "contentType":      [ type: "string" ],
      "persistent":       [ type: "boolean" ],
      "messageId":        [ type: "string" ],
      "headers":          [ type: "object" ]
    ]
  ]
]
