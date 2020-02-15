#!/usr/bin/env bash


[[ -z "${ENV}" ]] && { echo "ENV was empty"; exit 1; }
[[ -z "${API_KEY}" ]] && { echo "API_KEY was empty"; exit 1; }
[[ -z "${SASL_CLASS}" ]] && { echo "SASL_CLASS was empty"; exit 1; }
[[ -z "${SR_API_KEY}" ]] && { echo "SR_API_KEY was empty"; exit 1; }
[[ -z "${SECRET_KEY}" ]] && { echo "SECRET_KEY was empty"; exit 1; }
[[ -z "${SR_SECRET_KEY}" ]] && { echo "SR_SECRET_KEY was empty"; exit 1; }
[[ -z "${BOOTSTRAP_SERVERS}" ]] && { echo "BOOTSTRAP_SERVERS was empty"; exit 1; }
[[ -z "${SCHEMA_REGISTRY_URL}" ]] && { echo "SCHEMA_REGISTRY_URL was empty"; exit 1; }

export B64_API_KEY=$(base64 <(echo ${API_KEY}))
export B64_SASL_CLASS=$(base64 <(echo ${SASL_CLASS}))
export B64_SR_API_KEY=$(base64 <(echo ${SR_API_KEY}))
export B64_SECRET_KEY=$(base64 <(echo ${SECRET_KEY}))
export B64_SR_SECRET_KEY=$(base64 <(echo ${SR_SECRET_KEY}))
export B64_BOOTSTRAP_SERVERS=$(base64 <(echo ${BOOTSTRAP_SERVERS}))
export B64_SCHEMA_REGISTRY_URL=$(base64 <(echo ${SCHEMA_REGISTRY_URL}))

envsubst > $(dirname "$0")/.secrets/staging.yaml <<- "EOF"
---
apiVersion: v1
kind: Secret
metadata:
  name: ${ENV}-secretzz
data:
  api-key: ${B64_API_KEY}
  secret-key: ${B64_SECRET_KEY}
  bootstrap-servers: ${B64_BOOTSTRAP_SERVERS}
  schema-registry-url: ${B64_SCHEMA_REGISTRY_URL}
  sasl-class: ${B64_SASL_CLASS}
  sr-api-key: ${B64_SR_API_KEY}
  sr-secret-key: ${B64_SR_SECRET_KEY}

EOF

