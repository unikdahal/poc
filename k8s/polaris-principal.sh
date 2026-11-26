#!/bin/sh
# Creates the Sail POC principal in an existing Polaris, with read-only access to the
# namespaces in READ_NAMESPACES and read-write access to WRITE_NAMESPACE only.
# Run by a Polaris admin. Prints the new client id and secret once: store them in the
# polaris-client Secret. Needs: curl, jq.
#
#   POLARIS=https://polaris.internal  CATALOG=<catalog>  ADMIN_TOKEN=<admin bearer token> \
#   READ_NAMESPACES="ns_a ns_b"  WRITE_NAMESPACE=sail_poc  [REALM=<realm>]  sh polaris-principal.sh
set -eu
: "${POLARIS:?}" "${CATALOG:?}" "${ADMIN_TOKEN:?}" "${READ_NAMESPACES:?}" "${WRITE_NAMESPACE:?}"
PRINCIPAL=${PRINCIPAL:-sail_poc}
H_AUTH="Authorization: Bearer $ADMIN_TOKEN"
H_REALM="Polaris-Realm: ${REALM:-POLARIS}"
api() { # method path [json]
  code=$(curl -s -o /tmp/polaris.out -w '%{http_code}' -X "$1" "$POLARIS$2" -H "$H_AUTH" -H "$H_REALM" \
         -H 'Content-Type: application/json' ${3:+-d "$3"})
  case $code in 2??|409) ;; *) echo "FAILED $1 $2 -> $code: $(cat /tmp/polaris.out)" >&2; exit 1;; esac
}
grant() { # namespace privilege
  ns_json=$(printf '%s' "$1" | jq -R 'split(".")')
  api PUT "/api/management/v1/catalogs/$CATALOG/catalog-roles/$PRINCIPAL/grants" \
    "{\"grant\":{\"type\":\"namespace\",\"namespace\":$ns_json,\"privilege\":\"$2\"}}"
}

# 1. Principal (the response holds its credentials, shown only once)
api POST /api/management/v1/principals "{\"principal\":{\"name\":\"$PRINCIPAL\"}}"
if [ "$code" = 409 ]; then echo "principal $PRINCIPAL already exists: rotate its credentials instead" >&2
else jq '{client_id: .credentials.clientId, client_secret: .credentials.clientSecret}' /tmp/polaris.out; fi

# 2. Principal role and catalog role, linked
api POST /api/management/v1/principal-roles "{\"principalRole\":{\"name\":\"$PRINCIPAL\"}}"
api PUT "/api/management/v1/principals/$PRINCIPAL/principal-roles" "{\"principalRole\":{\"name\":\"$PRINCIPAL\"}}"
api POST "/api/management/v1/catalogs/$CATALOG/catalog-roles" "{\"catalogRole\":{\"name\":\"$PRINCIPAL\"}}"
api PUT "/api/management/v1/principal-roles/$PRINCIPAL/catalog-roles/$CATALOG" "{\"catalogRole\":{\"name\":\"$PRINCIPAL\"}}"

# 3. List namespaces anywhere in the catalog
api PUT "/api/management/v1/catalogs/$CATALOG/catalog-roles/$PRINCIPAL/grants" \
  '{"grant":{"type":"catalog","privilege":"NAMESPACE_LIST"}}'

# 4. Read-only on production namespaces
for ns in $READ_NAMESPACES; do
  for p in NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do grant "$ns" "$p"; done
done

# 5. Read-write on the POC namespace (created here if missing)
api POST "/api/catalog/v1/$CATALOG/namespaces" "{\"namespace\":$(printf '%s' "$WRITE_NAMESPACE" | jq -R 'split(".")')}"
for p in NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_CREATE TABLE_DROP TABLE_READ_PROPERTIES \
         TABLE_WRITE_PROPERTIES TABLE_READ_DATA TABLE_WRITE_DATA; do grant "$WRITE_NAMESPACE" "$p"; done
echo "done: principal $PRINCIPAL, read: $READ_NAMESPACES, write: $WRITE_NAMESPACE" >&2
