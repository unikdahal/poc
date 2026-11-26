# Minimal image for the Polaris token refresher CronJob: sh, curl, jq, kubectl.
FROM alpine:3.20
RUN apk add --no-cache curl jq kubectl
