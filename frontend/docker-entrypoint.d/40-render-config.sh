#!/bin/sh
set -eu

envsubst '${ASK_SERVER_EMBEDDING_MODE}' \
    < /usr/share/nginx/html/config.js.template \
    > /usr/share/nginx/html/config.js
