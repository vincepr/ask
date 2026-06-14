#!/bin/sh
set -eu

echo "http://localhost:${ASK_FRONTEND_EXPOSE_PORT:-13001}"
