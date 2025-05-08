#!/bin/sh

echo "Generating .htpasswd..."
htpasswd -cb /etc/nginx/.htpasswd "$NGINX_USER" "$NGINX_PASSWORD"

exec nginx -g "daemon off;"
