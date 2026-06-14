FROM nginx:alpine

COPY frontend/nginx/default.conf.template /etc/nginx/templates/default.conf.template
COPY frontend/docker-entrypoint.d/40-render-config.sh /docker-entrypoint.d/40-render-config.sh
COPY frontend/www /usr/share/nginx/html

RUN chmod +x /docker-entrypoint.d/40-render-config.sh
