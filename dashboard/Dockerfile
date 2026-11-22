# syntax=docker/dockerfile:1.7
FROM node:22.21.1-alpine AS build
WORKDIR /app

COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci

COPY dashboard/ ./
COPY docs/openapi/gateway.json /docs/openapi/gateway.json
RUN npm run gen:api && npm run build

FROM nginx:1.27.5-alpine AS runtime
# Drop to non-root (nginx image provides nginx user).
RUN rm -rf /usr/share/nginx/html/* \
  && mkdir -p /var/cache/nginx /var/run /var/log/nginx \
  && chown -R nginx:nginx /usr/share/nginx/html /var/cache/nginx /var/run /var/log/nginx /etc/nginx

COPY dashboard/nginx.conf /etc/nginx/nginx.conf
COPY --from=build /app/dist /usr/share/nginx/html

USER nginx
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=6 --start-period=5s \
  CMD wget -q -O /dev/null http://127.0.0.1:8080/ || exit 1

CMD ["nginx", "-g", "daemon off;"]
