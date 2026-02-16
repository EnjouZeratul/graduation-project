FROM node:20-alpine AS build

WORKDIR /app
RUN npm config set registry https://registry.npmmirror.com
COPY package.json tsconfig.json vite.config.ts index.html ./
COPY src ./src

RUN npm install && npm run build

FROM nginx:1.27-alpine

COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80

