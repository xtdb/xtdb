FROM node:24.16.0
RUN corepack enable && corepack prepare yarn@1.22.22 --activate

WORKDIR /app

ENTRYPOINT ["yarn"]
CMD ["run", "test"]

COPY package.json package.json
COPY yarn.lock yarn.lock

RUN yarn install

COPY . .
