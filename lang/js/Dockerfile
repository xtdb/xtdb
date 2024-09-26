FROM node:latest

WORKDIR /app

ENTRYPOINT ["yarn"]
CMD ["run", "test"]

COPY package.json package.json
COPY yarn.lock yarn.lock

RUN yarn install

COPY . .

