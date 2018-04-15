FROM node:9.10.1

RUN mkdir /app
WORKDIR /app

COPY package.json yarn.lock /app/
RUN yarn install

COPY . /app
