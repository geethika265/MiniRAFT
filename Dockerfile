FROM node:18

WORKDIR /app

COPY gateway/package*.json ./gateway/
COPY replica/package*.json ./replica/

RUN cd gateway && npm install
RUN cd replica && npm install

COPY . .

EXPOSE 3000 8080 5001 5002 5003
