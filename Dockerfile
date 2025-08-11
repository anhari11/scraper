

FROM mcr.microsoft.com/playwright:v1.54.2-jammy

WORKDIR /app

COPY package.json ./

COPY . .

RUN npm install

CMD ["npm", "start"]