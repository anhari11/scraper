FROM mcr.microsoft.com/playwright:v1.44.0-jammy

WORKDIR /app

COPY package.json ./

COPY . .

RUN npm install

EXPOSE 3000

CMD ["npm", "start"]