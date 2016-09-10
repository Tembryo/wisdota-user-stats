FROM node:5

VOLUME ["/source"]
WORKDIR /source

CMD ["node", "user-stats.js"]
