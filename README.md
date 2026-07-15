Docker Compose
```
services:
  weather-dashboard:
    image: ghcr.io/aweeri/weerweather:latest
    container_name: weather-dash
    ports:
      - 3005:3000
    restart: unless-stopped
    environment:
      - NODE_ENV=production
networks: {}
```
