## ビルド

```bash
docker build -t yt-api .
```

## 実行

```bash
docker run -d \
  --name yt-api \
  --restart=always \
  -p 3000:3000 \
  -e WORKER_SECRET=xxx \
  -e PROXY_URL=http://proxy:port \
  yt-api
```
