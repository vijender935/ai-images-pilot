# ai-images-pilot

Cloudflare Worker for AI image processing, description, embedding and semantic search.

## Bindings

- **AI** – Workers AI (vision + embeddings)
- **DB** – D1 database (`ai-images-db`)
- **IMAGES** – R2 bucket (`ai-images`)
- **VECTORIZE** – Vectorize index (`ai-images-index`)

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` or `/health` | Health check + binding status |
| GET | `/images` | List images from D1 (`?limit=&offset=&status=`) |
| GET | `/search?q=` | Semantic search via Vectorize |
| POST | `/process` | Describe image (vision), embed, store |
| GET | `/r2` | List raw R2 objects |

## Deploy

This repo is ready to connect to the existing Cloudflare Worker `ai-images-pilot`.

1. Go to Cloudflare Dashboard → Workers & Pages → **ai-images-pilot**
2. Settings → Builds → **Connect**
3. Select this GitHub repository (`vijender935/ai-images-pilot`)
4. Set root directory to `/` and ensure Worker name matches `ai-images-pilot`
5. Save and push a commit (or trigger deploy)

Local development:

```bash
npm install
npx wrangler dev
```
