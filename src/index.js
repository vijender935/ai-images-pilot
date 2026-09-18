/**
 * ai-images-pilot Worker
 * Bindings: AI (Workers AI), DB (D1), IMAGES (R2), VECTORIZE (Vectorize)
 * Vision model: @cf/meta/llama-3.2-11b-vision-instruct (Free tier)
 */

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS },
  });
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS });
    }

    const url = new URL(request.url);
    const path = url.pathname.replace(/\/$/, '') || '/';

    try {
      // Health / status
      if (path === '/' || path === '/health') {
        return json({
          status: 'ok',
          worker: 'ai-images-pilot',
          vision_model: '@cf/meta/llama-3.2-11b-vision-instruct',
          bindings: {
            AI: !!env.AI,
            DB: !!env.DB,
            IMAGES: !!env.IMAGES,
            VECTORIZE: !!env.VECTORIZE,
          },
        });
      }

      // List images from D1
      if (path === '/images' && request.method === 'GET') {
        const limit = Math.min(parseInt(url.searchParams.get('limit') || '20', 10), 100);
        const offset = parseInt(url.searchParams.get('offset') || '0', 10);
        const status = url.searchParams.get('status');

        let query = 'SELECT id, r2_key, content_type, size_bytes, description, tags, status, embedding_id, created_at, updated_at FROM images';
        const params = [];
        if (status) {
          query += ' WHERE status = ?';
          params.push(status);
        }
        query += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
        params.push(limit, offset);

        const { results } = await env.DB.prepare(query).bind(...params).all();
        return json({ images: results, count: results.length });
      }

      // Semantic search via Vectorize + D1 enrichment
      if (path === '/search' && request.method === 'GET') {
        const q = url.searchParams.get('q');
        if (!q) return json({ error: 'Missing query parameter q' }, 400);

        const topK = Math.min(parseInt(url.searchParams.get('topK') || '5', 10), 20);

        // Embed the query
        const embedRes = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
          text: [q],
        });
        const vector = embedRes.data?.[0];
        if (!vector) return json({ error: 'Failed to generate embedding' }, 500);

        // Query Vectorize
        const matches = await env.VECTORIZE.query(vector, {
          topK,
          returnMetadata: true,
        });

        // Enrich with D1 records
        const ids = (matches.matches || []).map(m => m.id).filter(Boolean);
        let dbRows = [];
        if (ids.length) {
          const placeholders = ids.map(() => '?').join(',');
          const { results } = await env.DB.prepare(
            `SELECT * FROM images WHERE id IN (${placeholders}) OR embedding_id IN (${placeholders})`
          ).bind(...ids, ...ids).all();
          dbRows = results;
        }

        return json({
          query: q,
          matches: matches.matches || [],
          images: dbRows,
        });
      }

      // Process an image: describe + embed + store
      if (path === '/process' && request.method === 'POST') {
        let body = {};
        try {
          body = await request.json();
        } catch {}

        let r2Key = body.r2_key;
        let imageId = body.id;

        // If no key given, pick a pending row or list R2 and create pending
        if (!r2Key) {
          const pending = await env.DB.prepare(
            "SELECT id, r2_key FROM images WHERE status = 'pending' LIMIT 1"
          ).first();
          if (pending) {
            imageId = pending.id;
            r2Key = pending.r2_key;
          }
        }

        if (!r2Key) {
          // Find an R2 object not yet in D1
          const listed = await env.IMAGES.list({ limit: 10 });
          for (const obj of listed.objects || []) {
            const exists = await env.DB.prepare(
              'SELECT id FROM images WHERE r2_key = ?'
            ).bind(obj.key).first();
            if (!exists) {
              r2Key = obj.key;
              break;
            }
          }
        }

        if (!r2Key) {
          return json({ error: 'No image to process' }, 404);
        }

        // Ensure a D1 row exists
        if (!imageId) {
          imageId = crypto.randomUUID();
          const obj = await env.IMAGES.head(r2Key);
          await env.DB.prepare(
            `INSERT INTO images (id, r2_key, etag, content_type, size_bytes, status, created_at, updated_at)
              VALUES (?, ?, ?, ?, ?, 'pending', datetime('now'), datetime('now'))`
          ).bind(
            imageId,
            r2Key,
            obj?.etag || null,
            obj?.httpMetadata?.contentType || 'image/jpeg',
            obj?.size || null
          ).run();
        }

        // Mark as processing
        await env.DB.prepare(
          "UPDATE images SET status = 'processing', updated_at = datetime('now') WHERE id = ?"
        ).bind(imageId).run();

        try {
          // Fetch image from R2
          const object = await env.IMAGES.get(r2Key);
          if (!object) throw new Error('Object not found in R2');

          const arrayBuffer = await object.arrayBuffer();
          // Convert to base64 for the vision model
          const bytes = new Uint8Array(arrayBuffer);
          let binary = '';
          for (let i = 0; i < bytes.length; i++) {
            binary += String.fromCharCode(bytes[i]);
          }
          const base64 = btoa(binary);

          // Describe image with Llama 3.2 11B Vision (Free)
          let description = '';
          try {
            const contentType = object.httpMetadata?.contentType || 'image/jpeg';
            const imageDataUrl = 'data:' + contentType + ';base64,' + base64;

            const vision = await env.AI.run('@cf/meta/llama-3.2-11b-vision-instruct', {
              messages: [
                { role: 'system', content: 'You are a helpful image catalog assistant.' },
                { role: 'user', content: 'Describe this image in detail for a fashion/lifestyle catalog. Include clothing, colors, style, mood, setting, and notable objects or people. Be specific and useful for semantic search.' },
              ],
              image: imageDataUrl,
              max_tokens: 1024,
            });

            // Response format can vary
            description =
              vision?.response ||
              vision?.result ||
              vision?.description ||
              vision?.choices?.[0]?.message?.content ||
              (typeof vision === 'string' ? vision : JSON.stringify(vision));
          } catch (visionErr) {
            description = `Image from R2 key: ${r2Key}`;
            console.warn('Vision model failed, using fallback description', visionErr.message);
          }

          // Embed the description
          const embedRes = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
            text: [description],
          });
          const vector = embedRes.data?.[0];
          if (!vector) throw new Error('Embedding failed');

          // Upsert into Vectorize
          const vectorId = imageId;
          await env.VECTORIZE.upsert([
            {
              id: vectorId,
              values: vector,
              metadata: {
                r2_key: r2Key,
                description,
              },
            },
          ]);

          // Update D1
          await env.DB.prepare(
            `UPDATE images
               SET description = ?, status = 'ready', embedding_id = ?, processed_at = datetime('now'), updated_at = datetime('now'), error = NULL
               WHERE id = ?`
          ).bind(description, vectorId, imageId).run();

          return json({
            success: true,
            id: imageId,
            r2_key: r2Key,
            description,
            embedding_id: vectorId,
            model: '@cf/meta/llama-3.2-11b-vision-instruct',
          });
        } catch (err) {
          await env.DB.prepare(
            "UPDATE images SET status = 'error', error = ?, updated_at = datetime('now') WHERE id = ?"
          ).bind(String(err.message || err), imageId).run();
          throw err;
        }
      }

      // List raw R2 objects (helper)
      if (path === '/r2' && request.method === 'GET') {
        const listed = await env.IMAGES.list({ limit: 20 });
        return json({
          objects: (listed.objects || []).map(o => ({
            key: o.key,
            size: o.size,
            etag: o.etag,
            uploaded: o.uploaded,
          })),
          truncated: listed.truncated,
        });
      }

      return json({ error: 'Not found', path }, 404);
    } catch (err) {
      console.error(err);
      return json({ error: err.message || String(err) }, 500);
    }
  },
};
