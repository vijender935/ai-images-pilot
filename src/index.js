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


async function processImage(env, r2Key, imageId) {
  if (!r2Key) throw new Error('Missing R2 key');

  // Ensure a D1 row exists
  if (!imageId) {
    imageId = crypto.randomUUID();
    const obj = await env.IMAGES.head(r2Key);
    if (!obj) throw new Error('Object not found in R2');
    await env.DB.prepare(
      `INSERT INTO images (id, r2_key, etag, content_type, size_bytes, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, 'pending', datetime('now'), datetime('now'))`
    ).bind(
      imageId,
      r2Key,
      obj.etag || null,
      obj.httpMetadata?.contentType || 'image/jpeg',
      obj.size || null
    ).run();
  }

  await env.DB.prepare(
    "UPDATE images SET status = 'processing', updated_at = datetime('now') WHERE id = ?"
  ).bind(imageId).run();

  try {
    const object = await env.IMAGES.get(r2Key);
    if (!object) throw new Error('Object not found in R2');

    const arrayBuffer = await object.arrayBuffer();
    const bytes = new Uint8Array(arrayBuffer);
    let binary = '';
    for (let i = 0; i < bytes.length; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    const base64 = btoa(binary);

    let description = '';
    let descriptionSource = 'vision';
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

      description =
        vision?.response ||
        vision?.result ||
        vision?.description ||
        vision?.choices?.[0]?.message?.content ||
        (typeof vision === 'string' ? vision : JSON.stringify(vision));
    } catch (visionErr) {
      description = `Image from R2 key: ${r2Key}`;
      descriptionSource = 'fallback';
      console.warn('Vision model failed, using fallback description', visionErr.message);
    }

    const embedRes = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
      text: [description],
    });
    const vector = embedRes.data?.[0];
    if (!vector) throw new Error('Embedding failed');

    const vectorId = imageId;
    await env.VECTORIZE.upsert([
      {
        id: vectorId,
        values: vector,
        metadata: {
          r2_key: r2Key,
          description,
          description_source: descriptionSource,
        },
      },
    ]);

    await env.DB.prepare(
      `UPDATE images
         SET description = ?, status = 'ready', embedding_id = ?, processed_at = datetime('now'), updated_at = datetime('now'), error = NULL
         WHERE id = ?`
    ).bind(description, vectorId, imageId).run();

    return {
      success: true,
      id: imageId,
      r2_key: r2Key,
      description,
      embedding_id: vectorId,
      model: '@cf/meta/llama-3.2-11b-vision-instruct',
    };
  } catch (err) {
    await env.DB.prepare(
      "UPDATE images SET status = 'error', error = ?, updated_at = datetime('now') WHERE id = ?"
    ).bind(String(err.message || err), imageId).run();
    throw err;
  }
}

export default {
  async scheduled(event, env, ctx) {
    // Daily fallback scanner for accounts where R2 Event Notifications are unavailable.
    const listed = await env.IMAGES.list({ limit: 100 });
    let processed = 0;

    for (const obj of listed.objects || []) {
      if (!obj.key) continue;

      const existing = await env.DB.prepare(
        'SELECT id, status, etag FROM images WHERE r2_key = ?'
      ).bind(obj.key).first();

      if (existing?.status === 'ready' && existing?.etag === obj.etag) continue;
      if (existing?.status === 'processing') continue;

      try {
        const imageId = existing?.id || crypto.randomUUID();

        if (!existing) {
          await env.DB.prepare(
            `INSERT INTO images (id, r2_key, etag, content_type, size_bytes, status, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, 'pending', datetime('now'), datetime('now'))`
          ).bind(
            imageId,
            obj.key,
            obj.etag || null,
            obj.httpMetadata?.contentType || 'image/jpeg',
            obj.size || null
          ).run();
        }

        await processImage(env, obj.key, imageId);
        processed++;
      } catch (err) {
        console.error('Scheduled image processing failed:', obj.key, err);
      }
    }

    console.log(`Daily scan complete. Processed: ${processed}`);
  },

  async queue(batch, env, ctx) {
    for (const message of batch.messages) {
      const event = message.body;
      const r2Key = event?.object?.key;
      if (!r2Key || event?.bucket !== 'ai-images') {
        message.ack();
        continue;
      }

      try {
        // Ignore deletes; this queue is configured for object-create.
        const existing = await env.DB.prepare(
          'SELECT id, status, etag FROM images WHERE r2_key = ?'
        ).bind(r2Key).first();

        if (existing?.status === 'ready' && existing?.etag === event?.object?.eTag) {
          message.ack();
          continue;
        }

        const imageId = existing?.id || crypto.randomUUID();
        if (!existing) {
          await env.DB.prepare(
            `INSERT INTO images (id, r2_key, etag, content_type, size_bytes, status, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, 'pending', datetime('now'), datetime('now'))`
          ).bind(
            imageId,
            r2Key,
            event?.object?.eTag || null,
            'image/jpeg',
            event?.object?.size || null
          ).run();
        }

        await processImage(env, r2Key, imageId);
        message.ack();
        console.log('Automatically processed R2 object:', r2Key);
      } catch (err) {
        console.error('Automatic image processing failed:', r2Key, err);
        message.retry({ delaySeconds: 30 });
      }
    }
  },

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

      // Retrieve a single image from the private R2 bucket.
      // The MCP gateway uses this endpoint through a Cloudflare Service Binding,
      // so the R2 bucket itself does not need a public URL.
      if (path === '/image' && request.method === 'GET') {
        const key = url.searchParams.get('key');
        if (!key) return json({ error: 'Missing query parameter key' }, 400);

        const object = await env.IMAGES.get(key);
        if (!object) return json({ error: 'Image not found', key }, 404);

        const headers = new Headers(CORS);
        headers.set(
          'Content-Type',
          object.httpMetadata?.contentType || 'application/octet-stream'
        );
        headers.set('Cache-Control', 'private, max-age=300');
        if (object.httpEtag) headers.set('ETag', object.httpEtag);

        return new Response(object.body, { headers });
      }

      // Process an image manually or as a fallback.
      if (path === '/process' && request.method === 'POST') {
        let body = {};
        try {
          body = await request.json();
        } catch {}

        let r2Key = body.r2_key;
        let imageId = body.id;

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

        if (!r2Key) return json({ error: 'No image to process' }, 404);

        try {
          const result = await processImage(env, r2Key, imageId);
          return json(result);
        } catch (err) {
          console.error('Manual processing failed', err);
          return json({ error: err.message || String(err) }, 500);
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
