const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

const QUEUE_NAME = 'ai-images-process';
const STALE_AFTER_MINUTES = 30;
const DRAIN_CRON = '*/10 * * * *';
const DEFAULT_DRAIN_LIMIT = 40;

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS },
  });
}

function isStaleProcessing(row) {
  if (row?.status !== 'processing' || !row.updated_at) return false;
  const ts = Date.parse(String(row.updated_at).replace(' ', 'T') + 'Z');
  return Number.isFinite(ts) && ts < Date.now() - STALE_AFTER_MINUTES * 60 * 1000;
}

function toBase64(bytes) {
  let binary = '';
  const chunkSize = 0x8000;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
  }
  return btoa(binary);
}

async function loadExistingByKey(env) {
  const byKey = new Map();
  let offset = 0;
  const pageSize = 500;
  while (true) {
    const { results } = await env.DB.prepare(
      'SELECT id, r2_key, status, etag, updated_at FROM images LIMIT ? OFFSET ?'
    ).bind(pageSize, offset).all();
    const rows = results || [];
    for (const row of rows) byKey.set(row.r2_key, row);
    if (rows.length < pageSize) break;
    offset += pageSize;
  }
  return byKey;
}

async function sendQueueBatches(env, messages) {
  let queued = 0;
  for (let i = 0; i < messages.length; i += 100) {
    const batch = messages.slice(i, i + 100);
    if (!batch.length) continue;
    await env.PROCESS_QUEUE.sendBatch(batch);
    queued += batch.length;
  }
  return queued;
}

async function ensureImageRow(env, r2Key, imageId, object) {
  if (imageId) return imageId;
  const id = crypto.randomUUID();
  await env.DB.prepare(
    `INSERT INTO images (id, r2_key, etag, content_type, size_bytes, status, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, 'pending', datetime('now'), datetime('now'))`
  ).bind(
    id,
    r2Key,
    object?.etag || null,
    object?.httpMetadata?.contentType || 'image/jpeg',
    object?.size || null,
  ).run();
  return id;
}

async function processImage(env, r2Key, imageId) {
  if (!r2Key) throw new Error('Missing R2 key');
  const object = await env.IMAGES.get(r2Key);
  if (!object) throw new Error('Object not found in R2');
  imageId = await ensureImageRow(env, r2Key, imageId, object);

  await env.DB.prepare(
    "UPDATE images SET status = 'processing', updated_at = datetime('now'), error = NULL WHERE id = ?"
  ).bind(imageId).run();

  try {
    const arrayBuffer = await object.arrayBuffer();
    const bytes = new Uint8Array(arrayBuffer);
    const contentType = object.httpMetadata?.contentType || 'image/jpeg';
    const imageDataUrl = `data:${contentType};base64,${toBase64(bytes)}`;

    const vision = await env.AI.run('@cf/meta/llama-3.2-11b-vision-instruct', {
      messages: [
        { role: 'system', content: 'You are a helpful image catalog assistant.' },
        {
          role: 'user',
          content: 'Describe this fashion/lifestyle photo in 80-120 words. Include clothing, colors, style, mood, setting, and notable objects. Be specific for search.',
        },
      ],
      image: imageDataUrl,
      max_tokens: 400,
    });

    const description =
      vision?.response ||
      vision?.result ||
      vision?.description ||
      vision?.choices?.[0]?.message?.content ||
      (typeof vision === 'string' ? vision : JSON.stringify(vision));

    if (!description || description.startsWith('{')) {
      throw new Error('Vision model returned no usable description');
    }

    const embedRes = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
      text: [description],
    });
    const vector = embedRes.data?.[0];
    if (!vector) throw new Error('Embedding failed');

    await env.VECTORIZE.upsert([{
      id: imageId,
      values: vector,
      metadata: {
        r2_key: r2Key,
        description,
        description_source: 'vision',
      },
    }]);

    await env.DB.prepare(
      `UPDATE images
       SET description = ?, status = 'ready', embedding_id = ?,
           processed_at = datetime('now'), updated_at = datetime('now'), error = NULL,
           etag = ?, content_type = ?, size_bytes = ?
       WHERE id = ?`
    ).bind(
      description,
      imageId,
      object.etag || null,
      contentType,
      object.size || null,
      imageId,
    ).run();

    return { success: true, id: imageId, r2_key: r2Key, description, embedding_id: imageId };
  } catch (err) {
    await env.DB.prepare(
      "UPDATE images SET status = 'error', error = ?, updated_at = datetime('now') WHERE id = ?"
    ).bind(String(err.message || err), imageId).run();
    throw err;
  }
}

async function drainPending(env, limit = DEFAULT_DRAIN_LIMIT) {
  if (!env.PROCESS_QUEUE) throw new Error(`Missing queue binding PROCESS_QUEUE (${QUEUE_NAME})`);
  const cap = Math.max(1, Math.min(Number(limit) || DEFAULT_DRAIN_LIMIT, 100));

  await env.DB.prepare(
    `UPDATE images
     SET status = 'pending', error = NULL, updated_at = datetime('now')
     WHERE status = 'processing'
       AND updated_at < datetime('now', ?)`
  ).bind(`-${STALE_AFTER_MINUTES} minutes`).run();

  const { results } = await env.DB.prepare(
    `SELECT id, r2_key FROM images
     WHERE status IN ('pending', 'error')
     ORDER BY updated_at ASC
     LIMIT ?`
  ).bind(cap).all();

  const rows = results || [];
  const messages = rows
    .filter((row) => row.r2_key)
    .map((row) => ({ body: { type: 'process-image', r2_key: row.r2_key, id: row.id } }));

  const queued = await sendQueueBatches(env, messages);
  return { queue: QUEUE_NAME, selected: rows.length, queued, limit: cap };
}

async function enqueueAllImages(env) {
  if (!env.PROCESS_QUEUE) throw new Error(`Missing queue binding PROCESS_QUEUE (${QUEUE_NAME})`);
  const existingByKey = await loadExistingByKey(env);

  let cursor;
  let scanned = 0;
  let queued = 0;
  let skipped = 0;
  let pageCount = 0;

  do {
    const page = await env.IMAGES.list({ limit: 1000, cursor });
    pageCount++;
    const messages = [];

    for (const object of page.objects || []) {
      if (!object.key) continue;
      scanned++;
      const existing = existingByKey.get(object.key);
      const stale = isStaleProcessing(existing);
      const unchangedReady = existing?.status === 'ready' && existing.etag === object.etag;
      if (unchangedReady || (existing?.status === 'processing' && !stale)) {
        skipped++;
        continue;
      }

      const id = existing?.id || crypto.randomUUID();
      if (!existing) {
        await env.DB.prepare(
          `INSERT INTO images (id, r2_key, etag, content_type, size_bytes, status, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, 'pending', datetime('now'), datetime('now'))`
        ).bind(id, object.key, object.etag || null, object.httpMetadata?.contentType || 'image/jpeg', object.size || null).run();
        existingByKey.set(object.key, { id, r2_key: object.key, status: 'pending', etag: object.etag });
      } else {
        await env.DB.prepare(
          "UPDATE images SET status = 'pending', error = NULL, updated_at = datetime('now') WHERE id = ?"
        ).bind(id).run();
        existing.status = 'pending';
      }
      messages.push({ body: { type: 'process-image', r2_key: object.key, id } });
    }

    queued += await sendQueueBatches(env, messages);
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);

  return { queue: QUEUE_NAME, scanned, queued, skipped, pages: pageCount, stale_after_minutes: STALE_AFTER_MINUTES };
}

async function progress(env) {
  const { results } = await env.DB.prepare(
    `SELECT status, COUNT(*) AS count FROM images GROUP BY status ORDER BY status`
  ).all();
  const totals = Object.fromEntries((results || []).map((r) => [r.status, Number(r.count)]));
  const total = Object.values(totals).reduce((a, b) => a + b, 0);
  return { total, counts: totals, complete: total > 0 && (totals.ready || 0) === total };
}

export default {
  async scheduled(event, env, ctx) {
    const job = event.cron === DRAIN_CRON
      ? drainPending(env, DEFAULT_DRAIN_LIMIT)
      : enqueueAllImages(env);
    ctx.waitUntil(job.then((r) => console.log('scheduled job complete', event.cron, r)).catch((e) => console.error('scheduled job failed', event.cron, e)));
  },

  async queue(batch, env) {
    for (const message of batch.messages) {
      const job = message.body;
      if (!job?.r2_key) {
        message.ack();
        continue;
      }
      try {
        await processImage(env, job.r2_key, job.id);
        message.ack();
      } catch (err) {
        console.error('Queue image processing failed', job.r2_key, err);
        message.retry({ delaySeconds: 10 });
      }
    }
  },

  async fetch(request, env) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/$/, '') || '/';

    try {
      if (path === '/' || path === '/health') {
        return json({ status: 'ok', worker: 'ai-images-pilot', queue: QUEUE_NAME, bindings: {
          AI: !!env.AI, DB: !!env.DB, IMAGES: !!env.IMAGES, VECTORIZE: !!env.VECTORIZE, PROCESS_QUEUE: !!env.PROCESS_QUEUE,
        }});
      }
      if (path === '/progress' && request.method === 'GET') return json(await progress(env));
      if (path === '/enqueue' && request.method === 'POST') return json(await enqueueAllImages(env));
      if (path === '/drain' && request.method === 'POST') {
        const limit = url.searchParams.get('limit') || DEFAULT_DRAIN_LIMIT;
        return json(await drainPending(env, limit));
      }

      if (path === '/images' && request.method === 'GET') {
        const limit = Math.min(parseInt(url.searchParams.get('limit') || '20', 10), 100);
        const offset = parseInt(url.searchParams.get('offset') || '0', 10);
        const status = url.searchParams.get('status');
        let query = 'SELECT id, r2_key, content_type, size_bytes, description, tags, status, embedding_id, created_at, updated_at, error FROM images';
        const params = [];
        if (status) { query += ' WHERE status = ?'; params.push(status); }
        query += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
        params.push(limit, offset);
        const { results } = await env.DB.prepare(query).bind(...params).all();
        return json({ images: results, count: results.length });
      }

      if (path === '/search' && request.method === 'GET') {
        const q = url.searchParams.get('q');
        if (!q) return json({ error: 'Missing query parameter q' }, 400);
        const topK = Math.min(parseInt(url.searchParams.get('topK') || '5', 10), 20);
        const embedRes = await env.AI.run('@cf/baai/bge-base-en-v1.5', { text: [q] });
        const vector = embedRes.data?.[0];
        if (!vector) return json({ error: 'Failed to generate embedding' }, 500);
        const matches = await env.VECTORIZE.query(vector, { topK, returnMetadata: true });
        const ids = (matches.matches || []).map((m) => m.id).filter(Boolean);
        let dbRows = [];
        if (ids.length) {
          const placeholders = ids.map(() => '?').join(',');
          const { results } = await env.DB.prepare(`SELECT * FROM images WHERE id IN (${placeholders}) OR embedding_id IN (${placeholders})`).bind(...ids, ...ids).all();
          dbRows = results;
        }
        return json({ query: q, matches: matches.matches || [], images: dbRows });
      }

      if (path === '/image' && request.method === 'GET') {
        const key = url.searchParams.get('key');
        if (!key) return json({ error: 'Missing query parameter key' }, 400);
        const object = await env.IMAGES.get(key);
        if (!object) return json({ error: 'Image not found', key }, 404);
        const headers = new Headers(CORS);
        headers.set('Content-Type', object.httpMetadata?.contentType || 'application/octet-stream');
        headers.set('Cache-Control', 'private, max-age=300');
        return new Response(object.body, { headers });
      }

      if (path === '/process' && request.method === 'POST') {
        const body = await request.json().catch(() => ({}));
        let r2Key = body.r2_key;
        let imageId = body.id;
        if (!r2Key) {
          const pending = await env.DB.prepare("SELECT id, r2_key FROM images WHERE status IN ('pending','error') ORDER BY updated_at ASC LIMIT 1").first();
          if (pending) { imageId = pending.id; r2Key = pending.r2_key; }
        }
        if (!r2Key) return json({ error: 'No image to process' }, 404);
        return json(await processImage(env, r2Key, imageId));
      }

      if (path === '/r2' && request.method === 'GET') {
        const listed = await env.IMAGES.list({ limit: 1000 });
        return json({ objects: listed.objects || [], truncated: listed.truncated, cursor: listed.cursor });
      }
      return json({ error: 'Not found', path }, 404);
    } catch (err) {
      console.error(err);
      return json({ error: err.message || String(err) }, 500);
    }
  },
};
