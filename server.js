// server.js
import express from 'express';
import fetch from 'node-fetch';
import dotenv from 'dotenv';
import cron from 'node-cron';

dotenv.config();
const app = express();
app.use(express.json());

/* ===================== ENV ===================== */
const {
  SHOP,
  ADMIN_TOKEN,

  // 컬렉션 설정
  DISCOUNT_COLLECTIONS,
  RANDOM_COLLECTIONS,
  HIGHSTOCK_COLLECTIONS,

  // 크론 (기본 30분)
  AUTO_CRON = '*/30 * * * *',

  // 수동/신호 트리거 보안
  TRIGGER_TOKEN,

  // GraphQL 튜닝
  MOVES_CHUNK = 100,
  GQL_MAX_RETRIES = 8,
  GQL_BASE_DELAY_MS = 800,

  // 최소 실행 간격 (기본 5분)
  MIN_GAP_MS = 5 * 60 * 1000,

  PORT = 3000
} = process.env;

const API_VERSION = '2024-07';
const GQL_ENDPOINT = `https://${SHOP}/admin/api/${API_VERSION}/graphql.json`;

/* ===================== Utils ===================== */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const parseIds = (csv) => (csv || '').split(',').map(s => s.trim()).filter(Boolean);

/* ===================== GraphQL with retry ===================== */
async function gql(query, variables = {}) {
  let attempt = 0;
  let lastErr;

  while (attempt <= Number(GQL_MAX_RETRIES)) {
    try {
      const res = await fetch(GQL_ENDPOINT, {
        method: 'POST',
        headers: {
          'X-Shopify-Access-Token': ADMIN_TOKEN,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query, variables })
      });

      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${await res.text()}`);
      }

      const json = await res.json();

      if (json.errors?.length) {
        const throttled = json.errors.some(e =>
          e?.extensions?.code === 'THROTTLED'
        );
        if (throttled) {
          await sleep(Number(GQL_BASE_DELAY_MS) * Math.pow(2, attempt++));
          continue;
        }
        throw new Error(JSON.stringify(json.errors));
      }

      return json.data;
    } catch (e) {
      lastErr = e;
      if (attempt >= Number(GQL_MAX_RETRIES)) break;
      await sleep(Number(GQL_BASE_DELAY_MS) * Math.pow(2, attempt++));
    }
  }
  throw lastErr;
}

/* ===================== Run Guards ===================== */
const running = { discount: false, random: false, stock: false };
const lastRunAt = { discount: 0, random: 0, stock: 0 };

async function guardRun(kind, fn) {
  const now = Date.now();
  if (running[kind]) return;
  if (now - lastRunAt[kind] < Number(MIN_GAP_MS)) return;

  running[kind] = true;
  try {
    await fn();
    lastRunAt[kind] = Date.now();
  } finally {
    running[kind] = false;
  }
}

/* ===================== Collection Lock ===================== */
const colLock = new Set();
async function withCollectionLock(colId, fn) {
  if (colLock.has(colId)) return;
  colLock.add(colId);
  try { await fn(); }
  finally { colLock.delete(colId); }
}

/* ===================== Core Runners ===================== */
async function runDiscountSorts() {
  await guardRun('discount', async () => {
    const ids = parseIds(DISCOUNT_COLLECTIONS);
    console.log('[RUN] discount:', ids);

    for (const colId of ids) {
      await withCollectionLock(colId, async () => {
        if (!(await ensureManualSort(colId))) return;
        await sortCollectionByDiscount(colId);
        console.log('[DONE] discount', colId);
      });
    }
  });
}

async function runRandomSorts() {
  await guardRun('random', async () => {
    const ids = parseIds(RANDOM_COLLECTIONS);
    console.log('[RUN] random:', ids);

    for (const colId of ids) {
      await withCollectionLock(colId, async () => {
        if (!(await ensureManualSort(colId))) return;
        await shuffleCollection(colId);
        console.log('[DONE] random', colId);
      });
    }
  });
}

async function runHighStockSorts() {
  await guardRun('stock', async () => {
    const ids = parseIds(HIGHSTOCK_COLLECTIONS);
    console.log('[RUN] stock:', ids);

    for (const colId of ids) {
      await withCollectionLock(colId, async () => {
        if (!(await ensureManualSort(colId))) return;
        await sortCollectionByStockDesc(colId);
        console.log('[DONE] stock', colId);
      });
    }
  });
}

/* ===================== HTTP Trigger (Python → 할인 정렬) ===================== */
app.post('/signal/discount', async (req, res) => {
  const token = req.headers['x-trigger-token'] || req.query.key;
  if (TRIGGER_TOKEN && token !== TRIGGER_TOKEN) {
    return res.status(401).send('unauthorized');
  }

  res.send('discount sort started');
  runDiscountSorts().catch(e => console.error('[SIGNAL discount]', e));
});

/* ===================== HTTP Trigger (수동 실행) ===================== */
app.post('/signal/discount', async (req, res) => {
  const token = req.headers['x-trigger-token'] || req.query.key;
  if (TRIGGER_TOKEN && token !== TRIGGER_TOKEN) {
    return res.status(401).send('unauthorized');
  }

  res.send('discount sort started');
  runDiscountSorts().catch(e => console.error('[SIGNAL discount]', e));
});

app.post('/signal/random', async (req, res) => {
  const token = req.headers['x-trigger-token'] || req.query.key;
  if (TRIGGER_TOKEN && token !== TRIGGER_TOKEN) {
    return res.status(401).send('unauthorized');
  }

  res.send('random sort started');
  runRandomSorts().catch(e => console.error('[SIGNAL random]', e));
});

app.post('/signal/stock', async (req, res) => {
  const token = req.headers['x-trigger-token'] || req.query.key;
  if (TRIGGER_TOKEN && token !== TRIGGER_TOKEN) {
    return res.status(401).send('unauthorized');
  }

  res.send('highstock sort started');
  runHighStockSorts().catch(e => console.error('[SIGNAL stock]', e));
});


/* ===================== Root / Health ===================== */
app.get('/', (_req, res) => res.send('Auto Sortify running'));
app.get('/healthz', (_req, res) => res.send('ok'));

app.get('/debug', (_req, res) => {
  res.json({
    running,
    lastRunAt,
    collections: {
      DISCOUNT_COLLECTIONS,
      RANDOM_COLLECTIONS,
      HIGHSTOCK_COLLECTIONS
    }
  });
});

/* ===================== Shopify Logic ===================== */
async function ensureManualSort(collectionId) {
  const q = `query($id:ID!){ collection(id:$id){ id sortOrder } }`;
  const d = await gql(q, { id: collectionId });
  if (!d.collection) return false;

  if (d.collection.sortOrder !== 'MANUAL') {
    const m = `
      mutation($id:ID!){
        collectionUpdate(input:{id:$id, sortOrder:MANUAL}){
          collection{ id }
        }
      }
    `;
    await gql(m, { id: collectionId });
  }
  return true;
}

async function fetchCollectionProducts(collectionId) {
  const products = [];
  let cursor = null;

  const q = `
    query($id:ID!, $cursor:String){
      collection(id:$id){
        products(first:250, after:$cursor){
          pageInfo{ hasNextPage }
          edges{
            cursor
            node{
              id
              totalInventory
              variants(first:100){
                nodes{ price compareAtPrice }
              }
            }
          }
        }
      }
    }
  `;

  while (true) {
    const d = await gql(q, { id: collectionId, cursor });
    const edges = d.collection.products.edges;
    edges.forEach(e => products.push(e.node));
    if (!d.collection.products.pageInfo.hasNextPage) break;
    cursor = edges.at(-1).cursor;
  }
  return products;
}

function maxDiscount(p) {
  let max = 0;
  for (const v of p.variants.nodes) {
    const price = +v.price || 0;
    const cmp = +v.compareAtPrice || 0;
    if (cmp > price) max = Math.max(max, (cmp - price) / cmp);
  }
  return max;
}

async function sortCollectionByDiscount(colId) {
  const list = await fetchCollectionProducts(colId);
  const inStock = list.filter(p => p.totalInventory > 0);
  const out = list.filter(p => p.totalInventory <= 0);

  inStock.sort((a, b) => maxDiscount(b) - maxDiscount(a));
  await reorder(colId, [...inStock, ...out].map(p => p.id));
}

async function shuffleCollection(colId) {
  const list = await fetchCollectionProducts(colId);
  const inStock = list.filter(p => p.totalInventory > 0);
  const out = list.filter(p => p.totalInventory <= 0);

  for (let i = inStock.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [inStock[i], inStock[j]] = [inStock[j], inStock[i]];
  }
  await reorder(colId, [...inStock, ...out].map(p => p.id));
}

async function sortCollectionByStockDesc(colId) {
  const list = await fetchCollectionProducts(colId);
  const inStock = list.filter(p => p.totalInventory > 0);
  const out = list.filter(p => p.totalInventory <= 0);

  inStock.sort((a, b) => b.totalInventory - a.totalInventory);
  await reorder(colId, [...inStock, ...out].map(p => p.id));
}

async function reorder(collectionId, newOrder) {
  const moves = newOrder.map((id, i) => ({ id, newPosition: String(i) }));
  for (let i = 0; i < moves.length; i += Number(MOVES_CHUNK)) {
    const part = moves.slice(i, i + Number(MOVES_CHUNK));
    const m = `
      mutation($id:ID!, $moves:[MoveInput!]!){
        collectionReorderProducts(id:$id, moves:$moves){
          job{ id }
        }
      }
    `;
    await gql(m, { id: collectionId, moves: part });
  }
}

/* ===================== CRON ===================== */
cron.schedule(AUTO_CRON, async () => {
  console.log('[CRON] auto sort tick');
  try {
    await runRandomSorts();
    await runHighStockSorts();
  } catch (e) {
    console.error('[CRON] error', e);
  }
});

/* ===================== START ===================== */
app.listen(PORT, () => {
  console.log(`Auto Sortify running on :${PORT}`);
});
