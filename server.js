// server.js
import express from 'express';
import fetch from 'node-fetch';
import crypto from 'crypto';
import dotenv from 'dotenv';
import cron from 'node-cron';

dotenv.config();
const app = express();

/* ===================== Body parsers ===================== */
// 웹훅은 HMAC 검증을 위해 raw body 필요
app.use('/webhooks', express.raw({ type: '*/*' }));
// 나머지는 일반 JSON
app.use(express.json());

/* ===================== ENV ===================== */
const {
  SHOP,
  ADMIN_TOKEN,
  API_SECRET,
  WEBHOOK_SHARED_SECRET,        // (옵션) Admin 설정페이지에서 만든 웹훅의 서명 키

  // CSV (콤마 구분, 공백 없이 권장)
  DISCOUNT_COLLECTIONS,
  RANDOM_COLLECTIONS,
  HIGHSTOCK_COLLECTIONS,

  // 크론(미지정 시 30분)
  RANDOM_CRON,
  HIGHSTOCK_CRON,

  // 수동 트리거 토큰
  TRIGGER_TOKEN,

  // 레이트리밋 튜닝
  MOVES_CHUNK = 100,
  GQL_MAX_RETRIES = 8,
  GQL_BASE_DELAY_MS = 800,

  // 부팅 시 웹훅 자동 등록 스위치 (기본 끔)
  AUTO_REGISTER_WEBHOOKS = '0',

  PORT = 3000
} = process.env;

const API_VERSION = '2024-07';
const GQL_ENDPOINT = `https://${SHOP}/admin/api/${API_VERSION}/graphql.json`;

/* ===================== GraphQL with retry/backoff ===================== */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function gqlWithRetry(query, variables = {}) {
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
        const text = await res.text();
        throw new Error(`HTTP ${res.status} from Shopify: ${text}`);
      }

      const json = await res.json();

      if (json.errors && json.errors.length) {
        const throttled = json.errors.some(
          (e) => e?.extensions?.code === 'THROTTLED' || /throttled/i.test(e?.message ?? '')
        );
        if (throttled) {
          await sleep(Number(GQL_BASE_DELAY_MS) * Math.pow(2, attempt));
          attempt++;
          continue;
        }
        throw new Error('GraphQL errors: ' + JSON.stringify(json.errors));
      }

      if (json.data?.userErrors?.length) {
        throw new Error('User errors: ' + JSON.stringify(json.data.userErrors));
      }

      const throttle = json?.extensions?.cost?.throttleStatus;
      if (throttle && typeof throttle.currentlyAvailable === 'number') {
        if (throttle.currentlyAvailable < 10) await sleep(1000);
      }

      return json.data;
    } catch (e) {
      lastErr = e;
      if (attempt >= Number(GQL_MAX_RETRIES)) break;
      await sleep(Number(GQL_BASE_DELAY_MS) * Math.pow(2, attempt));
      attempt++;
    }
  }
  throw lastErr ?? new Error('Unknown GraphQL failure');
}
const gql = gqlWithRetry;

/* ===================== HMAC (웹훅 보안) ===================== */
function safeEqual(a, b) {
  try { return crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b)); }
  catch { return false; }
}
function hmacDigest(secret, bodyBuf) {
  return crypto.createHmac('sha256', secret).update(bodyBuf).digest('base64');
}
function verifyWebhook(req) {
  try {
    const hmac = req.headers['x-shopify-hmac-sha256'] || '';
    const body = req.body; // Buffer (express.raw)
    const okApp = API_SECRET ? safeEqual(hmacDigest(API_SECRET, body), hmac) : false;
    const okShared = WEBHOOK_SHARED_SECRET
      ? safeEqual(hmacDigest(WEBHOOK_SHARED_SECRET, body), hmac)
      : false;
    return okApp || okShared;
  } catch {
    return false;
  }
}

/* ===================== 유틸 / 동시 실행 가드 ===================== */
const parseIds = (csv) => (csv || '').split(',').map(s => s.trim()).filter(Boolean);

const running = { discount: false, random: false, stock: false };
async function guardRun(kind, fn) {
  if (running[kind]) { console.log(`[GUARD] ${kind} already running. skip`); return; }
  running[kind] = true;
  try { await fn(); } finally { running[kind] = false; }
}

/* ===================== 디바운스 스케줄러 ===================== */
const debounceState = {
  discount: { timer: null, firstAt: 0 },
  stock:    { timer: null, firstAt: 0 },
};

function scheduleDebounced(kind, runFn, debounceMs, maxWaitMs) {
  const st = debounceState[kind];
  const now = Date.now();
  if (!st.firstAt) st.firstAt = now;      // 폭주 시작 시간 기록
  if (st.timer) clearTimeout(st.timer);   // 기존 타이머 취소

  const elapsed = now - st.firstAt;
  const delay   = (maxWaitMs && elapsed >= maxWaitMs) ? 0 : debounceMs;

  st.timer = setTimeout(async () => {
    const waited = ((Date.now() - st.firstAt) / 1000).toFixed(1);
    console.log(`[DEBOUNCE ${kind}] firing after ${waited}s calm`);
    try {
      await runFn();
    } catch (e) {
      console.error(`[DEBOUNCE ${kind}] error:`, e);
    } finally {
      // 리셋
      st.timer = null;
      st.firstAt = 0;
    }
  }, delay);
}

/* ===================== 웹훅 ===================== */
// 재고 변경 → 재고정렬(디바운스)
app.post('/webhooks/inventory-levels-update', async (req, res) => {
  if (!verifyWebhook(req)) return res.status(401).send('Invalid HMAC');
  res.status(200).send('ok');

  const debounceMs = Number(process.env.STOCK_DEBOUNCE_MS || 120000);
  const maxWaitMs  = Number(process.env.STOCK_MAXWAIT_MS  || 600000);
  scheduleDebounced('stock', runHighStockSorts, debounceMs, maxWaitMs);
});

// 상품/가격 변경 → 할인정렬(디바운스)
app.post('/webhooks/products-update', async (req, res) => {
  if (!verifyWebhook(req)) return res.status(401).send('Invalid HMAC');
  res.status(200).send('ok');

  const debounceMs = Number(process.env.DISCOUNT_DEBOUNCE_MS || 120000);
  const maxWaitMs  = Number(process.env.DISCOUNT_MAXWAIT_MS  || 600000);
  scheduleDebounced('discount', runDiscountSorts, debounceMs, maxWaitMs);
});
/* ===================== 수동 실행(랜덤/전체) & 루트 ===================== */
app.post('/run-random', async (req, res) => {
  const token = req.headers['x-trigger-token'] || req.query.key;
  if (TRIGGER_TOKEN && token !== TRIGGER_TOKEN) return res.status(401).send('unauthorized');
  res.send('random started');
  runRandomSorts().catch(e => console.error('[RUN-RANDOM] error:', e));
});

app.get('/run-now', async (_req, res) => {
  try {
    res.send('started');
    (async () => {
      await runDiscountSorts();
      await runRandomSorts();
      await runHighStockSorts();
      console.log('[RUNNOW] finished all');
    })().catch(e => console.error('[RUNNOW] error:', e));
  } catch (e) {
    console.error('[RUNNOW] fatal:', e);
    res.status(500).send('Error: ' + (e?.message || e));
  }
});

app.get('/', (_req, res) => {
  res.send('Auto Sortify is running. Use /run-random (POST) or wait for webhooks.');
});

/* ===================== 실행 플로우 ===================== */
async function runDiscountSorts() {
  await guardRun('discount', async () => {
    const ids = parseIds(DISCOUNT_COLLECTIONS);
    console.log('[RUN] discount:', ids);
    for (const colId of ids) {
      const ok = await ensureManualSort(colId);
      if (!ok) { console.log('[RUN] discount->NOT FOUND', colId); continue; }
      await sortCollectionByDiscount(colId, true);
      console.log('[RUN] discount->done', colId);
    }
  });
}

async function runRandomSorts() {
  await guardRun('random', async () => {
    const ids = parseIds(RANDOM_COLLECTIONS);
    console.log('[RUN] random  :', ids);
    for (const colId of ids) {
      const ok = await ensureManualSort(colId);
      if (!ok) { console.log('[RUN] random->NOT FOUND', colId); continue; }
      await shuffleCollection(colId, true);
      console.log('[RUN] random->done', colId);
    }
  });
}

async function runHighStockSorts() {
  await guardRun('stock', async () => {
    const ids = parseIds(HIGHSTOCK_COLLECTIONS);
    console.log('[RUN] highstk :', ids);
    for (const colId of ids) {
      const ok = await ensureManualSort(colId);
      if (!ok) { console.log('[RUN] highstk->NOT FOUND', colId); continue; }
      await sortCollectionByStockDesc(colId, true);
      console.log('[RUN] highstk->done', colId);
    }
  });
}

/* ===================== 정렬 로직 ===================== */
async function ensureManualSort(collectionId) {
  const q = `query($id: ID!){ collection(id:$id){ id sortOrder title } }`;
  const d = await gql(q, { id: collectionId });
  if (!d.collection) { console.error('[ensureManualSort] Collection NOT FOUND:', collectionId); return false; }
  if (d.collection.sortOrder !== 'MANUAL') {
    const m = `
      mutation($id: ID!){
        collectionUpdate(input:{id:$id, sortOrder:MANUAL}){
          collection{ id sortOrder }
          userErrors{ field message }
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
    query($id: ID!, $cursor: String){
      collection(id:$id){
        id
        products(first:250, after:$cursor){
          pageInfo{ hasNextPage }
          edges{
            cursor
            node{
              id
              title
              totalInventory
              variants(first:100){
                nodes{
                  id
                  price
                  compareAtPrice
                  inventoryQuantity
                }
              }
            }
          }
        }
      }
    }
  `;
  while (true) {
    const d = await gql(q, { id: collectionId, cursor });
    if (!d.collection) break;
    const edges = d.collection.products.edges;
    for (const e of edges) products.push(e.node);
    if (!d.collection.products.pageInfo.hasNextPage) break;
    cursor = edges[edges.length - 1].cursor;
  }
  return products;
}

function maxDiscountOfProduct(p) {
  let max = 0;
  for (const v of (p.variants?.nodes || [])) {
    const price = parseFloat(v.price ?? '0');
    const cmp = v.compareAtPrice ? parseFloat(v.compareAtPrice) : 0;
    if (cmp > price && cmp > 0) {
      const d = (cmp - price) / cmp;
      if (d > max) max = d;
    }
  }
  return max;
}

function splitByStock(products) {
  const inStock = [], out = [];
  for (const p of products) ((p.totalInventory ?? 0) > 0 ? inStock : out).push(p);
  return { inStock, out };
}

async function sortCollectionByDiscount(collectionId, pushOOSBottom) {
  const list = await fetchCollectionProducts(collectionId);
  const { inStock, out } = splitByStock(list);
  inStock.sort((a, b) => maxDiscountOfProduct(b) - maxDiscountOfProduct(a));
  const target = pushOOSBottom ? [...inStock, ...out] : inStock.concat(out);
  await reorderToTarget(collectionId, list.map(p => p.id), target.map(p => p.id));
}

async function shuffleCollection(collectionId, pushOOSBottom) {
  const list = await fetchCollectionProducts(collectionId);
  const { inStock, out } = splitByStock(list);
  for (let i = inStock.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [inStock[i], inStock[j]] = [inStock[j], inStock[i]];
  }
  const target = pushOOSBottom ? [...inStock, ...out] : inStock.concat(out);
  await reorderToTarget(collectionId, list.map(p => p.id), target.map(p => p.id));
}

async function sortCollectionByStockDesc(collectionId, pushOOSBottom) {
  const list = await fetchCollectionProducts(collectionId);
  const { inStock, out } = splitByStock(list);
  inStock.sort((a, b) => (b.totalInventory ?? 0) - (a.totalInventory ?? 0));
  const target = pushOOSBottom ? [...inStock, ...out] : inStock.concat(out);
  await reorderToTarget(collectionId, list.map(p => p.id), target.map(p => p.id));
}

async function reorderToTarget(collectionId, oldOrder, newOrder) {
  const pos = new Map();
  oldOrder.forEach((id, i) => pos.set(id, i));

  const moves = [];
  for (let i = 0; i < newOrder.length; i++) {
    const id = newOrder[i];
    const cur = pos.get(id);
    if (cur !== i) {
      moves.push({ id, newPosition: String(i) });
      oldOrder.splice(cur, 1);
      oldOrder.splice(i, 0, id);
      oldOrder.forEach((pid, idx) => pos.set(pid, idx));
    }
  }
  if (moves.length === 0) return;

  const chunk = Number(MOVES_CHUNK);
  for (let i = 0; i < moves.length; i += chunk) {
    const part = moves.slice(i, i + chunk);
    const m = `
      mutation($id:ID!, $moves:[MoveInput!]!){
        collectionReorderProducts(id:$id, moves:$moves){
          job{ id }
          userErrors{ field message }
        }
      }
    `;
    const data = await gql(m, { id: collectionId, moves: part });
    const jobId = data.collectionReorderProducts.job?.id;
    if (jobId) await waitJob(jobId);
  }
}

async function waitJob(jobId) {
  const q = `query($id:ID!){ job(id:$id){ id done } }`;
  for (let i = 0; i < 20; i++) {
    const d = await gql(q, { id: jobId });
    if (d.job?.done) return;
    await sleep(1000);
  }
}

/* ===================== 크론 ===================== */
const randomCronExpr    = RANDOM_CRON    || '*/30 * * * *';
const highStockCronExpr = HIGHSTOCK_CRON || '*/30 * * * *';

try {
  cron.schedule(randomCronExpr, async () => {
    try { console.log('[CRON random] tick', randomCronExpr); await runRandomSorts(); }
    catch (e) { console.error('[CRON random] error:', e); }
  });
} catch (e) { console.error('[CRON] random schedule error:', e); }

try {
  cron.schedule(highStockCronExpr, async () => {
    try { console.log('[CRON highstk] tick', highStockCronExpr); await runHighStockSorts(); }
    catch (e) { console.error('[CRON highstk] error:', e); }
  });
} catch (e) { console.error('[CRON] highstk schedule error:', e); }

/* ===================== 서버 시작 ===================== */
app.listen(PORT, async () => {
  console.log(`Auto Sortify running on :${PORT}`);
  // 필요할 때만 켜세요. (기본 끔)
  if (AUTO_REGISTER_WEBHOOKS === '1') {
    try {
      if (process.env.PUBLIC_BASE_URL) {
        await ensureWebhooks(process.env.PUBLIC_BASE_URL);
      } else {
        console.warn('[ensureWebhooks] PUBLIC_BASE_URL missing. skipped.');
      }
    } catch (e) {
      console.error('[ensureWebhooks] failed:', e?.message || e);
    }
  }
});

/* ===================== 웹훅 자동 등록 (선택) ===================== */
// PUBLIC_BASE_URL 예: https://auto-sortify.onrender.com
async function ensureWebhooks(publicBaseUrl) {
  const listQ = `
    query {
      webhookSubscriptions(first:100){
        edges{
          node{
            id
            topic
            endpoint{ __typename ... on WebhookHttpEndpoint { callbackUrl } }
          }
        }
      }
    }
  `;
  const createM = `
    mutation CreateHook($topic: WebhookSubscriptionTopic!, $url: URL!) {
      webhookSubscriptionCreate(
        topic:$topic,
        webhookSubscription:{ callbackUrl:$url, format:JSON }
      ){
        webhookSubscription{ id }
        userErrors{ field message }
      }
    }
  `;

  const want = ['PRODUCTS_UPDATE', 'INVENTORY_LEVELS_UPDATE'];
  const cur = await gql(listQ);
  const existing = new Map(
    (cur.webhookSubscriptions?.edges || []).map(e => [e.node.topic, e.node])
  );

  for (const t of want) {
    const url = `${publicBaseUrl}/webhooks/${t.toLowerCase().replace(/_/g,'-')}`;
    if (existing.has(t)) {
      const cb = existing.get(t).endpoint?.callbackUrl;
      console.log('[WEBHOOK exists]', t, '->', cb);
      continue;
    }
    console.log('[WEBHOOK create]', t, '->', url);
    const r = await gql(createM, { topic: t, url });
    const errs = r?.webhookSubscriptionCreate?.userErrors || [];
    if (errs.length) {
      console.error('[WEBHOOK error]', t, errs);
    } else {
      console.log('[WEBHOOK created]', t, r.webhookSubscriptionCreate?.webhookSubscription?.id);
    }
  }
}