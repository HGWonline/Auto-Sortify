// server.js
import express from 'express';
import fetch from 'node-fetch';
import crypto from 'crypto';
import dotenv from 'dotenv';
import cron from 'node-cron';

dotenv.config();
const app = express();

// 웹훅 HMAC 검증용: raw body 유지 (다른 라우트는 GET만 사용)
app.use(express.raw({ type: '*/*' }));

/* ===================== ENV ===================== */
const {
  SHOP,
  ADMIN_TOKEN,
  API_SECRET,

  // CSV (콤마로 구분, 공백 없이 권장)
  DISCOUNT_COLLECTIONS,       // 할인순 정렬 컬렉션들
  RANDOM_COLLECTIONS,         // 랜덤 정렬 컬렉션들
  HIGHSTOCK_COLLECTIONS,      // 재고 많은 순 정렬 컬렉션들

  // 크론 (미지정 시 기본 30분)
  RANDOM_CRON,
  HIGHSTOCK_CRON,

  PORT = 3000
} = process.env;

const API_VERSION = '2024-07';
const GQL_ENDPOINT = `https://${SHOP}/admin/api/${API_VERSION}/graphql.json`;

/* ===================== 공통: GraphQL 호출 ===================== */
async function gql(query, variables = {}) {
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
  if (json.errors) {
    throw new Error('GraphQL errors: ' + JSON.stringify(json.errors));
  }
  if (json.data?.userErrors?.length) {
    throw new Error('User errors: ' + JSON.stringify(json.data.userErrors));
  }
  return json.data;
}

/* ===================== HMAC (웹훅 보안) ===================== */
function verifyWebhook(req) {
  try {
    const hmac = req.headers['x-shopify-hmac-sha256'];
    const digest = crypto
      .createHmac('sha256', API_SECRET)
      .update(req.body, 'utf8')
      .digest('base64');
    return crypto.timingSafeEqual(Buffer.from(digest), Buffer.from(hmac || ''));
  } catch {
    return false;
  }
}

/* ===================== 유틸 ===================== */
const parseIds = (csv) =>
  (csv || '').split(',').map(s => s.trim()).filter(Boolean);

/* ===================== 웹훅 ===================== */
/** 요구사항 1: 상품 정보(가격/할인가 등) 변경 시 → “할인 정렬 컬렉션만” 재정렬 */
app.post('/webhooks/products-update', async (req, res) => {
  if (!verifyWebhook(req)) return res.status(401).send('Invalid HMAC');
  res.status(200).send('ok');
  runDiscountSorts().catch(e => console.error('[WEBHOOK products] error:', e));
});

/* ===================== 수동 실행 & 루트 ===================== */
app.get('/run-now', async (_req, res) => {
  try {
    res.send('started'); // 즉시 응답
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
  res.send('Auto Sortify is running. Use /run-now to trigger, or wait for product webhooks.');
});

/* ===================== 실행 플로우 (분리) ===================== */
async function runDiscountSorts() {
  const ids = parseIds(DISCOUNT_COLLECTIONS);
  console.log('[RUN] discount:', ids);
  for (const colId of ids) {
    const ok = await ensureManualSort(colId);
    if (!ok) { console.log('[RUN] discount->NOT FOUND', colId); continue; }
    await sortCollectionByDiscount(colId, true); // 재고 0은 항상 맨 뒤
    console.log('[RUN] discount->done', colId);
  }
}

async function runRandomSorts() {
  const ids = parseIds(RANDOM_COLLECTIONS);
  console.log('[RUN] random  :', ids);
  for (const colId of ids) {
    const ok = await ensureManualSort(colId);
    if (!ok) { console.log('[RUN] random->NOT FOUND', colId); continue; }
    await shuffleCollection(colId, true); // 재고 0은 항상 맨 뒤
    console.log('[RUN] random->done', colId);
  }
}

async function runHighStockSorts() {
  const ids = parseIds(HIGHSTOCK_COLLECTIONS);
  console.log('[RUN] highstk :', ids);
  for (const colId of ids) {
    const ok = await ensureManualSort(colId);
    if (!ok) { console.log('[RUN] highstk->NOT FOUND', colId); continue; }
    await sortCollectionByStockDesc(colId, true); // 재고 0은 항상 맨 뒤
    console.log('[RUN] highstk->done', colId);
  }
}

/* ===================== 정렬 로직 ===================== */
// 컬렉션 sortOrder를 MANUAL로 강제 (필수)
async function ensureManualSort(collectionId) {
  const q = `query($id: ID!){ collection(id:$id){ id sortOrder title } }`;
  const d = await gql(q, { id: collectionId });

  if (!d.collection) {
    console.error('[ensureManualSort] Collection NOT FOUND:', collectionId);
    return false;
  }
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

// 컬렉션 내 상품 조회 (페이지네이션)
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

// 할인율 계산: (compare_at - price)/compare_at 의 최대값
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

// 재고 분리: 재고 0은 out 배열로
function splitByStock(products) {
  const inStock = [], out = [];
  for (const p of products) ((p.totalInventory ?? 0) > 0 ? inStock : out).push(p);
  return { inStock, out };
}

// 정렬 1: 할인율 내림차순 (재고 0 맨뒤)
async function sortCollectionByDiscount(collectionId, pushOOSBottom) {
  const list = await fetchCollectionProducts(collectionId);
  const { inStock, out } = splitByStock(list);
  inStock.sort((a, b) => maxDiscountOfProduct(b) - maxDiscountOfProduct(a));
  const target = pushOOSBottom ? [...inStock, ...out] : inStock.concat(out);
  await reorderToTarget(collectionId, list.map(p => p.id), target.map(p => p.id));
}

// 정렬 2: 랜덤 섞기 (재고 0 맨뒤)
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

// 정렬 3: 총 재고량 내림차순 (재고 0 맨뒤)
async function sortCollectionByStockDesc(collectionId, pushOOSBottom) {
  const list = await fetchCollectionProducts(collectionId);
  const { inStock, out } = splitByStock(list);
  inStock.sort((a, b) => (b.totalInventory ?? 0) - (a.totalInventory ?? 0));
  const target = pushOOSBottom ? [...inStock, ...out] : inStock.concat(out);
  await reorderToTarget(collectionId, list.map(p => p.id), target.map(p => p.id));
}

// 현재 순서 → 목표 순서로 이동 (MoveInput.newPosition 은 문자열!)
async function reorderToTarget(collectionId, oldOrder, newOrder) {
  const pos = new Map();
  oldOrder.forEach((id, i) => pos.set(id, i));

  const moves = [];
  for (let i = 0; i < newOrder.length; i++) {
    const id = newOrder[i];
    const cur = pos.get(id);
    if (cur !== i) {
      moves.push({ id, newPosition: String(i) }); // 중요: 문자열
      // simulate
      oldOrder.splice(cur, 1);
      oldOrder.splice(i, 0, id);
      oldOrder.forEach((pid, idx) => pos.set(pid, idx));
    }
  }
  if (moves.length === 0) return;

  const chunk = 250;
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
    await new Promise(r => setTimeout(r, 1000));
  }
}

/* ===================== 크론 (요구사항 2,3) ===================== */
// 기본 30분(*/30 * * * *) — 환경변수로 쉽게 변경 가능
const randomCronExpr   = RANDOM_CRON   || '*/30 * * * *';
const highStockCronExpr = HIGHSTOCK_CRON || '*/30 * * * *';

try {
  cron.schedule(randomCronExpr, async () => {
    try {
      console.log('[CRON random] tick', randomCronExpr);
      await runRandomSorts();
    } catch (e) {
      console.error('[CRON random] error:', e);
    }
  });
} catch (e) {
  console.error('[CRON] random schedule error:', e);
}

try {
  cron.schedule(highStockCronExpr, async () => {
    try {
      console.log('[CRON highstk] tick', highStockCronExpr);
      await runHighStockSorts();
    } catch (e) {
      console.error('[CRON highstk] error:', e);
    }
  });
} catch (e) {
  console.error('[CRON] highstk schedule error:', e);
}

/* ===================== 서버 시작 ===================== */
app.listen(PORT, async () => {
  console.log(`Auto Sortify running on :${PORT}`);
  // 필요 시 한 번만 실행해 웹훅 수동 등록:
  // await ensureWebhooks('https://auto-sortify.onrender.com');
});

/* ========= (선택) 필요시 사용할 수 있는 웹훅 자동 등록 함수 ========= */
// 사용하지 않으면 삭제/주석 처리해도 됩니다.
async function ensureWebhooks(publicBaseUrl) {
  const m = `
    mutation($topic:WebhookSubscriptionTopic!, $url:String!){
      webhookSubscriptionCreate(
        topic:$topic,
        webhookSubscription:{ callbackUrl:$url, format:JSON }
      ){
        webhookSubscription{ id }
        userErrors{ field message }
      }
    }
  `;
  const topics = ['PRODUCTS_UPDATE']; // 재고 웹훅은 현재 사용하지 않음
  for (const t of topics) {
    await gql(m, { topic: t, url: `${publicBaseUrl}/webhooks/${t.toLowerCase().replace(/_/g,'-')}` });
  }
}
