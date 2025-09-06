// server.js
import express from 'express';
import fetch from 'node-fetch';
import crypto from 'crypto';
import dotenv from 'dotenv';
import cron from 'node-cron';

dotenv.config();
const app = express();

/* ===================== Body parsers ===================== */
/** 웹훅은 HMAC 검증을 위해 "raw body"가 필요합니다. */
app.use('/webhooks', express.raw({ type: '*/*' }));
/** 그 외 라우트는 일반 JSON 파서 사용(필요 시) */
app.use(express.json());

/* ===================== ENV ===================== */
const {
  SHOP,
  ADMIN_TOKEN,
  API_SECRET,

  // CSV (콤마 구분, 공백 없이 권장)
  DISCOUNT_COLLECTIONS,       // 할인순 정렬 컬렉션들
  RANDOM_COLLECTIONS,         // 랜덤 정렬 컬렉션들
  HIGHSTOCK_COLLECTIONS,      // 재고 많은 순 정렬 컬렉션들

  // 크론 (미지정 시 기본 30분)
  RANDOM_CRON,
  HIGHSTOCK_CRON,

  // 수동 트리거 보안 토큰
  TRIGGER_TOKEN,

  // (옵션) 레이트리밋 튜닝
  MOVES_CHUNK = 100,          // collectionReorderProducts 청크 크기(기본 100)
  GQL_MAX_RETRIES = 8,
  GQL_BASE_DELAY_MS = 800,

  PORT = 3000
} = process.env;

const API_VERSION = '2024-07';
const GQL_ENDPOINT = `https://${SHOP}/admin/api/${API_VERSION}/graphql.json`;

/* ===================== 공통: GraphQL 호출(리트라이/백오프) ===================== */
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

      // GraphQL top-level errors
      if (json.errors && json.errors.length) {
        const throttled = json.errors.some(
          (e) =>
            e?.extensions?.code === 'THROTTLED' ||
            /throttled/i.test(e?.message ?? '')
        );
        if (throttled) {
          // 지수 백오프
          await sleep(Number(GQL_BASE_DELAY_MS) * Math.pow(2, attempt));
          attempt++;
          continue;
        }
        throw new Error('GraphQL errors: ' + JSON.stringify(json.errors));
      }

      // data.userErrors
      if (json.data?.userErrors?.length) {
        throw new Error('User errors: ' + JSON.stringify(json.data.userErrors));
      }

      // (소프트 대기) 버킷이 너무 낮으면 1초 휴식
      const throttle = json?.extensions?.cost?.throttleStatus;
      if (throttle && typeof throttle.currentlyAvailable === 'number') {
        if (throttle.currentlyAvailable < 10) {
          await sleep(1000);
        }
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

// 기존 함수명과 호환
const gql = gqlWithRetry;

/* ===================== HMAC (웹훅 보안) ===================== */
function verifyWebhook(req) {
  try {
    const hmac = req.headers['x-shopify-hmac-sha256'];
    const digest = crypto
      .createHmac('sha256', API_SECRET)
      .update(req.body, 'utf8')     // webhooks 라우트에서 raw buffer를 받음
      .digest('base64');
    return crypto.timingSafeEqual(Buffer.from(digest), Buffer.from(hmac || ''));
  } catch {
    return false;
  }
}

/* ===================== 유틸 ===================== */
const parseIds = (csv) =>
  (csv || '').split(',').map(s => s.trim()).filter(Boolean);

/* ===================== 동시 실행 가드 ===================== */
const running = { discount: false, random: false, stock: false };
async function guardRun(kind, fn) {
  if (running[kind]) {
    console.log(`[GUARD] ${kind} is already running. Skipped.`);
    return;
  }
  running[kind] = true;
  try { await fn(); }
  finally { running[kind] = false; }
}

/* ===================== 웹훅 ===================== */
// 재고 변경 → 재고정렬만
app.post('/webhooks/inventory-levels-update', async (req, res) => {
  if (!verifyWebhook(req)) return res.status(401).send('Invalid HMAC');
  res.status(200).send('ok');
  runHighStockSorts().catch(e => console.error('[WEBHOOK inventory] error:', e));
});

// 상품/가격 변경 → 할인정렬만
app.post('/webhooks/products-update', async (req, res) => {
  if (!verifyWebhook(req)) return res.status(401).send('Invalid HMAC');
  res.status(200).send('ok');
  runDiscountSorts().catch(e => console.error('[WEBHOOK products] error:', e));
});

/* ===================== 수동 실행(랜덤 전용/전체) & 루트 ===================== */
// 보안 토큰으로 보호된 "랜덤 정렬만" 트리거
app.post('/run-random', async (req, res) => {
  const token = req.headers['x-trigger-token'] || req.query.key;
  if (TRIGGER_TOKEN && token !== TRIGGER_TOKEN) {
    return res.status(401).send('unauthorized');
  }
  res.send('random started');
  runRandomSorts().catch(e => console.error('[RUN-RANDOM] error:', e));
});

// 전체 실행 (필요 시만 사용)
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
      await sortCollectionByDiscount(colId, true); // 재고 0은 항상 맨 뒤
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
      await shuffleCollection(colId, true); // 재고 0은 항상 맨 뒤
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
      await sortCollectionByStockDesc(colId, true); // 재고 0은 항상 맨 뒤
      console.log('[RUN] highstk->done', colId);
    }
  });
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

// 현재 순서 → 목표 순서로 이동
async function reorderToTarget(collectionId, oldOrder, newOrder) {
  const pos = new Map();
  oldOrder.forEach((id, i) => pos.set(id, i));

  const moves = [];
  for (let i = 0; i < newOrder.length; i++) {
    const id = newOrder[i];
    const cur = pos.get(id);
    if (cur !== i) {
      moves.push({ id, newPosition: String(i) }); // 문자열!
      // simulate
      oldOrder.splice(cur, 1);
      oldOrder.splice(i, 0, id);
      oldOrder.forEach((pid, idx) => pos.set(pid, idx));
    }
  }
  if (moves.length === 0) return;

  const chunk = Number(MOVES_CHUNK); // 기본 100
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
// Render 무료티어는 슬립 시 tick이 멈출 수 있으니, 외부 크론(Cloudflare/GitHub Actions) 권장.
// 일단 앱 내부에도 설정은 유지합니다.
const randomCronExpr    = RANDOM_CRON    || '*/30 * * * *';
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
  // 필요할 때만 잠깐 켜서 실행하세요. 성공 후엔 다시 주석 처리!
  try {
    if (process.env.PUBLIC_BASE_URL) {
      await ensureWebhooks(process.env.PUBLIC_BASE_URL);
    } else {
      console.warn('[ensureWebhooks] PUBLIC_BASE_URL is missing. skipped.');
    }
  } catch (e) {
    console.error('[ensureWebhooks] failed:', e?.message || e);
  }
});

/* ========= (선택) 웹훅 자동 등록 ========= */
// PUBLIC_BASE_URL 예: https://auto-sortify.onrender.com
// ensureWebhooks: URL 타입으로 수정 + 중복확인 + 에러로그
async function ensureWebhooks(publicBaseUrl) {
  // 0) 현재 구독 목록
  const q = `
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
  const existing = await gql(q);
  const current = new Map(
    (existing.webhookSubscriptions?.edges || []).map(e => [e.node.topic, e.node])
  );

  // 1) 필요한 토픽
  const topics = ['PRODUCTS_UPDATE', 'INVENTORY_LEVELS_UPDATE'];

  // 2) 생성 뮤테이션 (★ url 타입을 URL! 로 변경)
  const m = `
    mutation CreateHook($topic: WebhookSubscriptionTopic!, $url: URL!) {
      webhookSubscriptionCreate(
        topic: $topic,
        webhookSubscription: { callbackUrl: $url, format: JSON }
      ) {
        webhookSubscription { id }
        userErrors { field message }
      }
    }
  `;

  for (const t of topics) {
    const url = `${publicBaseUrl}/webhooks/${t.toLowerCase().replace(/_/g,'-')}`;
    if (current.has(t)) {
      const cb = current.get(t).endpoint?.callbackUrl;
      console.log('[WEBHOOK exists]', t, '->', cb);
      continue;
    }
    console.log('[WEBHOOK create]', t, '->', url);
    const d = await gql(m, { topic: t, url });
    const errs = d?.webhookSubscriptionCreate?.userErrors || [];
    if (errs.length) {
      console.error('[WEBHOOK error]', t, errs);
    } else {
      console.log('[WEBHOOK created]', t, d?.webhookSubscriptionCreate?.webhookSubscription?.id);
    }
  }
}

