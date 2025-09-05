import express from 'express';
import fetch from 'node-fetch';
import crypto from 'crypto';
import dotenv from 'dotenv';
import cron from 'node-cron';

dotenv.config();
const app = express();

// 웹훅 HMAC 검증 위해 raw body 유지 (다른 라우트에는 영향 없음)
app.use(express.raw({ type: '*/*' }));

const {
  SHOP, ADMIN_TOKEN, API_SECRET,
  DISCOUNT_COLLECTIONS, RANDOM_COLLECTIONS,
  RANDOM_CRON, PORT = 3000
} = process.env;

// 안정 버전 권장
const API_VERSION = '2024-07';
const GQL_ENDPOINT = `https://${SHOP}/admin/api/${API_VERSION}/graphql.json`;

/* ------------------------------------------------------------------ */
/* Common: Admin GraphQL caller (상세 에러 메시지 포함)                 */
/* ------------------------------------------------------------------ */
async function gql(query, variables = {}) {
  const res = await fetch(GQL_ENDPOINT, {
    method: 'POST',
    headers: {
      'X-Shopify-Access-Token': ADMIN_TOKEN,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ query, variables })
  });

  // 200이 아니면 원문 출력
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

/* ------------------------------------------------------------------ */
/* Webhook HMAC 검증                                                   */
/* ------------------------------------------------------------------ */
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

/* ------------------------------------------------------------------ */
/* Webhook endpoints: 제품/재고 변경 시 자동 트리거                    */
/* ------------------------------------------------------------------ */
app.post('/webhooks/products-update', async (req, res) => {
  if (!verifyWebhook(req)) return res.status(401).send('Invalid HMAC');
  res.status(200).send('ok');
  runAllAutoSorts().catch(e => console.error('[WEBHOOK products] error:', e));
});

app.post('/webhooks/inventory-levels-update', async (req, res) => {
  if (!verifyWebhook(req)) return res.status(401).send('Invalid HMAC');
  res.status(200).send('ok');
  runAllAutoSorts().catch(e => console.error('[WEBHOOK inventory] error:', e));
});

/* ------------------------------------------------------------------ */
/* 수동 실행: 즉시 응답, 백그라운드 처리 + 단계별 로그                 */
/* ------------------------------------------------------------------ */
app.get('/run-now', async (_req, res) => {
  try {
    console.log('[RUNNOW] received');
    res.send('started'); // 즉시 응답
    runAllAutoSorts()
      .then(() => console.log('[RUNNOW] finished'))
      .catch(e => console.error('[RUNNOW] error:', e));
  } catch (e) {
    console.error('[RUNNOW] fatal:', e);
    res.status(500).send('Error: ' + (e?.message || e));
  }
});

/* ------------------------------------------------------------------ */
/* 루트 안내 페이지                                                    */
/* ------------------------------------------------------------------ */
app.get('/', (_req, res) => {
  res.send('Auto Sortify is running. Use /run-now to trigger, or wait for webhooks.');
});

/* ------------------------------------------------------------------ */
/* 핵심 실행 플로우                                                    */
/* ------------------------------------------------------------------ */
async function runAllAutoSorts() {
  const discountCollections = (DISCOUNT_COLLECTIONS || '').split(',').map(s => s.trim()).filter(Boolean);
  const randomCollections   = (RANDOM_COLLECTIONS   || '').split(',').map(s => s.trim()).filter(Boolean);

  console.log('[RUN] discount:', discountCollections);
  console.log('[RUN] random  :', randomCollections);

  for (const colId of discountCollections) {
    console.log('[RUN] discount->ensureManualSort', colId);
    const ok = await ensureManualSort(colId);
    if (!ok) { console.log('[RUN] discount->NOT FOUND', colId); continue; }

    console.log('[RUN] discount->sortCollectionByDiscount start', colId);
    await sortCollectionByDiscount(colId, true);
    console.log('[RUN] discount->done', colId);
  }

  for (const colId of randomCollections) {
    console.log('[RUN] random->ensureManualSort', colId);
    const ok = await ensureManualSort(colId);
    if (!ok) { console.log('[RUN] random->NOT FOUND', colId); continue; }

    console.log('[RUN] random->shuffleCollection start', colId);
    await shuffleCollection(colId, true);
    console.log('[RUN] random->done', colId);
  }
}

/* ------------------------------------------------------------------ */
/* 컬렉션 sortOrder를 MANUAL로 보장 + 미존재시 skip                     */
/* ------------------------------------------------------------------ */
async function ensureManualSort(collectionId) {
  const q = `
    query($id: ID!) { collection(id:$id){ id sortOrder title } }
  `;
  const d = await gql(q, { id: collectionId });

  if (!d.collection) {
    console.error('[ensureManualSort] Collection NOT FOUND:', collectionId);
    return false;
  }

  if (d.collection.sortOrder !== 'MANUAL') {
    const m = `
      mutation($id: ID!){
        collectionUpdate(input:{id:$id, sortOrder:MANUAL}) {
          collection { id sortOrder }
          userErrors { field message }
        }
      }
    `;
    await gql(m, { id: collectionId });
  }
  return true;
}

/* ------------------------------------------------------------------ */
/* 컬렉션 내 상품 로드 (페이지네이션)                                   */
/* ------------------------------------------------------------------ */
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
    if (!d.collection) break; // 방어
    const edges = d.collection.products.edges;
    for (const e of edges) products.push(e.node);
    if (!d.collection.products.pageInfo.hasNextPage) break;
    cursor = edges[edges.length - 1].cursor;
  }
  return products;
}

/* ------------------------------------------------------------------ */
/* 할인율 계산 & 재고 분리                                             */
/* ------------------------------------------------------------------ */
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

/* ------------------------------------------------------------------ */
/* 정렬 실행: 할인율 내림차순                                          */
/* ------------------------------------------------------------------ */
async function sortCollectionByDiscount(collectionId, pushOOSBottom) {
  const list = await fetchCollectionProducts(collectionId);
  const { inStock, out } = splitByStock(list);
  inStock.sort((a, b) => maxDiscountOfProduct(b) - maxDiscountOfProduct(a));
  const target = pushOOSBottom ? [...inStock, ...out] : inStock.concat(out);

  await reorderToTarget(collectionId, list.map(p => p.id), target.map(p => p.id));
}

/* ------------------------------------------------------------------ */
/* 정렬 실행: 랜덤 셔플                                                */
/* ------------------------------------------------------------------ */
async function shuffleCollection(collectionId, pushOOSBottom) {
  const list = await fetchCollectionProducts(collectionId);
  const { inStock, out } = splitByStock(list);

  // Fisher–Yates
  for (let i = inStock.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [inStock[i], inStock[j]] = [inStock[j], inStock[i]];
  }
  const target = pushOOSBottom ? [...inStock, ...out] : inStock.concat(out);
  await reorderToTarget(collectionId, list.map(p => p.id), target.map(p => p.id));
}

/* ------------------------------------------------------------------ */
/* Reorder: 최소 move 생성 후 배치 적용                                */
/* ------------------------------------------------------------------ */
async function reorderToTarget(collectionId, oldOrder, newOrder) {
  const pos = new Map();
  oldOrder.forEach((id, i) => pos.set(id, i));

  const moves = [];
  for (let i = 0; i < newOrder.length; i++) {
    const id = newOrder[i];
    const cur = pos.get(id);
    if (cur !== i) {
      moves.push({ id, newPosition: String(i) });
      // simulate
      oldOrder.splice(cur, 1);
      oldOrder.splice(i, 0, id);
      oldOrder.forEach((pid, idx) => pos.set(pid, idx));
    }
  }
  if (moves.length === 0) return;

  // 한 번에 최대 250 moves
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

/* ------------------------------------------------------------------ */
/* Job 완료 대기                                                       */
/* ------------------------------------------------------------------ */
async function waitJob(jobId) {
  const q = `query($id:ID!){ job(id:$id){ id done } }`;
  for (let i = 0; i < 20; i++) {
    const d = await gql(q, { id: jobId });
    if (d.job?.done) return;
    await new Promise(r => setTimeout(r, 1000));
  }
}

/* ------------------------------------------------------------------ */
/* 웹훅 구독(원하면 배포 후 1회만 실행)                                */
/* ------------------------------------------------------------------ */
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
  const topics = ['PRODUCTS_UPDATE','INVENTORY_LEVELS_UPDATE'];
  for (const t of topics) {
    await gql(m, { topic: t, url: `${publicBaseUrl}/webhooks/${t.toLowerCase().replace(/_/g,'-')}` });
  }
}

/* ------------------------------------------------------------------ */
/* (선택) 랜덤 셔플 크론                                               */
/* ------------------------------------------------------------------ */
let cronTask = null;
if (RANDOM_CRON) {
  try {
    cronTask = cron.schedule(RANDOM_CRON, async () => {
      const randomCollections = (RANDOM_COLLECTIONS || '').split(',').map(s => s.trim()).filter(Boolean);
      console.log('[CRON] random tick', RANDOM_CRON, randomCollections);
      for (const colId of randomCollections) {
        const ok = await ensureManualSort(colId);
        if (!ok) { console.log('[CRON] random->NOT FOUND', colId); continue; }
        await shuffleCollection(colId, true);
      }
      console.log('[CRON] random finished');
    });
  } catch (e) {
    console.error('[CRON] schedule error:', e);
  }
}

/* ------------------------------------------------------------------ */
/* (디버그) 진단용 라우트 3종 - 문제 해결 후 삭제 권장                  */
/* ------------------------------------------------------------------ */
app.get('/debug-shop', async (_req, res) => {
  try {
    const base = `https://${process.env.SHOP}/admin/api/${API_VERSION}`;
    const headers = { 'X-Shopify-Access-Token': process.env.ADMIN_TOKEN };

    const restRes  = await fetch(`${base}/shop.json`, { headers });
    const restText = await restRes.text();

    const gqlRes = await fetch(`${base}/graphql.json`, {
      method: 'POST',
      headers: { ...headers, 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: `{ shop { name myshopifyDomain } }` })
    });
    const gqlText = await gqlRes.text();

    res.type('text/plain').send(
      [
        `REST ${restRes.status} /shop.json`,
        restText,
        '',
        `GQL  ${gqlRes.status} /graphql.json`,
        gqlText
      ].join('\n')
    );
  } catch (e) {
    res.status(500).send('debug error: ' + (e?.message || e));
  }
});

app.get('/debug-collections', async (_req, res) => {
  try {
    let cursor = null, out = [];
    const q = `
      query($cursor:String){
        collections(first:100, after:$cursor){
          pageInfo{ hasNextPage }
          edges{ cursor node{ id title handle } }
        }
      }
    `;
    do {
      const d = await gql(q, { cursor });
      const edges = d.collections.edges;
      out = out.concat(edges.map(e => e.node));
      cursor = d.collections.pageInfo.hasNextPage ? edges.at(-1).cursor : null;
    } while (cursor);

    res.type('text/plain').send(out.map(c => `${c.title} | ${c.id}`).join('\n'));
  } catch (e) {
    res.status(500).send('debug error: ' + (e?.message || e));
  }
});

app.get('/debug-collection-count', async (req, res) => {
  try {
    const id = req.query.id; // gid
    const q = `
      query($id:ID!){
        collection(id:$id){
          id title
          products(first:1){ pageInfo{ hasNextPage } edges{ node{ id } } }
        }
      }
    `;
    const d = await gql(q, { id });
    if (!d.collection) return res.status(404).send('collection not found');
    res.type('text/plain').send(`OK: ${d.collection.title} (${d.collection.id})`);
  } catch (e) {
    res.status(500).send('debug error: ' + (e?.message || e));
  }
});

/* ------------------------------------------------------------------ */
/* Start                                                               */
/* ------------------------------------------------------------------ */
app.listen(PORT, async () => {
  console.log(`Auto Sortify running on :${PORT}`);
  // 필요 시 1회만 실행해서 웹훅 등록:
  // await ensureWebhooks('https://auto-sortify.onrender.com');
});