import express from 'express';
import fetch from 'node-fetch';
import crypto from 'crypto';
import dotenv from 'dotenv';
import cron from 'node-cron';

dotenv.config();
const app = express();
app.use(express.raw({ type: '*/*' })); // 웹훅 HMAC 검증 위해 raw body 유지

const {
  SHOP, ADMIN_TOKEN, API_SECRET,
  DISCOUNT_COLLECTIONS, RANDOM_COLLECTIONS,
  RANDOM_CRON, PORT = 3000
} = process.env;

const API_VERSION = '2024-04'; // 최신 버전 기준
const GQL_ENDPOINT = `https://${SHOP}/admin/api/${API_VERSION}/graphql.json`;

// ---- 공통: Shopify GraphQL 호출
async function gql(query, variables = {}) {
  const res = await fetch(GQL_ENDPOINT, {
    method: 'POST',
    headers: {
      'X-Shopify-Access-Token': ADMIN_TOKEN,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ query, variables })
  });

  // 상태 코드 확인 (200이 아니면 본문을 그대로 출력)
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

// ---- HMAC 검증 (웹훅 보안)
function verifyWebhook(req) {
  const hmac = req.headers['x-shopify-hmac-sha256'];
  const digest = crypto
    .createHmac('sha256', API_SECRET)
    .update(req.body, 'utf8')
    .digest('base64');
  return crypto.timingSafeEqual(Buffer.from(digest), Buffer.from(hmac || ''));
}

// ---- 제품/재고 변경 시 정렬을 트리거하는 웹훅 엔드포인트
app.post('/webhooks/products-update', async (req, res) => {
  if (!verifyWebhook(req)) return res.status(401).send('Invalid HMAC');
  res.status(200).send('ok');
  await runAllAutoSorts();
});

app.post('/webhooks/inventory-levels-update', async (req, res) => {
  if (!verifyWebhook(req)) return res.status(401).send('Invalid HMAC');
  res.status(200).send('ok');
  await runAllAutoSorts();
});

// ---- 수동 테스트용 엔드포인트
app.get('/run-now', async (_req, res) => {
  try {
    await runAllAutoSorts();
    res.send('done');
  } catch (e) {
    console.error('RUN-NOW ERROR:', e);
    res.status(500).send('Error: ' + (e?.message || e));
  }
});

// ---- 루트 안내 페이지 (새로 추가)
app.get('/', (_req, res) => {
  res.send('Auto Sortify is running. Use /run-now to trigger, or wait for webhooks.');
});

// ---- (핵심) 모든 규칙 실행
async function runAllAutoSorts() {
  const discountCollections = (DISCOUNT_COLLECTIONS || '').split(',').map(s => s.trim()).filter(Boolean);
  const randomCollections = (RANDOM_COLLECTIONS || '').split(',').map(s => s.trim()).filter(Boolean);

  for (const colId of discountCollections) {
    await ensureManualSort(colId);
    await sortCollectionByDiscount(colId, true); // 재고 0은 항상 아래
  }
  for (const colId of randomCollections) {
    await ensureManualSort(colId);
    await shuffleCollection(colId, true);
  }
}

// ---- 컬렉션 sortOrder를 MANUAL로 강제 (필수)
async function ensureManualSort(collectionId) {
  const q = `
    query($id: ID!) { collection(id:$id){ id sortOrder } }
  `;
  const d = await gql(q, { id: collectionId });
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
}

// ---- 컬렉션 내 제품 목록/필드 조회 (페이지네이션 처리)
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
    const edges = d.collection.products.edges;
    for (const e of edges) products.push(e.node);
    if (!d.collection.products.pageInfo.hasNextPage) break;
    cursor = edges[edges.length - 1].cursor;
  }
  return products;
}

// ---- 할인율 계산: (compareAt - price)/compareAt 의 최대값
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

// ---- OOS(재고 0) 아래로 내리기
function splitByStock(products) {
  const inStock = [], out = [];
  for (const p of products) ((p.totalInventory ?? 0) > 0 ? inStock : out).push(p);
  return { inStock, out };
}

// ---- 정렬 실행: 할인율 내림차순
async function sortCollectionByDiscount(collectionId, pushOOSBottom) {
  const list = await fetchCollectionProducts(collectionId);
  const { inStock, out } = splitByStock(list);
  inStock.sort((a, b) => maxDiscountOfProduct(b) - maxDiscountOfProduct(a));
  const target = pushOOSBottom ? [...inStock, ...out] : inStock.concat(out);

  await reorderToTarget(collectionId, list.map(p => p.id), target.map(p => p.id));
}

// ---- 정렬 실행: 랜덤 섞기
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

// ---- 현재 순서(oldOrder) -> 목표 순서(newOrder)로 최소 move 생성 후 적용
async function reorderToTarget(collectionId, oldOrder, newOrder) {
  const pos = new Map();
  oldOrder.forEach((id, i) => pos.set(id, i));

  const moves = [];
  for (let i = 0; i < newOrder.length; i++) {
    const id = newOrder[i];
    const cur = pos.get(id);
    if (cur !== i) {
      moves.push({ id, newPosition: i });
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

// ---- 작업(Job) 완료 대기
async function waitJob(jobId) {
  const q = `query($id:ID!){ job(id:$id){ id done } }`;
  for (let i = 0; i < 20; i++) {
    const d = await gql(q, { id: jobId });
    if (d.job?.done) return;
    await new Promise(r => setTimeout(r, 1000));
  }
}

// ---- 웹훅 구독(배포 환경에서 1회 실행)
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

app.listen(PORT, async () => {
  console.log(`Auto Sortify running on :${PORT}`);
  // 배포 후, 한 번만 아래 호출로 웹훅 등록 (public URL로 교체):
  // await ensureWebhooks('https://auto-sortify.onrender.com');
});
