import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { Trend, Counter, Rate } from 'k6/metrics';

const BASE_URL = __ENV.API_URL || 'http://api:8080';
const RUN_ID = __ENV.RUN_ID || `gate14c1000_${Date.now()}`;
const NUM_ORDERS = parseInt(__ENV.NUM_ORDERS || '1000', 10);
const TARGET_VUS = parseInt(__ENV.VUS || '15', 10);

// Métricas do Gate 1.4C (1.000 Pedidos)
const orderCreateDuration = new Trend('gate14c1000_order_create_duration', true);
const orderSuccessCounter = new Counter('gate14c1000_order_create_success_total');
const orderFailedCounter = new Counter('gate14c1000_order_create_failed_total');
const errorRate = new Rate('gate14c1000_error_rate');

export const options = {
  scenarios: {
    gate14c_1000_batch: {
      executor: 'shared-iterations',
      vus: TARGET_VUS,
      iterations: NUM_ORDERS,
      maxDuration: '10m',
      gracefulStop: '15s',
    },
  },
  thresholds: {
    'gate14c1000_order_create_duration': ['p(95)<300', 'p(99)<800'],
    'gate14c1000_error_rate': [{ threshold: 'rate<0.005', abortOnFail: true, delayAbortEval: '5s' }],
    'http_req_failed{endpoint:CreateOrder}': [{ threshold: 'rate<0.005', abortOnFail: true, delayAbortEval: '5s' }],
  },
};

export function setup() {
  console.log(`[GATE 1.4C LOTE 1000 SETUP] Preparando envio de ${NUM_ORDERS} pedidos (VUs: ${TARGET_VUS})...`);
  console.log(`[GATE 1.4C LOTE 1000 SETUP] RUN_ID: ${RUN_ID}`);

  // Autenticação do lojista
  const loginUrl = `${BASE_URL}/auth.v1.AuthService/Login`;
  const loginPayload = JSON.stringify({
    email: __ENV.MERCHANT_EMAIL || 'loja@elo.com',
    password: __ENV.MERCHANT_PASSWORD || 'Merchant@123!',
  });

  const res = http.post(loginUrl, loginPayload, {
    headers: { 'Content-Type': 'application/json' },
    tags: { endpoint: 'MerchantLogin_Setup' },
  });

  if (res.status !== 200) {
    throw new Error(`Falha no login do lojista: status=${res.status} body=${res.body}`);
  }

  const body = JSON.parse(res.body);
  console.log(`[GATE 1.4C LOTE 1000 SETUP] Lojista autenticado com sucesso.`);

  return {
    token: body.accessToken,
    runId: RUN_ID,
    numOrders: NUM_ORDERS,
  };
}

export default function (data) {
  const url = `${BASE_URL}/logistics.v1.LogisticsService/CreateOrder`;
  const vuId = exec.vu.idInTest;
  const iter = exec.scenario.iterationInTest;

  const customerName = `gate14c1000-${data.runId}-vu${vuId}-it${iter}`;
  const itemName = `item-gate14c1000-${iter}`;

  // Variação de coordenadas em torno de São Paulo (Av Paulista)
  const latOffset = (iter % 1000) * 0.0001;
  const lngOffset = (iter % 1000) * 0.0001;

  const payload = JSON.stringify({
    customer: {
      name: customerName,
      phone: '+5511999999999',
    },
    destination: {
      lat: -23.55052 + latOffset,
      lng: -46.633308 + lngOffset,
      address: `Av Paulista, ${1000 + (iter % 800)}`,
    },
    packageSize: 1, // SMALL
    items: [
      {
        name: itemName,
        quantity: 1,
        unitPrice: 25.0,
      },
    ],
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${data.token}`,
    },
    tags: { endpoint: 'CreateOrder' },
  };

  const res = http.post(url, payload, params);
  orderCreateDuration.add(res.timings.duration);

  let success = false;
  if (res.status === 200) {
    try {
      const resBody = JSON.parse(res.body);
      if (resBody.orderId && resBody.orderId.length > 0 && resBody.deliveryPin) {
        success = true;
      }
    } catch (_) {
      success = false;
    }
  }

  const checkPass = check(res, {
    'http status is 200': (r) => r.status === 200,
    'order created successfully with valid id and pin': () => success,
  });

  if (checkPass) {
    orderSuccessCounter.add(1);
    errorRate.add(0);
  } else {
    orderFailedCounter.add(1);
    errorRate.add(1);
    console.error(`[FALHA HTTP] Order ${customerName} falhou: status=${res.status} body=${res.body}`);
  }

  // Pacing controlado de 10ms entre iterações da mesma VU
  sleep(0.010);
}

export function teardown(data) {
  console.log(`[GATE 1.4C LOTE 1000 TEARDOWN] Envio dos ${data.numOrders} pedidos concluído. RUN_ID=${data.runId}.`);
}
