import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.API_URL || 'http://api:8080';
const RUN_ID = __ENV.RUN_ID || `pacing_${Date.now()}`;
const NUM_ORDERS = parseInt(__ENV.NUM_ORDERS || '50', 10);

const orderCreateDuration = new Trend('order_create_duration', true);
const orderSuccessCounter = new Counter('order_create_success_total');
const orderFailedCounter = new Counter('order_create_failed_total');

export const options = {
  scenarios: {
    pacing_batch: {
      executor: 'shared-iterations',
      vus: 5,
      iterations: NUM_ORDERS,
      maxDuration: '1m',
      gracefulStop: '5s',
    },
  },
  thresholds: {
    'order_create_duration': ['p(95)<500', 'p(99)<1000'],
    'http_req_failed{endpoint:CreateOrder}': ['rate<0.01'],
  },
};

export function setup() {
  const loginUrl = `${BASE_URL}/auth.v1.AuthService/Login`;
  const loginPayload = JSON.stringify({
    email: __ENV.MERCHANT_EMAIL || 'loja@elo.com',
    password: __ENV.MERCHANT_PASSWORD || 'Merchant@123!',
  });

  const res = http.post(loginUrl, loginPayload, {
    headers: { 'Content-Type': 'application/json' },
    tags: { endpoint: 'Login' },
  });

  if (res.status !== 200) {
    throw new Error(`Failed to login in setup: status=${res.status} body=${res.body}`);
  }

  const body = JSON.parse(res.body);
  console.log(`[SETUP] Logged in successfully. RUN_ID=${RUN_ID}, NUM_ORDERS=${NUM_ORDERS}`);
  return { 
    token: body.accessToken,
    runId: RUN_ID,
  };
}

export default function (data) {
  const url = `${BASE_URL}/logistics.v1.LogisticsService/CreateOrder`;
  const vuId = exec.vu.idInTest;
  const iter = exec.scenario.iterationInTest;

  const customerName = `pacing-${data.runId}-vu${vuId}-it${iter}`;
  const itemName = `item-${data.runId}-${iter}`;

  const payload = JSON.stringify({
    customer: {
      name: customerName,
      phone: '+5511999999999',
    },
    destination: {
      lat: -23.55052 + (iter * 0.0001),
      lng: -46.633308 + (iter * 0.0001),
      address: `Av Paulista, ${1000 + iter}`,
    },
    packageSize: 1,
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
  try {
    const body = JSON.parse(res.body);
    const pass = check(res, {
      'status is 200': (r) => r.status === 200,
      'order_id returned': () => body.orderId && body.orderId.length > 0,
    });
    success = pass;
  } catch (err) {
    success = false;
  }

  if (success) {
    orderSuccessCounter.add(1);
  } else {
    orderFailedCounter.add(1);
  }

  // Pacing suave de 10ms
  sleep(0.01);
}
