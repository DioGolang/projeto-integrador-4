import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.API_URL || 'http://api:8080';
const RUN_ID = __ENV.RUN_ID || `run_${Date.now()}`;

// Métricas isoladas para criação de pedidos (sem contaminação por login/bcrypt)
const orderCreateDuration = new Trend('order_create_duration', true);
const orderSuccessCounter = new Counter('order_create_success_total');
const orderFailedCounter = new Counter('order_create_failed_total');

export const options = {
  scenarios: {
    ramp_5_vus: {
      executor: 'shared-iterations',
      vus: 5,
      iterations: 200,
      startTime: '0s',
      maxDuration: '30s',
      gracefulStop: '5s',
    },
    ramp_10_vus: {
      executor: 'shared-iterations',
      vus: 10,
      iterations: 300,
      startTime: '5s',
      maxDuration: '30s',
      gracefulStop: '5s',
    },
    ramp_25_vus: {
      executor: 'shared-iterations',
      vus: 25,
      iterations: 500,
      startTime: '12s',
      maxDuration: '30s',
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

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
    tags: { endpoint: 'Login' },
  };

  const loginStart = Date.now();
  const res = http.post(loginUrl, loginPayload, params);
  const loginDuration = Date.now() - loginStart;

  const ok = check(res, {
    'login status is 200': (r) => r.status === 200,
    'login token returned': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.accessToken && body.accessToken.length > 0;
      } catch (e) {
        return false;
      }
    },
  });

  if (!ok) {
    throw new Error(`Failed to login in setup: status=${res.status} body=${res.body}`);
  }

  const body = JSON.parse(res.body);
  console.log(`[SETUP] Logged in successfully. Token length: ${body.accessToken.length}. Login duration: ${loginDuration}ms. RUN_ID=${RUN_ID}`);
  return { 
    token: body.accessToken,
    runId: RUN_ID,
  };
}

export default function (data) {
  const url = `${BASE_URL}/logistics.v1.LogisticsService/CreateOrder`;
  const scenarioName = exec.scenario.name;
  const vuId = exec.vu.idInTest;
  const iter = exec.scenario.iterationInTest;

  const customerName = `gate12-${data.runId}-${scenarioName}-${vuId}-${iter}`;
  const itemName = `item-${data.runId}-${vuId}-${iter}`;

  const payload = JSON.stringify({
    customer: {
      name: customerName,
      phone: '+5511999999999',
    },
    destination: {
      lat: -23.55052,
      lng: -46.633308,
      address: 'Av Paulista, 1000',
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
      'create order status is 200': (r) => r.status === 200,
      'order_id returned': () => body.orderId && body.orderId.length > 0,
      'delivery_pin returned': () => body.deliveryPin && body.deliveryPin.length > 0,
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

  // Pacing suave de 5ms para manter a concorrência estável sem saturar sockets em microburst
  sleep(0.005);
}
