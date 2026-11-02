import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { Trend, Counter, Rate } from 'k6/metrics';

const BASE_URL = __ENV.API_URL || 'http://api:8080';
const RUN_ID = __ENV.RUN_ID || `gate14a_${Date.now()}`;
const NUM_ORDERS = parseInt(__ENV.NUM_ORDERS || '10000', 10);
const TARGET_VUS = parseInt(__ENV.VUS || '25', 10);

// Métricas de isolamento da Etapa 1.4A
const orderCreateDuration = new Trend('gate14a_order_create_duration', true);
const orderSuccessCounter = new Counter('gate14a_order_create_success_total');
const orderFailedCounter = new Counter('gate14a_order_create_failed_total');
const errorRate = new Rate('gate14a_error_rate');

export const options = {
  scenarios: {
    gate14a_10k_batch: {
      executor: 'shared-iterations',
      vus: TARGET_VUS,
      iterations: NUM_ORDERS,
      maxDuration: '10m',
      gracefulStop: '15s',
    },
  },
  thresholds: {
    // Critérios rígidos de qualidade HTTP (sucesso mínimo de 99.9% com interrupção automática)
    'gate14a_order_create_duration': ['p(95)<300', 'p(99)<800'],
    'gate14a_error_rate': [{ threshold: 'rate<0.001', abortOnFail: true, delayAbortEval: '5s' }],
    'http_req_failed{endpoint:CreateOrder}': [{ threshold: 'rate<0.001', abortOnFail: true, delayAbortEval: '5s' }],
  },
};

export function setup() {
  console.log(`[GATE 1.4A SETUP] Iniciando preparação do teste de 10.000 criações...`);
  console.log(`[GATE 1.4A SETUP] RUN_ID: ${RUN_ID} | Pedidos: ${NUM_ORDERS} | VUs: ${TARGET_VUS}`);

  // Autenticação prévia do lojista
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
    throw new Error(`Falha no login do lojista durante setup: status=${res.status} body=${res.body}`);
  }

  const body = JSON.parse(res.body);
  console.log(`[GATE 1.4A SETUP] Lojista autenticado com sucesso. Token gerado.`);

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

  // Identificador unívoco do pedido com prefixo explícito da rodada
  const customerName = `gate14a-${data.runId}-vu${vuId}-it${iter}`;
  const itemName = `item-gate14a-${iter}`;

  // Coordenadas ligeiramente variadas em torno da Av. Paulista
  const latOffset = (iter % 1000) * 0.00005;
  const lngOffset = (iter % 1000) * 0.00005;

  const payload = JSON.stringify({
    customer: {
      name: customerName,
      phone: '+5511999999999',
    },
    destination: {
      lat: -23.55052 + latOffset,
      lng: -46.633308 + lngOffset,
      address: `Av Paulista, ${1000 + (iter % 500)}`,
    },
    packageSize: 1, // SMALL
    items: [
      {
        name: itemName,
        quantity: 1,
        unitPrice: 35.0,
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
    'order created successfully with valid id': () => success,
  });

  if (checkPass) {
    orderSuccessCounter.add(1);
    errorRate.add(0);
  } else {
    orderFailedCounter.add(1);
    errorRate.add(1);
    if (orderFailedCounter.value <= 10) {
      console.error(`[FALHA HTTP #${orderFailedCounter.value}] Order ${customerName} falhou: status=${res.status} body=${res.body}`);
    }
  }

  // Pacing controlado de 5ms por VU para evitar micro-bursts de socket
  sleep(0.005);
}

export function teardown(data) {
  console.log(`[GATE 1.4A TEARDOWN] Envio concluído. RUN_ID=${data.runId}. Aguardando drenagem do Outbox...`);
}
