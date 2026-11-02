import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.API_URL || 'http://api:8080';
const NUM_ORDERS = parseInt(__ENV.NUM_ORDERS || '25', 10);
const RUN_ID = __ENV.RUN_ID || `race_${Date.now()}`;

// Custom metrics
const raceRoundDuration = new Trend('race_round_duration', true);
const raceWinnerCounter = new Counter('race_winner_count');
const raceLoserCounter = new Counter('race_loser_count');
const raceUnexpectedCounter = new Counter('race_unexpected_error_count');
const idempotentRetrySuccessCounter = new Counter('idempotent_retry_success_count');
const loserRetryRejectionCounter = new Counter('loser_retry_rejection_count');

export const options = {
  scenarios: {
    aggregate_race: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: NUM_ORDERS,
      maxDuration: '2m',
    },
  },
  thresholds: {
    'race_winner_count': [`count==${NUM_ORDERS}`],
    'race_loser_count': [`count==${NUM_ORDERS * 9}`],
    'race_unexpected_error_count': ['count==0'],
    'idempotent_retry_success_count': [`count==${NUM_ORDERS}`],
    'loser_retry_rejection_count': [`count==${NUM_ORDERS}`],
  },
};

export function setup() {
  console.log(`[SETUP] Starting setup for Gate 1.3 Concurrency Test (RUN_ID: ${RUN_ID}, NUM_ORDERS: ${NUM_ORDERS})...`);

  // 1. Login as Merchant
  const merchantLoginRes = http.post(
    `${BASE_URL}/auth.v1.AuthService/Login`,
    JSON.stringify({
      email: __ENV.MERCHANT_EMAIL || 'loja@elo.com',
      password: __ENV.MERCHANT_PASSWORD || 'Merchant@123!',
    }),
    { headers: { 'Content-Type': 'application/json' }, tags: { endpoint: 'MerchantLogin' } }
  );

  if (merchantLoginRes.status !== 200) {
    throw new Error(`Merchant login failed: status=${merchantLoginRes.status} body=${merchantLoginRes.body}`);
  }
  const merchantToken = JSON.parse(merchantLoginRes.body).accessToken;
  console.log('[SETUP] Merchant logged in successfully.');

  // 2. Create NUM_ORDERS orders
  const orders = [];
  for (let i = 0; i < NUM_ORDERS; i++) {
    const createOrderPayload = JSON.stringify({
      customer: {
        name: `gate13-race-${RUN_ID}-${i}`,
        phone: '+5511999999999',
      },
      destination: {
        lat: -23.55052 + (i * 0.0001),
        lng: -46.633308 + (i * 0.0001),
        address: `Av Paulista, ${1000 + i}`,
      },
      packageSize: 1,
      items: [
        {
          name: `item-gate13-${i}`,
          quantity: 1,
          unitPrice: 30.0,
        },
      ],
    });

    const createRes = http.post(
      `${BASE_URL}/logistics.v1.LogisticsService/CreateOrder`,
      createOrderPayload,
      {
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${merchantToken}`,
        },
        tags: { endpoint: 'CreateOrder_Setup' },
      }
    );

    if (createRes.status !== 200) {
      throw new Error(`Failed to create order #${i}: status=${createRes.status} body=${createRes.body}`);
    }

    const orderBody = JSON.parse(createRes.body);
    orders.push({
      orderId: orderBody.orderId,
      deliveryPin: orderBody.deliveryPin,
    });
  }
  console.log(`[SETUP] Successfully created ${orders.length} orders for concurrency testing.`);

  // 3. Login as 10 Deliverers
  const driverTokens = [];
  for (let d = 1; d <= 10; d++) {
    const driverEmail = `gate13_driver_${d}@elo.com`;
    const driverLoginRes = http.post(
      `${BASE_URL}/auth.v1.AuthService/Login`,
      JSON.stringify({
        email: driverEmail,
        password: 'Entregador@123!',
      }),
      { headers: { 'Content-Type': 'application/json' }, tags: { endpoint: 'DriverLogin' } }
    );

    if (driverLoginRes.status !== 200) {
      throw new Error(`Driver login failed for ${driverEmail}: status=${driverLoginRes.status} body=${driverLoginRes.body}`);
    }

    const token = JSON.parse(driverLoginRes.body).accessToken;
    driverTokens.push({
      index: d,
      email: driverEmail,
      token: token,
    });
  }
  console.log(`[SETUP] Successfully authenticated all ${driverTokens.length} drivers.`);

  return {
    orders: orders,
    driverTokens: driverTokens,
    runId: RUN_ID,
  };
}

export default function (data) {
  const iter = exec.scenario.iterationInTest;
  const targetOrder = data.orders[iter];
  if (!targetOrder) {
    throw new Error(`No order available for iteration ${iter}`);
  }

  const orderId = targetOrder.orderId;
  const acceptPayload = JSON.stringify({ orderId: orderId });

  // Prepara 10 requisições simultâneas, uma para cada entregador
  const raceRequests = data.driverTokens.map((driver) => ({
    method: 'POST',
    url: `${BASE_URL}/logistics.v1.LogisticsService/AcceptDelivery`,
    body: acceptPayload,
    params: {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${driver.token}`,
      },
      tags: { endpoint: 'AcceptDelivery_Race', driver: driver.email },
    },
  }));

  const startRace = Date.now();
  // Dispara as 10 requisições em paralelo via http.batch
  const responses = http.batch(raceRequests);
  const raceDuration = Date.now() - startRace;
  raceRoundDuration.add(raceDuration);

  let winnerIndex = -1;
  let winnerDriver = null;
  let winnerCount = 0;
  let loserCount = 0;
  let unexpectedCount = 0;

  responses.forEach((res, idx) => {
    const driver = data.driverTokens[idx];
    if (res.status === 200) {
      winnerCount++;
      winnerIndex = idx;
      winnerDriver = driver;
      raceWinnerCounter.add(1);
    } else if (res.status === 400) {
      loserCount++;
      raceLoserCounter.add(1);
    } else {
      unexpectedCount++;
      raceUnexpectedCounter.add(1);
      console.error(`[UNEXPECTED] Order ${orderId} - Driver ${driver.email} got status ${res.status}: ${res.body}`);
    }
  });

  // Verificações rigorosas da disputa concorrente
  check(responses, {
    'exactly one driver won (status 200)': () => winnerCount === 1,
    'exactly nine drivers rejected (status 400)': () => loserCount === 9,
    'zero unexpected errors (no 500s or timeouts)': () => unexpectedCount === 0,
  });

  if (winnerIndex >= 0) {
    const winRes = responses[winnerIndex];
    let winBody = {};
    try { winBody = JSON.parse(winRes.body); } catch (_) {}
    check(winRes, {
      'winner response has success: true': () => winBody.success === true,
      'winner response has status: DISPATCHED': () => winBody.status === 'DISPATCHED',
    });
  }

  // Verificar mensagem de erro dos perdedores
  let allLosersFailedPrecondition = true;
  responses.forEach((res, idx) => {
    if (idx !== winnerIndex && res.status === 400) {
      try {
        const body = JSON.parse(res.body);
        if (body.code !== 'failed_precondition') {
          allLosersFailedPrecondition = false;
        }
      } catch (_) {
        allLosersFailedPrecondition = false;
      }
    }
  });

  check(responses, {
    'all 9 losers received failed_precondition error code': () => allLosersFailedPrecondition,
  });

  console.log(
    `[RACE #${iter + 1}/${data.orders.length}] Order ${orderId.slice(0, 8)}... ` +
    `Winner: Driver #${winnerIndex + 1} (${winnerDriver ? winnerDriver.email : 'NONE'}) | ` +
    `Duration: ${raceDuration}ms | Winners: ${winnerCount} Losers: ${loserCount} Errs: ${unexpectedCount}`
  );

  // -------------------------------------------------------------
  // Teste de Idempotência e Repetição
  // -------------------------------------------------------------
  if (winnerDriver) {
    // 1. Reenvio pelo MESMO entregador vencedor (deve ser idempotente: status 200, success: true)
    const retryWinnerRes = http.post(
      `${BASE_URL}/logistics.v1.LogisticsService/AcceptDelivery`,
      acceptPayload,
      {
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${winnerDriver.token}`,
        },
        tags: { endpoint: 'AcceptDelivery_IdempotentRetry' },
      }
    );

    let retryBody = {};
    try { retryBody = JSON.parse(retryWinnerRes.body); } catch (_) {}

    const idempotentOk = check(retryWinnerRes, {
      'winner retry returns 200': (r) => r.status === 200,
      'winner retry returns success: true': () => retryBody.success === true,
      'winner retry returns status: DISPATCHED': () => retryBody.status === 'DISPATCHED',
    });

    if (idempotentOk) {
      idempotentRetrySuccessCounter.add(1);
    } else {
      console.error(`[IDEMPOTENCY FAIL] Order ${orderId} retry by winner failed: status=${retryWinnerRes.status} body=${retryWinnerRes.body}`);
    }

    // 2. Reenvio por um dos entregadores PERDEDORES (deve ser rejeitado: status 400, failed_precondition)
    const loserIdx = (winnerIndex + 1) % data.driverTokens.length;
    const loserDriver = data.driverTokens[loserIdx];

    const retryLoserRes = http.post(
      `${BASE_URL}/logistics.v1.LogisticsService/AcceptDelivery`,
      acceptPayload,
      {
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${loserDriver.token}`,
        },
        tags: { endpoint: 'AcceptDelivery_LoserRetry' },
      }
    );

    let loserBody = {};
    try { loserBody = JSON.parse(retryLoserRes.body); } catch (_) {}

    const loserRejectedOk = check(retryLoserRes, {
      'loser retry returns 400': (r) => r.status === 400,
      'loser retry returns failed_precondition': () => loserBody.code === 'failed_precondition',
    });

    if (loserRejectedOk) {
      loserRetryRejectionCounter.add(1);
    } else {
      console.error(`[LOSER RETRY FAIL] Order ${orderId} retry by loser did not reject as expected: status=${retryLoserRes.status} body=${retryLoserRes.body}`);
    }
  }

  // Pacing suave de 20ms entre rodadas de corrida
  sleep(0.02);
}
