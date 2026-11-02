import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  vus: 2,
  duration: '30s',
  thresholds: {
    // Interrupção automática se a taxa de falha for >= 0.1% (sucesso < 99.9%)
    'http_req_failed': [{ threshold: 'rate<0.001', abortOnFail: true, delayAbortEval: '1s' }],
  },
};

export default function () {
  // Chamada simulada para endpoint inexistente que retorna 404 para acionar o circuit breaker
  http.get('http://api:8080/nonexistent_circuit_breaker_test');
  sleep(0.1);
}
