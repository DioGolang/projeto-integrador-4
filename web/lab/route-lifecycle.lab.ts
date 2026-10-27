/**
 * 6E.8.7.6 — Validação do Ciclo de Vida da Rota no TemporalReplayer (Opção A)
 *
 * Cenários Obrigatórios:
 * 1. Snapshot com rota R1         → Reprodutor inicializado com R1
 * 2. Evento de posição sem rota    → R1 permanece ativa
 * 3. Nova rota R2                  → R2 substitui R1 sem salto artificial na posição visual
 * 4. Repetição de R2               → Nenhuma substituição desnecessária (ignorado)
 * 5. Atualização de rota inválida  → Rota atual preservada
 * 6. Troca de pedido               → A rota do pedido anterior não vaza para o novo
 *
 *   npx tsx lab/route-lifecycle.lab.ts
 */

import {
  TemporalReplayer,
  DEFAULT_POLICY,
  haversineMeters,
  type LatLng,
  type Scheduler,
} from "../lib/temporal-replayer";
import { RoutePolyline } from "../lib/route-polyline";

// ───────────────────────── Relógio Virtual ─────────────────────────

class VirtualScheduler implements Scheduler {
  private t = 0;
  private nextId = 1;
  private pending = new Map<number, (t: number) => void>();
  maxConcurrentLoops = 0;

  now() {
    return this.t;
  }

  requestFrame(cb: (t: number) => void) {
    const id = this.nextId++;
    this.pending.set(id, cb);
    this.maxConcurrentLoops = Math.max(this.maxConcurrentLoops, this.pending.size);
    return id;
  }

  cancelFrame(id: number) {
    this.pending.delete(id);
  }

  advanceTo(target: number) {
    const FRAME_MS = 16;
    let next = Math.floor(this.t / FRAME_MS) * FRAME_MS + FRAME_MS;
    while (next <= target) {
      this.t = next;
      const cbs = Array.from(this.pending.values());
      this.pending.clear();
      for (const cb of cbs) cb(this.t);
      next += FRAME_MS;
    }
    this.t = target;
  }
}

// ───────────────────────── Rotas de Teste ─────────────────────────

// Rota 1 (Leg TO_PICKUP): Ponto A -> Restaurante (Restaurante em -23.5500, -46.6330)
const R1_POINTS: LatLng[] = [
  { lat: -23.5550, lng: -46.6330 },
  { lat: -23.5500, lng: -46.6330 },
];

// Rota 2 (Leg TO_DROPOFF): Começa no Restaurante (-23.5500, -46.6330) -> Cliente (-23.5450, -46.6300)
const R2_POINTS: LatLng[] = [
  { lat: -23.5500, lng: -46.6330 },
  { lat: -23.5480, lng: -46.6330 },
  { lat: -23.5450, lng: -46.6300 },
];

let failures = 0;
function check(cond: boolean, name: string) {
  console.log(`  ${cond ? "PASS" : "FAIL"}  ${name}`);
  if (!cond) failures++;
}

console.log("===============================================================================");
console.log("6E.8.7.6 — TESTES DO CICLO DE VIDA DA ROTA (CONTRATO OPÇÃO A)");
console.log("===============================================================================\n");

const scheduler = new VirtualScheduler();
let lastPos: LatLng | null = null;
const replayer = new TemporalReplayer(
  scheduler,
  (pos) => {
    lastPos = pos;
  },
  DEFAULT_POLICY,
);

// ───────────────────── 1. Snapshot com rota R1 ─────────────────────
console.log("── 1. Snapshot com rota R1 ──");
const okR1 = replayer.setRoute(R1_POINTS, {
  routeId: "order-1-route",
  revision: BigInt(1),
  leg: "TO_PICKUP",
});
check(okR1, "setRoute aceitou R1 válida");
check(replayer.currentRoute !== null, "Reprodutor inicializado com R1");
check(replayer.currentMetadata?.routeId === "order-1-route", "RouteId correto gravado");
check(replayer.currentMetadata?.revision === BigInt(1), "Revisão 1 gravada");
check(replayer.currentMetadata?.leg === "TO_PICKUP", "Trecho TO_PICKUP ativo");

// Posiciona no snapshot (P0 da R1)
replayer.setTarget(R1_POINTS[0], BigInt(4));
check(lastPos !== null, "Posição visual inicializada pelo snapshot");
check(haversineMeters(lastPos!, R1_POINTS[0]) < 1e-4, "Posição visual coincide com início da R1");

// ───────────────────── 2. Evento de posição sem rota ─────────────────────
console.log("\n── 2. Evento de posição sem rota (atualização de telemetria) ──");
scheduler.advanceTo(1000);
// Próximo ponto ao longo da R1
const midR1: LatLng = { lat: -23.5525, lng: -46.6330 };
replayer.setTarget(midR1, BigInt(5));

check(replayer.currentRoute !== null, "R1 permanece ativa");
check(replayer.currentMetadata?.revision === BigInt(1), "Revisão 1 permanece ativa");
check(replayer.isAnimating, "Animação iniciada ao longo da R1");

// Avança tempo para animar parcialmente
scheduler.advanceTo(1500);
check(lastPos !== null, "Marcador navegando sobre a R1");

// ───────────────────── 3. Nova rota R2 (transição de trecho) ─────────────────────
console.log("\n── 3. Nova rota R2 (sem salto artificial na posição visual) ──");
// Chega ao restaurante no final da R1
scheduler.advanceTo(3000);
replayer.setTarget(R1_POINTS[1], BigInt(6));
// Aguarda a conclusão da animação até o restaurante (t = 3000 + 3000ms de duração máxima)
scheduler.advanceTo(6000);

const posBeforeR2 = { ...replayer.sample(scheduler.now())! };

// Evento com nova rota R2 (Leg TO_DROPOFF, revision 2)
const okR2 = replayer.setRoute(R2_POINTS, {
  routeId: "order-1-route",
  revision: BigInt(2),
  leg: "TO_DROPOFF",
});
const posAfterR2 = { ...replayer.sample(scheduler.now())! };

check(okR2, "setRoute aceitou R2 com revisão BigInt(2)");
check(replayer.currentMetadata?.revision === BigInt(2), "Revisão atualizada para BigInt(2)");
check(replayer.currentMetadata?.leg === "TO_DROPOFF", "Trecho atualizado para TO_DROPOFF");

const transitionJumpM = haversineMeters(posBeforeR2, posAfterR2);
check(
  transitionJumpM < 1e-3,
  `Transição suave sem salto artificial (salto = ${transitionJumpM.toFixed(4)}m)`,
);
check(scheduler.maxConcurrentLoops === 1, "Exatamente 1 loop de animação concorrente ativo");

// ───────────────────── 4. Repetição de R2 ─────────────────────
console.log("\n── 4. Repetição de R2 (mesma rota e revisão) ──");
const okDupR2 = replayer.setRoute(R2_POINTS, {
  routeId: "order-1-route",
  revision: BigInt(2),
  leg: "TO_DROPOFF",
});
check(!okDupR2, "Repetição de R2 foi ignorada (nenhuma substituição desnecessária)");
check(replayer.currentMetadata?.revision === BigInt(2), "Revisão permanece BigInt(2)");

// ───────────────────── 5. Atualização de rota inválida ─────────────────────
console.log("\n── 5. Atualização de rota inválida (menos de 2 pontos) ──");
const okInvalid = replayer.setRoute(
  [{ lat: -23.5500, lng: -46.6330 }], // apenas 1 ponto: inválido!
  { routeId: "order-1-route", revision: BigInt(3), leg: "TO_DROPOFF" },
);
check(!okInvalid, "Rota inválida rejeitada com sucesso");
check(replayer.currentMetadata?.revision === BigInt(2), "Rota R2 anterior preservada intacta");
check(replayer.currentRoute?.points.length === R2_POINTS.length, "Geometria de R2 mantida");

// ───────────────────── 6. Troca de pedido ─────────────────────
console.log("\n── 6. Troca de pedido (limpeza de estado e sem vazamento) ──");
replayer.dispose();

check(replayer.currentRoute === null, "Rota do pedido anterior limpa");
check(replayer.currentMetadata === null, "Metadados limpos");
check(replayer.hasPosition === false, "Posição visual limpa (sem herança)");
check(replayer.isAnimating === false, "Nenhuma animação ativa pendente");

// Novo pedido 2 com rota R3
const R3_POINTS: LatLng[] = [
  { lat: -23.6000, lng: -46.7000 },
  { lat: -23.6050, lng: -46.7050 },
];
replayer.setRoute(R3_POINTS, {
  routeId: "order-2-route",
  revision: BigInt(1),
  leg: "TO_PICKUP",
});
replayer.setTarget(R3_POINTS[0], BigInt(1));

check(replayer.currentMetadata?.routeId === "order-2-route", "Novo pedido inicializado com R3");
check(
  haversineMeters(replayer.sample(scheduler.now())!, R3_POINTS[0]) < 1e-4,
  "Marcador do novo pedido inicia em R3 sem vazar do pedido anterior",
);

console.log("\n===============================================================================");
console.log(
  failures === 0
    ? "✔ TODOS OS 6 CENÁRIOS DE CICLO DE VIDA FORAM APROVADOS COM SUCESSO"
    : `✘ ${failures} CENÁRIO(S) FALHARAM`,
);
console.log("===============================================================================\n");

process.exit(failures === 0 ? 0 : 1);
