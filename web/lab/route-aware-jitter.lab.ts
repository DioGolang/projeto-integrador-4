/**
 * 6E.8.7.6 — Validação do TemporalReplayer com Reprodução Consciente da Rota (v2)
 *
 * Separação estrita dos cenários de teste:
 * 1. Jitter de Transporte (In-Order TCP):
 *    - Mensagens entregues estritamente em ordem causal (FIFO).
 *    - Latência variável produz intervalos irregulares e rajadas (head-of-line unblocking).
 * 2. Reordenamento na Aplicação (Resiliência Causal):
 *    - Mensagens entregues fora de ordem para validar o CausalGate e descarte de atrasados.
 *
 * Comparação direta entre:
 * - Replayer v1 (Interpolação direta / corte de esquinas)
 * - Replayer v2 (Consciente da rota / reprodução por polilinha)
 *
 *   npx tsx lab/route-aware-jitter.lab.ts
 */

import {
  TemporalReplayer,
  DEFAULT_POLICY,
  haversineMeters,
  type LatLng,
  type ReplayerLogEntry,
  type Scheduler,
} from "../lib/temporal-replayer";
import { RoutePolyline } from "../lib/route-polyline";
import { createCausalGate } from "../lib/causal-gate";

const SEED = 0x6e8776;
const PRODUCTION_INTERVAL_MS = 1000;
const TRUE_SPEED_MPS = 10; // 10 m/s (36 km/h)
const FIRST_SEQ = 4;
const MAX_EVENTS = 100;
const FRAME_MS = 16;

// ───────────────────────── PRNG determinístico ─────────────────────────

function mulberry32(seed: number) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

type Rng = () => number;
const uniform = (r: Rng, lo: number, hi: number) => lo + (hi - lo) * r();

type DelayProfile = { name: string; sample: (r: Rng) => number };

const PROFILES: DelayProfile[] = [
  { name: "estável (80–120ms)", sample: (r) => uniform(r, 80, 120) },
  { name: "moderado (0–800ms)", sample: (r) => uniform(r, 0, 800) },
  { name: "alto (0–2500ms)", sample: (r) => uniform(r, 0, 2500) },
  {
    name: "picos 85%/15%",
    sample: (r) => (r() < 0.85 ? uniform(r, 50, 150) : uniform(r, 1500, 3000)),
  },
];

// ───────────────────────── Geometria e Polilinhas ─────────────────────────

type XY = { x: number; y: number };
const ORIGIN: LatLng = { lat: -23.5505, lng: -46.6333 };
const M_PER_DEG_LAT = 111320;
const M_PER_DEG_LNG = 111320 * Math.cos((ORIGIN.lat * Math.PI) / 180);

const toLatLng = (p: XY): LatLng => ({
  lat: ORIGIN.lat + p.y / M_PER_DEG_LAT,
  lng: ORIGIN.lng + p.x / M_PER_DEG_LNG,
});

type GeoRoute = {
  name: string;
  polyline: RoutePolyline;
};

function createGeoRoute(name: string, ptsXY: XY[]): GeoRoute {
  const latLngs = ptsXY.map(toLatLng);
  return {
    name,
    polyline: new RoutePolyline(latLngs),
  };
}

const GEOMETRIES: GeoRoute[] = [
  createGeoRoute("reta", [{ x: 0, y: 0 }, { x: 0, y: 600 }]),
  createGeoRoute("L 90°", [{ x: 0, y: 0 }, { x: 0, y: 300 }, { x: 300, y: 300 }]),
  createGeoRoute("zigue-zague", [
    { x: 0, y: 0 },
    { x: 0, y: 150 },
    { x: 100, y: 250 },
    { x: 0, y: 350 },
    { x: 100, y: 450 },
    { x: 100, y: 475 },
  ]),
  createGeoRoute("grampo 180°", [
    { x: 0, y: 0 },
    { x: 0, y: 280 },
    { x: 30, y: 280 },
    { x: 30, y: 0 },
  ]),
];

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

function withRealClockForbidden<T>(fn: () => T): T {
  const perf = globalThis.performance;
  const origPerfNow = perf.now;
  const origDateNow = Date.now;
  const forbid = () => {
    throw new Error("relógio real acessado durante a simulação");
  };
  Object.defineProperty(perf, "now", { value: forbid, configurable: true, writable: true });
  Date.now = forbid;
  try {
    return fn();
  } finally {
    Object.defineProperty(perf, "now", { value: origPerfNow, configurable: true, writable: true });
    Date.now = origDateNow;
  }
}

// ───────────────────────── Geração de Eventos ─────────────────────────

type ProducedEvent = {
  seq: number;
  producedAt: number;
  delayMs: number;
  arrivalAt: number;
  s: number;
  pos: LatLng;
};

function generateEvents(
  route: GeoRoute,
  profile: DelayProfile,
  deliveryMode: "in_order_tcp" | "out_of_order_app",
): ProducedEvent[] {
  const rng = mulberry32(SEED);
  const delays = Array.from({ length: MAX_EVENTS }, () => profile.sample(rng));

  const totalDist = route.polyline.totalLength;
  const n = Math.floor(totalDist / ((TRUE_SPEED_MPS * PRODUCTION_INTERVAL_MS) / 1000)) + 1;
  const events: ProducedEvent[] = [];

  let lastArrival = 0;

  for (let i = 0; i < n; i++) {
    const producedAt = i * PRODUCTION_INTERVAL_MS;
    const s = Math.min((producedAt / 1000) * TRUE_SPEED_MPS, totalDist);
    const pos = route.polyline.pointAt(s);
    const delay = i === 0 ? 0 : Math.round(delays[i]);

    let arrivalAt = producedAt + delay;

    if (deliveryMode === "in_order_tcp") {
      // TCP stream: head-of-line blocking guarantees messages arrive in FIFO order.
      // If packet i was delayed, subsequent packets arrive with or after it (bursts).
      arrivalAt = Math.max(lastArrival, arrivalAt);
      lastArrival = arrivalAt;
    }

    events.push({
      seq: FIRST_SEQ + i,
      producedAt,
      delayMs: delay,
      arrivalAt,
      s,
      pos,
    });
  }

  return events;
}

// ───────────────────────── Execução do Replayer ─────────────────────────

type Metrics = {
  scenario: string;
  mode: "v1_direto" | "v2_rota";
  delivery: "tcp_em_ordem" | "app_fora_de_ordem";
  produced: number;
  accepted: number;
  rejected: number;
  interrupted: number;
  neverPursued: number;
  vMaxMps: number;
  vMeanMps: number;
  devMaxM: number;
  devMeanM: number;
  maxLoops: number;
  invariantsFailed: string[];
};

function runTest(
  label: string,
  route: GeoRoute,
  profile: DelayProfile,
  deliveryMode: "in_order_tcp" | "out_of_order_app",
  useRouteAware: boolean,
): Metrics {
  const events = generateEvents(route, profile, deliveryMode);
  // Sort arrivals in the order they hit the consumer
  const arrivals = [...events].sort((a, b) => a.arrivalAt - b.arrivalAt || a.seq - b.seq);

  const scheduler = new VirtualScheduler();
  const gate = createCausalGate();
  const logs: ReplayerLogEntry[] = [];
  const frames: { t: number; pos: LatLng }[] = [];
  const rejected = new Set<number>();

  const replayer = new TemporalReplayer(
    scheduler,
    (pos) => frames.push({ t: scheduler.now(), pos }),
    DEFAULT_POLICY,
    (e) => logs.push(e),
    useRouteAware ? route.polyline : null, // v2 usa polilinha, v1 usa null (direto)
  );

  withRealClockForbidden(() => {
    for (const ev of arrivals) {
      scheduler.advanceTo(ev.arrivalAt);
      if (!gate.offer(BigInt(ev.seq))) {
        rejected.add(ev.seq);
        continue;
      }
      replayer.setTarget(ev.pos, BigInt(ev.seq));
    }
    scheduler.advanceTo(arrivals[arrivals.length - 1].arrivalAt + 5000);
  });

  // Verificação de Invariantes
  const failed: string[] = [];
  if (logs[0]?.mode !== "teleport") failed.push("snapshot-teleport");
  const seqs = logs.map((l) => Number(l.sequence));
  if (!seqs.every((s, i) => i === 0 || s > seqs[i - 1])) failed.push("ordem-causal");
  if (seqs.some((s) => rejected.has(s))) failed.push("rejeitado-animou");
  if (scheduler.maxConcurrentLoops > 1) failed.push("loops-concorrentes");

  // Posição de partida visual
  for (let i = 1; i < logs.length; i++) {
    const cur = logs[i];
    if (cur.mode === "interpolate" && cur.animation_cancelled) {
      if (!cur.visual_position_at_start) {
        failed.push(`partida-nula(seq=${cur.sequence})`);
      }
    }
  }

  // Métricas
  const speeds: number[] = [];
  let movingDist = 0;
  let movingTime = 0;
  let devMax = 0;
  let devSum = 0;

  for (let i = 0; i < frames.length; i++) {
    const f = frames[i];
    const proj = route.polyline.project(f.pos);
    devMax = Math.max(devMax, proj.distanceM);
    devSum += proj.distanceM;

    if (i > 0) {
      const dt = f.t - frames[i - 1].t;
      const d = haversineMeters(frames[i - 1].pos, f.pos);
      if (dt > 0 && dt <= FRAME_MS && d > 1e-9) {
        speeds.push(d / (dt / 1000));
        movingDist += d;
        movingTime += dt / 1000;
      }
    }
  }
  speeds.sort((a, b) => a - b);

  let neverPursued = 0;
  for (let i = 1; i < logs.length; i++) {
    if (!logs[i].animation_cancelled) continue;
    const fromT = logs[i - 1].event_received_at;
    const toT = logs[i].event_received_at;
    if (!frames.some((f) => f.t > fromT && f.t <= toT)) neverPursued++;
  }

  return {
    scenario: label,
    mode: useRouteAware ? "v2_rota" : "v1_direto",
    delivery: deliveryMode === "in_order_tcp" ? "tcp_em_ordem" : "app_fora_de_ordem",
    produced: events.length,
    accepted: logs.length,
    rejected: rejected.size,
    interrupted: logs.filter((l) => l.animation_cancelled).length,
    neverPursued,
    vMaxMps: speeds[speeds.length - 1] ?? 0,
    vMeanMps: movingTime > 0 ? movingDist / movingTime : 0,
    devMaxM: devMax,
    devMeanM: frames.length ? devSum / frames.length : 0,
    maxLoops: scheduler.maxConcurrentLoops,
    invariantsFailed: failed,
  };
}

// ───────────────────────── Execução e Tabelas ─────────────────────────

console.log("===============================================================================");
console.log("6E.8.7.6 — EXPERIMENTO: TEMPORAL REPLAYER CONSCIENTE DA ROTA (v2) vs DIRETO (v1)");
console.log(`seed: 0x${SEED.toString(16)} | produção: ${PRODUCTION_INTERVAL_MS}ms | velocidade: ${TRUE_SPEED_MPS}m/s`);
console.log("===============================================================================\n");

const f1 = (n: number) => n.toFixed(1);

function printTable(rows: Metrics[]) {
  console.table(
    rows.map((m) => ({
      cenário: m.scenario,
      modo: m.mode,
      transporte: m.delivery,
      "prod": m.produced,
      "aceitos": m.accepted,
      "rejeit.": m.rejected,
      "interromp.": m.interrupted,
      "0 frames": m.neverPursued,
      "v máx m/s": f1(m.vMaxMps),
      "v méd m/s": f1(m.vMeanMps),
      "desvio máx (m)": f1(m.devMaxM),
      "desvio méd (m)": f1(m.devMeanM),
      "loops": m.maxLoops,
      "invariantes": m.invariantsFailed.length === 0 ? "OK" : m.invariantsFailed.join(","),
    })),
  );
}

// 1. COMPARAÇÃO V1 vs V2 EM TODAS AS GEOMETRIAS SOB JITTER TCP EM ORDEM (Alto 0-2500ms)
console.log("── PARTE 1: Comparação de Fidelidade Geométrica (Transporte TCP em Ordem, Jitter Alto) ──");
const highJitter = PROFILES[2];
const geomComparisons: Metrics[] = [];

for (const g of GEOMETRIES) {
  geomComparisons.push(runTest(`${g.name} [v1 Direto]`, g, highJitter, "in_order_tcp", false));
  geomComparisons.push(runTest(`${g.name} [v2 Rota]`, g, highJitter, "in_order_tcp", true));
}
printTable(geomComparisons);

// 2. SEPARAÇÃO FORMAL: JITTER DE TRANSPORTE (TCP) vs REORDENAMENTO NA APLICAÇÃO
console.log("\n── PARTE 2: Separação Formal: Transporte TCP em Ordem vs Reordenamento na Aplicação ──");
const lRoute = GEOMETRIES[1]; // L 90°
const deliveryComparisons: Metrics[] = [
  runTest("L 90° [TCP em ordem, estável]", lRoute, PROFILES[0], "in_order_tcp", true),
  runTest("L 90° [TCP em ordem, alto jitter]", lRoute, PROFILES[2], "in_order_tcp", true),
  runTest("L 90° [TCP em ordem, rajadas/picos]", lRoute, PROFILES[3], "in_order_tcp", true),
  runTest("L 90° [Aplicação fora de ordem, alto]", lRoute, PROFILES[2], "out_of_order_app", true),
];
printTable(deliveryComparisons);

// Verificações
const allTests = [...geomComparisons, ...deliveryComparisons];
const invariantsPass = allTests.every((t) => t.invariantsFailed.length === 0);

// Assertivas Chave de Aceite
const v2ZeroDeviation = geomComparisons
  .filter((m) => m.mode === "v2_rota")
  .every((m) => m.devMaxM < 0.05); // sub-decímetro

const tcpZeroRejections = deliveryComparisons
  .filter((m) => m.delivery === "tcp_em_ordem")
  .every((m) => m.rejected === 0);

const appHasRejections = deliveryComparisons.find(
  (m) => m.delivery === "app_fora_de_ordem",
)!.rejected > 0;

console.log("\n── Critérios de Aceite Verificados ──");
console.log(`  ${invariantsPass ? "PASS" : "FAIL"}  Todas as invariantes causais e de concorrência válidas`);
console.log(`  ${v2ZeroDeviation ? "PASS" : "FAIL"}  Geometria v2: Desvio lateral máximo = 0.0m em todas as rotas (sem corte de quarteirão)`);
console.log(`  ${tcpZeroRejections ? "PASS" : "FAIL"}  Transporte TCP: 0% de rejeição causal (ordem de entrega garantida por stream)`);
console.log(`  ${appHasRejections ? "PASS" : "FAIL"}  Aplicação fora de ordem: CausalGate rejeita eventos atrasados sem corromper estado`);

const allPass = invariantsPass && v2ZeroDeviation && tcpZeroRejections && appHasRejections;
console.log(`\n${allPass ? "✔ TODOS OS CRITÉRIOS DE ACEITE ATENDIDOS COM SUCESSO" : "✘ FALHA NOS CRITÉRIOS"}\n`);

process.exit(allPass ? 0 : 1);
