/**
 * 6E.8.7.6 — Etapa 3.3: produção regular × entrega irregular, trajetória conhecida.
 *
 * Modelo:
 *   - O veículo percorre uma polilinha de referência a velocidade real constante.
 *   - Um evento é PRODUZIDO a cada 1000 ms (cadência regular, como o simulador).
 *   - Cada evento sofre um ATRASO DE TRANSPORTE gerado por PRNG com seed fixa.
 *   - O consumidor recebe na ordem de CHEGADA → CausalGate → TemporalReplayer.
 *
 * Rodada 1: geometria fixa (L 90°), perfis de atraso variam.
 * Rodada 2: perfil de atraso fixo (mesma seed), geometria varia.
 *
 * Política de reprodução inalterada: DEFAULT_POLICY (15 m/s, 500–3000 ms).
 *
 *   npx tsx lab/replayer-jitter.lab.ts
 */
import {
  TemporalReplayer,
  DEFAULT_POLICY,
  haversineMeters,
  type LatLng,
  type ReplayerLogEntry,
  type Scheduler,
} from "../lib/temporal-replayer";
import { createCausalGate } from "../lib/causal-gate";

const SEED = 0x6e8776;
const PRODUCTION_INTERVAL_MS = 1000;
const TRUE_SPEED_MPS = 10; // velocidade real do veículo (≈36 km/h)
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
  { name: "estável 80–120", sample: (r) => uniform(r, 80, 120) },
  { name: "moderado 0–800", sample: (r) => uniform(r, 0, 800) },
  { name: "alto 0–2500", sample: (r) => uniform(r, 0, 2500) },
  {
    name: "picos 85%/15%",
    sample: (r) => (r() < 0.85 ? uniform(r, 50, 150) : uniform(r, 1500, 3000)),
  },
];

// ───────────────────────── Geometria (metros locais) ─────────────────────────

type XY = { x: number; y: number };
const ORIGIN: LatLng = { lat: -23.5505, lng: -46.6333 };
const M_PER_DEG_LAT = 111320;
const M_PER_DEG_LNG = 111320 * Math.cos((ORIGIN.lat * Math.PI) / 180);

const toLatLng = (p: XY): LatLng => ({
  lat: ORIGIN.lat + p.y / M_PER_DEG_LAT,
  lng: ORIGIN.lng + p.x / M_PER_DEG_LNG,
});
const toXY = (l: LatLng): XY => ({
  x: (l.lng - ORIGIN.lng) * M_PER_DEG_LNG,
  y: (l.lat - ORIGIN.lat) * M_PER_DEG_LAT,
});

class Polyline {
  readonly cum: number[] = [0];
  readonly length: number;

  constructor(readonly name: string, readonly pts: XY[]) {
    for (let i = 1; i < pts.length; i++) {
      this.cum.push(this.cum[i - 1] + Math.hypot(pts[i].x - pts[i - 1].x, pts[i].y - pts[i - 1].y));
    }
    this.length = this.cum[this.cum.length - 1];
  }

  pointAt(s: number): XY {
    const d = Math.min(Math.max(s, 0), this.length);
    for (let i = 1; i < this.pts.length; i++) {
      if (d <= this.cum[i]) {
        const seg = this.cum[i] - this.cum[i - 1];
        const t = seg === 0 ? 0 : (d - this.cum[i - 1]) / seg;
        const a = this.pts[i - 1];
        const b = this.pts[i];
        return { x: a.x + (b.x - a.x) * t, y: a.y + (b.y - a.y) * t };
      }
    }
    return this.pts[this.pts.length - 1];
  }

  /** Projeção no ponto mais próximo: progresso (s) e desvio perpendicular. */
  project(p: XY): { s: number; dist: number } {
    let best = { s: 0, dist: Infinity };
    for (let i = 1; i < this.pts.length; i++) {
      const a = this.pts[i - 1];
      const b = this.pts[i];
      const vx = b.x - a.x;
      const vy = b.y - a.y;
      const len2 = vx * vx + vy * vy;
      const t = len2 === 0 ? 0 : Math.min(Math.max(((p.x - a.x) * vx + (p.y - a.y) * vy) / len2, 0), 1);
      const dist = Math.hypot(p.x - (a.x + vx * t), p.y - (a.y + vy * t));
      if (dist < best.dist) best = { s: this.cum[i - 1] + t * Math.sqrt(len2), dist };
    }
    return best;
  }
}

const GEOMETRIES: Polyline[] = [
  new Polyline("reta", [{ x: 0, y: 0 }, { x: 0, y: 600 }]),
  new Polyline("L 90°", [{ x: 0, y: 0 }, { x: 0, y: 300 }, { x: 300, y: 300 }]),
  new Polyline("zigue-zague", [
    { x: 0, y: 0 }, { x: 0, y: 150 }, { x: 100, y: 250 },
    { x: 0, y: 350 }, { x: 100, y: 450 }, { x: 100, y: 475 },
  ]),
  new Polyline("grampo 180°", [{ x: 0, y: 0 }, { x: 0, y: 280 }, { x: 30, y: 280 }, { x: 30, y: 0 }]),
];

// ───────────────────────── Relógio virtual ─────────────────────────

class VirtualScheduler implements Scheduler {
  private t = 0;
  private nextId = 1;
  private pending = new Map<number, (t: number) => void>();
  maxConcurrentLoops = 0;

  now() { return this.t; }

  requestFrame(cb: (t: number) => void) {
    const id = this.nextId++;
    this.pending.set(id, cb);
    this.maxConcurrentLoops = Math.max(this.maxConcurrentLoops, this.pending.size);
    return id;
  }

  cancelFrame(id: number) { this.pending.delete(id); }

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

/** Qualquer acesso ao relógio real durante a simulação aborta o experimento. */
function withRealClockForbidden<T>(fn: () => T): T {
  const perf = globalThis.performance;
  const origPerfNow = perf.now;
  const origDateNow = Date.now;
  const forbid = () => { throw new Error("relógio real acessado durante a simulação"); };
  Object.defineProperty(perf, "now", { value: forbid, configurable: true, writable: true });
  Date.now = forbid;
  try {
    return fn();
  } finally {
    Object.defineProperty(perf, "now", { value: origPerfNow, configurable: true, writable: true });
    Date.now = origDateNow;
  }
}

// ───────────────────────── Experimento ─────────────────────────

type Ev = { seq: number; producedAt: number; delay: number; arrivalAt: number; s: number; pos: LatLng };

function buildEvents(path: Polyline, profile: DelayProfile): Ev[] {
  const rng = mulberry32(SEED);
  // Atrasos gerados antes e independentemente da geometria:
  // mesma seed + mesmo perfil ⇒ mesma sequência de atrasos em qualquer trajetória.
  const delays = Array.from({ length: MAX_EVENTS }, () => profile.sample(rng));

  const n = Math.floor(path.length / (TRUE_SPEED_MPS * PRODUCTION_INTERVAL_MS / 1000)) + 1;
  const events: Ev[] = [];
  for (let i = 0; i < n; i++) {
    const producedAt = i * PRODUCTION_INTERVAL_MS;
    const s = Math.min((producedAt / 1000) * TRUE_SPEED_MPS, path.length);
    const delay = i === 0 ? 0 : Math.round(delays[i]); // seq inicial = snapshot
    events.push({
      seq: FIRST_SEQ + i,
      producedAt,
      delay,
      arrivalAt: producedAt + delay,
      s,
      pos: toLatLng(path.pointAt(s)),
    });
  }
  return events;
}

type Metrics = {
  scenario: string;
  produced: number;
  accepted: number;
  rejected: number;
  interrupted: number;
  neverPursued: number;
  clamped: number;
  vMaxMps: number;
  vP95Mps: number;
  vMeanMps: number;
  devMaxM: number;
  devMeanM: number;
  spatialBackMaxM: number;
  lagMaxM: number;
  maxLoops: number;
  invariantsFailed: string[];
};

function runExperiment(label: string, path: Polyline, profile: DelayProfile) {
  const events = buildEvents(path, profile);
  const arrivals = [...events].sort((a, b) => a.arrivalAt - b.arrivalAt || a.seq - b.seq);

  const scheduler = new VirtualScheduler();
  const gate = createCausalGate();
  const logs: ReplayerLogEntry[] = [];
  const frames: { t: number; pos: LatLng }[] = [];
  const rejected = new Set<number>();
  const acceptedS: { t: number; s: number }[] = [];

  const replayer = new TemporalReplayer(
    scheduler,
    (pos) => frames.push({ t: scheduler.now(), pos }),
    DEFAULT_POLICY,
    (e) => logs.push(e),
  );

  withRealClockForbidden(() => {
    for (const ev of arrivals) {
      scheduler.advanceTo(ev.arrivalAt);
      if (!gate.offer(BigInt(ev.seq))) {
        rejected.add(ev.seq);
        continue;
      }
      acceptedS.push({ t: ev.arrivalAt, s: ev.s });
      replayer.setTarget(ev.pos, BigInt(ev.seq));
    }
    scheduler.advanceTo(arrivals[arrivals.length - 1].arrivalAt + 5000);
  });

  // ── Invariantes (duras) ──
  const failed: string[] = [];
  if (logs[0]?.mode !== "teleport") failed.push("snapshot-teleport");

  const seqs = logs.map((l) => Number(l.sequence));
  if (!seqs.every((s, i) => i === 0 || s > seqs[i - 1])) failed.push("ordem-causal");
  if (seqs.some((s) => rejected.has(s))) failed.push("rejeitado-animou");
  if (scheduler.maxConcurrentLoops > 1) failed.push("loops-concorrentes");

  for (let i = 1; i < logs.length; i++) {
    const prev = logs[i - 1];
    const cur = logs[i];
    if (cur.mode !== "interpolate" || prev.mode !== "interpolate") continue;
    const p = Math.min(Math.max((cur.event_received_at - prev.event_received_at) / prev.calculated_duration_ms, 0), 1);
    const from = prev.visual_position_at_start!;
    const expected = {
      lat: from.lat + (prev.destination.lat - from.lat) * p,
      lng: from.lng + (prev.destination.lng - from.lng) * p,
    };
    if (haversineMeters(expected, cur.visual_position_at_start!) > 1e-6) {
      failed.push(`partida-visual(seq=${cur.sequence})`);
      break;
    }
  }

  // ── Métricas ──
  const interruptedLogs = logs.filter((l) => l.animation_cancelled);
  let neverPursued = 0;
  for (let i = 1; i < logs.length; i++) {
    if (!logs[i].animation_cancelled) continue;
    const from = logs[i - 1].event_received_at;
    const to = logs[i].event_received_at;
    if (!frames.some((f) => f.t > from && f.t <= to)) neverPursued++;
  }

  const speeds: number[] = [];
  let movingDist = 0;
  let movingTime = 0;
  let devMax = 0;
  let devSum = 0;
  let backMax = 0;
  let lagMax = 0;
  let prevS: number | null = null;
  let accIdx = -1;

  for (let i = 0; i < frames.length; i++) {
    const f = frames[i];
    const proj = path.project(toXY(f.pos));
    devMax = Math.max(devMax, proj.dist);
    devSum += proj.dist;

    if (prevS !== null) backMax = Math.max(backMax, prevS - proj.s);
    prevS = proj.s;

    while (accIdx + 1 < acceptedS.length && acceptedS[accIdx + 1].t <= f.t) accIdx++;
    if (accIdx >= 0) lagMax = Math.max(lagMax, acceptedS[accIdx].s - proj.s);

    if (i > 0) {
      const dt = f.t - frames[i - 1].t;
      const d = haversineMeters(frames[i - 1].pos, f.pos);
      // Só intervalos entre frames consecutivos de animação (exclui ociosidade).
      if (dt > 0 && dt <= FRAME_MS && d > 1e-9) {
        speeds.push(d / (dt / 1000));
        movingDist += d;
        movingTime += dt / 1000;
      }
    }
  }
  speeds.sort((a, b) => a - b);

  const metrics: Metrics = {
    scenario: label,
    produced: events.length,
    accepted: logs.length,
    rejected: rejected.size,
    interrupted: interruptedLogs.length,
    neverPursued,
    clamped: logs.filter((l) => l.clamped && l.raw_duration_ms > DEFAULT_POLICY.maxDurationMs).length,
    vMaxMps: speeds[speeds.length - 1] ?? 0,
    vP95Mps: speeds[Math.floor(speeds.length * 0.95)] ?? 0,
    vMeanMps: movingTime > 0 ? movingDist / movingTime : 0,
    devMaxM: devMax,
    devMeanM: frames.length ? devSum / frames.length : 0,
    spatialBackMaxM: backMax,
    lagMaxM: lagMax,
    maxLoops: scheduler.maxConcurrentLoops,
    invariantsFailed: failed,
  };

  return { metrics, events };
}

// ───────────────────────── Relatório ─────────────────────────

const f1 = (n: number) => n.toFixed(1);

function table(rows: Metrics[]) {
  console.table(
    rows.map((m) => ({
      cenário: m.scenario,
      "prod": m.produced,
      "aceitos": m.accepted,
      "rejeit.": m.rejected,
      "interromp.": m.interrupted,
      "0 frames": m.neverPursued,
      "clamp": m.clamped,
      "v máx m/s": f1(m.vMaxMps),
      "v p95": f1(m.vP95Mps),
      "v média": f1(m.vMeanMps),
      "desvio máx m": f1(m.devMaxM),
      "desvio méd": f1(m.devMeanM),
      "retroc. máx m": f1(m.spatialBackMaxM),
      "atraso máx m": f1(m.lagMaxM),
      "loops": m.maxLoops,
      "invariantes": m.invariantsFailed.length === 0 ? "OK" : m.invariantsFailed.join(","),
    })),
  );
}

function runAll() {
  const L = GEOMETRIES[1];
  const round1 = PROFILES.map((p) => runExperiment(p.name, L, p));
  const high = PROFILES[2];
  const round2 = GEOMETRIES.map((g) => runExperiment(g.name, g, high));
  return { round1, round2 };
}

console.log(`\nseed=0x${SEED.toString(16)} | produção a cada ${PRODUCTION_INTERVAL_MS} ms | ` +
  `veículo ${TRUE_SPEED_MPS} m/s | política ${DEFAULT_POLICY.visualSpeedMps} m/s, ` +
  `${DEFAULT_POLICY.minDurationMs}–${DEFAULT_POLICY.maxDurationMs} ms`);

const first = runAll();

console.log("\n── Amostra de chegada (perfil 'alto 0–2500', L 90°) ──");
const sample = first.round1[2].events.slice(0, 10);
const order = [...sample].sort((a, b) => a.arrivalAt - b.arrivalAt || a.seq - b.seq).map((e) => e.seq);
console.table(sample.map((e) => ({ seq: e.seq, produzido: e.producedAt, atraso: e.delay, chegada: e.arrivalAt })));
console.log(`  ordem de chegada (primeiros 10): ${order.join(" → ")}`);

console.log("\n══ Rodada 1 — geometria fixa (L 90°), perfis de atraso variam ══");
table(first.round1.map((r) => r.metrics));

console.log("\n══ Rodada 2 — perfil fixo ('alto 0–2500', mesma seed), geometria varia ══");
table(first.round2.map((r) => r.metrics));

// Determinismo: mesma seed ⇒ resultado idêntico, bit a bit.
const second = runAll();
const same = JSON.stringify(first) === JSON.stringify(second);
const allMetrics = [...first.round1, ...first.round2].map((r) => r.metrics);
const invariantsOk = allMetrics.every((m) => m.invariantsFailed.length === 0);

console.log("\n── Verificações ──");
console.log(`  ${invariantsOk ? "PASS" : "FAIL"}  invariantes causais/temporais em todos os ${allMetrics.length} cenários`);
console.log(`  ${same ? "PASS" : "FAIL"}  duas execuções com a mesma seed produzem resultados idênticos`);
console.log(`  PASS  relógio real (performance.now / Date.now) bloqueado durante toda a simulação`);
console.log(`  INFO  retrocesso espacial é métrica, não invariante: a interpolação linear pode recuar\n` +
  `        em relação ao progresso na rota quando corta curvas.\n`);

process.exit(invariantsOk && same ? 0 : 1);
