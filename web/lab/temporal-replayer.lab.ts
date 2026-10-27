/**
 * 6E.8.7.6 — Etapa 3: Temporal Replayer sob jitter controlado (determinístico).
 *
 * Executa o MESMO código usado pelo DelivererMap (lib/temporal-replayer.ts e
 * lib/causal-gate.ts), mas com um relógio virtual: nenhum setTimeout real,
 * nenhuma aleatoriedade. Cada execução produz exatamente a mesma saída.
 *
 *   npx tsx lab/temporal-replayer.lab.ts
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

// ───────────────────────── Relógio virtual ─────────────────────────

class VirtualScheduler implements Scheduler {
  private t = 0;
  private nextId = 1;
  private pending = new Map<number, (t: number) => void>();
  readonly frameMs = 16;
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

  /** Avança o tempo executando os frames que caem no intervalo. */
  advanceTo(target: number) {
    let next = Math.floor(this.t / this.frameMs) * this.frameMs + this.frameMs;
    while (next <= target) {
      this.t = next;
      const cbs = Array.from(this.pending.values());
      this.pending.clear();
      for (const cb of cbs) cb(this.t);
      next += this.frameMs;
    }
    this.t = target;
  }
}

// ───────────────────────── Utilitários ─────────────────────────

const P0: LatLng = { lat: -23.5505, lng: -46.6333 };
// Trajeto reto para o norte: cada passo = 3e-4° de latitude ≈ 33,4 m.
const P = (i: number): LatLng => ({ lat: P0.lat + i * 3e-4, lng: P0.lng });

const fmt = (p: LatLng | null) =>
  p ? `(${p.lat.toFixed(6)}, ${p.lng.toFixed(6)})` : "—";

let failures = 0;
function check(cond: boolean, msg: string) {
  console.log(`  ${cond ? "PASS" : "FAIL"}  ${msg}`);
  if (!cond) failures++;
}

function printLog(logs: ReplayerLogEntry[]) {
  console.table(
    logs.map((l) => ({
      seq: l.sequence,
      mode: l.mode,
      received_at_ms: l.event_received_at,
      visual_start: fmt(l.visual_position_at_start),
      destination: fmt(l.destination),
      distance_m: l.distance_m.toFixed(1),
      raw_ms: l.raw_duration_ms.toFixed(0),
      duration_ms: l.calculated_duration_ms.toFixed(0),
      clamped: l.clamped,
      cancelled: l.animation_cancelled,
    })),
  );
}

type ScriptedEvent = { atMs: number; seq: number; pos: LatLng };

function run(events: ScriptedEvent[], settleMs = 5000) {
  const scheduler = new VirtualScheduler();
  const gate = createCausalGate();
  const logs: ReplayerLogEntry[] = [];
  const frames: { t: number; pos: LatLng }[] = [];
  const rejected: number[] = [];

  const replayer = new TemporalReplayer(
    scheduler,
    (pos) => frames.push({ t: scheduler.now(), pos }),
    DEFAULT_POLICY,
    (e) => logs.push(e),
  );

  for (const ev of events) {
    scheduler.advanceTo(ev.atMs);
    // Mesmo pipeline do browser: causalidade primeiro, apresentação depois.
    const seq = BigInt(ev.seq);
    if (!gate.offer(seq)) {
      rejected.push(ev.seq);
      continue;
    }
    replayer.setTarget(ev.pos, seq);
  }
  scheduler.advanceTo(events[events.length - 1].atMs + settleMs);

  return { scheduler, logs, frames, rejected, replayer };
}

// ───────────────────── Cenário A — rajadas e interrupções ─────────────────────

console.log("\n══ Cenário A — rajadas e interrupções ══");
console.log("  seq=4 @0 | seq=5 +100ms | seq=6 +2000ms | seq=7 +300ms | seq=8 +0ms\n");

const scenarioA: ScriptedEvent[] = [
  { atMs: 0, seq: 4, pos: P(0) },
  { atMs: 100, seq: 5, pos: P(1) },
  { atMs: 2100, seq: 6, pos: P(2) },
  { atMs: 2400, seq: 7, pos: P(3) },
  { atMs: 2400, seq: 8, pos: P(4) },
];

const a = run(scenarioA);
printLog(a.logs);

check(a.logs[0].mode === "teleport", "seq=4 (snapshot) posiciona imediatamente em P0");
check(a.logs[1].mode === "interpolate" && !a.logs[1].animation_cancelled,
  "seq=5 inicia interpolação P0 → P1 sem cancelar nada");

// Para cada retarget, a posição de partida deve ser a posição visual analítica
// daquele instante — recalculada aqui de forma independente.
let startsOk = true;
let notFromPrevTarget = true;
for (let i = 2; i < a.logs.length; i++) {
  const prev = a.logs[i - 1];
  const cur = a.logs[i];
  const p = Math.min(
    Math.max((cur.event_received_at - prev.event_received_at) / prev.calculated_duration_ms, 0),
    1,
  );
  const from = prev.visual_position_at_start!;
  const expected = {
    lat: from.lat + (prev.destination.lat - from.lat) * p,
    lng: from.lng + (prev.destination.lng - from.lng) * p,
  };
  if (haversineMeters(expected, cur.visual_position_at_start!) > 1e-6) startsOk = false;
  if (cur.animation_cancelled &&
      haversineMeters(prev.destination, cur.visual_position_at_start!) < 1e-6) {
    notFromPrevTarget = false;
  }
}
check(startsOk, "cada animação interrompida parte da posição visual do instante da chegada");
check(notFromPrevTarget, "nenhuma animação interrompida parte do destino anterior (sem 'pulo' para P_n)");
check(a.logs.slice(2).every((l) => l.animation_cancelled),
  "seq=6, 7 e 8 chegaram com animação ativa e a cancelaram");
check(a.logs[3].visual_position_at_start!.lat === a.logs[4].visual_position_at_start!.lat,
  "rajada (+0ms): seq=8 parte exatamente do mesmo ponto que seq=7");
check(a.scheduler.maxConcurrentLoops <= 1,
  `nunca há dois loops ativos (máx. simultâneo = ${a.scheduler.maxConcurrentLoops})`);

const seqs = a.logs.map((l) => Number(l.sequence));
check(seqs.every((s, i) => i === 0 || s > seqs[i - 1]), "destinos aplicados em ordem causal estrita");

let monotonic = true;
for (let i = 1; i < a.frames.length; i++) {
  if (a.frames[i].pos.lat < a.frames[i - 1].pos.lat - 1e-12) monotonic = false;
}
check(monotonic, `marcador nunca retrocede (${a.frames.length} frames verificados)`);

const lastA = a.frames[a.frames.length - 1].pos;
check(haversineMeters(lastA, P(4)) < 1e-6, "após estabilizar, marcador repousa em P4 (destino de seq=8)");
check(!a.replayer.isAnimating, "nenhum loop residual após a estabilização");

// ───────────────────── Cenário B — fora de ordem ─────────────────────

console.log("\n══ Cenário B — eventos fora de ordem ══");
console.log("  seq=4 → P0 @0 | seq=6 → P2 @500ms | seq=5 → P1 @600ms\n");

const scenarioB: ScriptedEvent[] = [
  { atMs: 0, seq: 4, pos: P(0) },
  { atMs: 500, seq: 6, pos: P(2) },
  { atMs: 600, seq: 5, pos: P(1) },
];

const b = run(scenarioB);
printLog(b.logs);

check(b.rejected.length === 1 && b.rejected[0] === 5, "CausalGate rejeita seq=5 (5 ≤ 6)");
check(b.logs.map((l) => l.sequence).join(",") === "4,6",
  "replayer recebeu apenas seq=4 e seq=6 (gap 4 → 6 aceito pelo contrato atual)");
check(!b.logs.some((l) => haversineMeters(l.destination, P(1)) < 1e-6),
  "nenhuma animação foi iniciada em direção a P1");
check(b.logs.filter((l) => l.animation_cancelled).length === 0,
  "a animação P0 → P2 não foi interrompida pelo evento atrasado");

let monotonicB = true;
for (let i = 1; i < b.frames.length; i++) {
  if (b.frames[i].pos.lat < b.frames[i - 1].pos.lat - 1e-12) monotonicB = false;
}
check(monotonicB, "marcador nunca retrocede");
check(haversineMeters(b.frames[b.frames.length - 1].pos, P(2)) < 1e-6, "marcador repousa em P2");

// ───────────────────────── Resultado ─────────────────────────

console.log(`\n${failures === 0 ? "✔ TODAS AS PROPRIEDADES VALIDADAS" : `✘ ${failures} FALHA(S)`}\n`);
process.exit(failures === 0 ? 0 : 1);
