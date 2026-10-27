/**
 * TemporalReplayer — visual projection of a causal sequence of positions.
 *
 * It knows nothing about transport or sequence validation: it only receives
 * targets that were already accepted causally (see CausalGate) and decides how
 * the marker travels between them.
 *
 * Concepts kept separate:
 *   - causal position  → last target received (`setTarget`)
 *   - visual position  → where the marker is at a given instant (`sample(now)`)
 *   - visual target    → destination of the active animation
 *
 * Route-aware policy (v2):
 *   When a RoutePolyline is provided, the replayer moves along the road geometry:
 *   - Visual progress is tracked as arc-length distance `s` (in meters) along the route.
 *   - Targets are projected orthogonally onto the route polyline.
 *   - Retargeting mid-animation starts from the current progress `s(now)`.
 *   - Lateral deviation (cross-track error) along the route is 0.0m by definition.
 *   - Without a route, it falls back to direct linear interpolation (v1).
 */

import { RoutePolyline } from "./route-polyline";

export type LatLng = { lat: number; lng: number };

export type PlaybackPolicy = {
  visualSpeedMps: number;
  minDurationMs: number;
  maxDurationMs: number;
};

export const DEFAULT_POLICY: PlaybackPolicy = {
  visualSpeedMps: 10, // ~36 km/h (velocidade média da moto em vias urbanas)
  minDurationMs: 500,
  maxDurationMs: 3000,
};

export type Scheduler = {
  now(): number;
  requestFrame(cb: (t: number) => void): number;
  cancelFrame(id: number): void;
};

export type ReplayerLogEntry = {
  sequence: string;
  mode: "teleport" | "interpolate" | "skip";
  event_received_at: number;
  visual_position_at_start: LatLng | null;
  destination: LatLng;
  distance_m: number;
  raw_duration_ms: number;
  calculated_duration_ms: number;
  clamped: boolean;
  animation_cancelled: boolean;
  route_progress_s?: number;
};

type RouteAnimation = {
  mode: "route";
  fromS: number;
  toS: number;
  startTime: number;
  durationMs: number;
};

type DirectAnimation = {
  mode: "direct";
  from: LatLng;
  to: LatLng;
  startTime: number;
  durationMs: number;
};

type Animation = RouteAnimation | DirectAnimation;

export function haversineMeters(a: LatLng, b: LatLng): number {
  const R = 6371e3;
  const phi1 = (a.lat * Math.PI) / 180;
  const phi2 = (b.lat * Math.PI) / 180;
  const dPhi = ((b.lat - a.lat) * Math.PI) / 180;
  const dLambda = ((b.lng - a.lng) * Math.PI) / 180;
  const h =
    Math.sin(dPhi / 2) ** 2 +
    Math.cos(phi1) * Math.cos(phi2) * Math.sin(dLambda / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
}

function samePoint(a: LatLng, b: LatLng): boolean {
  return a.lat === b.lat && a.lng === b.lng;
}

export type RouteMetadata = {
  routeId: string;
  revision: bigint;
  leg?: string;
  distanceMeters?: number;
  estimatedDurationMs?: bigint;
};

export class TemporalReplayer {
  private visual: LatLng | null = null;
  private visualProgressS: number | null = null;
  private anim: Animation | null = null;
  private frameId: number | null = null;
  private route: RoutePolyline | null = null;
  private metadata: RouteMetadata | null = null;
  private policy: PlaybackPolicy;

  constructor(
    private readonly scheduler: Scheduler,
    private readonly onPosition: (pos: LatLng) => void,
    policy: PlaybackPolicy = DEFAULT_POLICY,
    private readonly onLog?: (entry: ReplayerLogEntry) => void,
    route?: RoutePolyline | LatLng[] | null,
    metadata?: RouteMetadata,
  ) {
    this.policy = { ...policy };
    if (route) {
      this.setRoute(route, metadata);
    }
  }

  get hasPosition(): boolean {
    return this.visual !== null;
  }

  get isAnimating(): boolean {
    return this.frameId !== null;
  }

  get currentRoute(): RoutePolyline | null {
    return this.route;
  }

  get currentMetadata(): RouteMetadata | null {
    return this.metadata;
  }

  get currentProgressS(): number | null {
    return this.visualProgressS;
  }

  setSpeed(speedMps: number): void {
    if (speedMps > 0) {
      this.policy.visualSpeedMps = speedMps;
    }
  }

  /**
   * Sets or updates the active route geometry.
   * Returns true if route was updated, false if rejected or unchanged.
   */
  setRoute(route: RoutePolyline | LatLng[] | null, metadata?: RouteMetadata): boolean {
    if (!route) {
      this.route = null;
      this.visualProgressS = null;
      this.metadata = null;
      return true;
    }

    // Validation: must have at least 2 points
    const points = route instanceof RoutePolyline ? route.points : route;
    if (!Array.isArray(points) || points.length < 2) {
      return false; // Preserve existing route
    }

    // Repetition check: if same route_id and same revision, ignore duplicate
    if (
      metadata &&
      this.metadata &&
      metadata.routeId === this.metadata.routeId &&
      metadata.revision <= this.metadata.revision
    ) {
      return false;
    }

    const now = this.scheduler.now();
    const currentVisual = this.sample(now);

    // Cancel active animation frame
    if (this.frameId !== null) {
      this.scheduler.cancelFrame(this.frameId);
      this.frameId = null;
    }
    this.anim = null;

    this.route = route instanceof RoutePolyline ? route : new RoutePolyline(points);
    this.metadata = metadata ?? null;

    // Dynamically align motorcycle visual speed with route distance and estimated duration
    if (metadata?.estimatedDurationMs && metadata?.distanceMeters) {
      const durationSec = Number(metadata.estimatedDurationMs) / 1000;
      if (durationSec > 0) {
        const routeSpeedMps = metadata.distanceMeters / durationSec;
        this.policy.visualSpeedMps = Math.max(3, Math.min(25, routeSpeedMps));
      }
    }

    // Transition smoothly from previous visual position
    if (currentVisual) {
      const proj = this.route.project(currentVisual);
      this.visualProgressS = proj.s;
      this.visual = proj.point;
      this.onPosition(this.visual);
    }

    return true;
  }

  /** Visual position at instant `now`, derived analytically from the active animation. */
  sample(now: number): LatLng | null {
    if (!this.anim) return this.visual;

    const { startTime, durationMs } = this.anim;
    const p = Math.min(Math.max((now - startTime) / durationMs, 0), 1);

    if (this.anim.mode === "route" && this.route) {
      const s = this.anim.fromS + (this.anim.toS - this.anim.fromS) * p;
      return this.route.pointAt(s);
    }

    if (this.anim.mode === "direct") {
      const { from, to } = this.anim;
      return {
        lat: from.lat + (to.lat - from.lat) * p,
        lng: from.lng + (to.lng - from.lng) * p,
      };
    }

    return this.visual;
  }

  /**
   * Sample current route progress `s` (in meters) at instant `now`.
   */
  sampleProgress(now: number): number | null {
    if (!this.route) return null;
    if (!this.anim) return this.visualProgressS;

    if (this.anim.mode === "route") {
      const { startTime, durationMs, fromS, toS } = this.anim;
      const p = Math.min(Math.max((now - startTime) / durationMs, 0), 1);
      return fromS + (toS - fromS) * p;
    }

    return this.visualProgressS;
  }

  setTarget(dest: LatLng, sequence: bigint | null): void {
    const now = this.scheduler.now();
    const seq = sequence === null ? "null" : sequence.toString();

    // ────────────────────── MODE 1: ROUTE-AWARE REPLAY ──────────────────────
    if (this.route) {
      const proj = this.route.project(dest);

      // P0: nothing materialized visually yet → teleport to route projection.
      if (!this.visual || this.visualProgressS === null) {
        this.visualProgressS = proj.s;
        this.visual = proj.point;
        this.onPosition(this.visual);
        this.log({
          sequence: seq,
          mode: "teleport",
          event_received_at: now,
          visual_position_at_start: null,
          destination: proj.point,
          distance_m: 0,
          raw_duration_ms: 0,
          calculated_duration_ms: 0,
          clamped: false,
          animation_cancelled: false,
          route_progress_s: proj.s,
        });
        return;
      }

      // Sample progress at event arrival time
      const currentS = this.sampleProgress(now) ?? this.visualProgressS;
      // Target must not cause spatial regression along the route
      const targetS = Math.max(currentS, proj.s);
      const distance = targetS - currentS;

      // Already at target (or negligible progress): skip
      if (distance < 1e-4) {
        this.log({
          sequence: seq,
          mode: "skip",
          event_received_at: now,
          visual_position_at_start: this.sample(now),
          destination: proj.point,
          distance_m: 0,
          raw_duration_ms: 0,
          calculated_duration_ms: 0,
          clamped: false,
          animation_cancelled: false,
          route_progress_s: currentS,
        });
        return;
      }

      const cancelled = this.frameId !== null;
      if (this.frameId !== null) {
        this.scheduler.cancelFrame(this.frameId);
        this.frameId = null;
      }

      const raw = (distance / this.policy.visualSpeedMps) * 1000;
      const duration = Math.max(
        this.policy.minDurationMs,
        Math.min(this.policy.maxDurationMs, raw),
      );

      const fromPos = this.route.pointAt(currentS);
      this.visual = fromPos;
      this.visualProgressS = currentS;

      this.anim = {
        mode: "route",
        fromS: currentS,
        toS: targetS,
        startTime: now,
        durationMs: duration,
      };

      this.log({
        sequence: seq,
        mode: "interpolate",
        event_received_at: now,
        visual_position_at_start: fromPos,
        destination: proj.point,
        distance_m: distance,
        raw_duration_ms: raw,
        calculated_duration_ms: duration,
        clamped: duration !== raw,
        animation_cancelled: cancelled,
        route_progress_s: targetS,
      });

      this.frameId = this.scheduler.requestFrame(this.tick);
      return;
    }

    // ────────────────────── MODE 2: DIRECT FALLBACK (v1) ──────────────────────
    // P0: nothing materialized visually yet → teleport.
    if (!this.visual) {
      this.visual = { ...dest };
      this.onPosition(this.visual);
      this.log({
        sequence: seq,
        mode: "teleport",
        event_received_at: now,
        visual_position_at_start: null,
        destination: dest,
        distance_m: 0,
        raw_duration_ms: 0,
        calculated_duration_ms: 0,
        clamped: false,
        animation_cancelled: false,
      });
      return;
    }

    // Same destination: nothing to replay.
    const currentTarget =
      this.anim && this.anim.mode === "direct" ? this.anim.to : this.visual;
    if (samePoint(currentTarget, dest)) {
      this.log({
        sequence: seq,
        mode: "skip",
        event_received_at: now,
        visual_position_at_start: this.sample(now),
        destination: dest,
        distance_m: 0,
        raw_duration_ms: 0,
        calculated_duration_ms: 0,
        clamped: false,
        animation_cancelled: false,
      });
      return;
    }

    const from = this.sample(now)!;
    const cancelled = this.frameId !== null;
    if (this.frameId !== null) {
      this.scheduler.cancelFrame(this.frameId);
      this.frameId = null;
    }

    const distance = haversineMeters(from, dest);
    const raw = (distance / this.policy.visualSpeedMps) * 1000;
    const duration = Math.max(
      this.policy.minDurationMs,
      Math.min(this.policy.maxDurationMs, raw),
    );

    this.visual = from;
    this.anim = {
      mode: "direct",
      from,
      to: { ...dest },
      startTime: now,
      durationMs: duration,
    };

    this.log({
      sequence: seq,
      mode: "interpolate",
      event_received_at: now,
      visual_position_at_start: from,
      destination: dest,
      distance_m: distance,
      raw_duration_ms: raw,
      calculated_duration_ms: duration,
      clamped: duration !== raw,
      animation_cancelled: cancelled,
    });

    this.frameId = this.scheduler.requestFrame(this.tick);
  }

  dispose(): void {
    if (this.frameId !== null) this.scheduler.cancelFrame(this.frameId);
    this.frameId = null;
    this.anim = null;
    this.visual = null;
    this.visualProgressS = null;
    this.route = null;
    this.metadata = null;
  }

  private tick = (t: number): void => {
    this.frameId = null;
    if (!this.anim) return;

    const pos = this.sample(t)!;
    this.visual = pos;

    if (this.anim.mode === "route" && this.route) {
      const p = Math.min(Math.max((t - this.anim.startTime) / this.anim.durationMs, 0), 1);
      this.visualProgressS = this.anim.fromS + (this.anim.toS - this.anim.fromS) * p;
    }

    this.onPosition(pos);

    if (t - this.anim.startTime < this.anim.durationMs) {
      this.frameId = this.scheduler.requestFrame(this.tick);
    } else {
      if (this.anim.mode === "route" && this.route) {
        this.visualProgressS = this.anim.toS;
        this.visual = this.route.pointAt(this.anim.toS);
      } else if (this.anim.mode === "direct") {
        this.visual = { ...this.anim.to };
      }
      this.anim = null;
    }
  };

  private log(entry: ReplayerLogEntry): void {
    this.onLog?.(entry);
  }
}

export const browserScheduler: Scheduler = {
  now: () => (typeof performance !== "undefined" ? performance.now() : Date.now()),
  requestFrame: (cb) => {
    if (typeof requestAnimationFrame !== "undefined") {
      let called = false;
      const rafId = requestAnimationFrame((t) => {
        called = true;
        cb(t);
      });
      setTimeout(() => {
        if (!called) {
          cb(typeof performance !== "undefined" ? performance.now() : Date.now());
        }
      }, 32);
      return rafId;
    }
    return setTimeout(() => cb(Date.now()), 16) as unknown as number;
  },
  cancelFrame: (id) => {
    if (typeof cancelAnimationFrame !== "undefined") {
      cancelAnimationFrame(id);
    }
    clearTimeout(id);
  },
};
