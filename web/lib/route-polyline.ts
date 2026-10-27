import { LatLng, haversineMeters } from "./temporal-replayer";

export class RoutePolyline {
  readonly points: LatLng[];
  readonly cumDistances: number[]; // cumulative distance in meters at each vertex
  readonly totalLength: number; // in meters

  constructor(points: LatLng[]) {
    if (points.length < 2) {
      throw new Error("RoutePolyline requires at least 2 points");
    }
    this.points = points.map((p) => ({ ...p }));
    this.cumDistances = [0];

    for (let i = 1; i < this.points.length; i++) {
      const segDist = haversineMeters(this.points[i - 1], this.points[i]);
      this.cumDistances.push(this.cumDistances[i - 1] + segDist);
    }
    this.totalLength = this.cumDistances[this.cumDistances.length - 1];
  }

  /**
   * Returns the exact geographic point along the polyline at distance `s` (in meters).
   */
  pointAt(s: number): LatLng {
    const clampedS = Math.min(Math.max(s, 0), this.totalLength);

    for (let i = 1; i < this.points.length; i++) {
      if (clampedS <= this.cumDistances[i]) {
        const segLen = this.cumDistances[i] - this.cumDistances[i - 1];
        const t = segLen <= 0 ? 0 : (clampedS - this.cumDistances[i - 1]) / segLen;
        const a = this.points[i - 1];
        const b = this.points[i];
        return {
          lat: a.lat + (b.lat - a.lat) * t,
          lng: a.lng + (b.lng - a.lng) * t,
        };
      }
    }
    return { ...this.points[this.points.length - 1] };
  }

  /**
   * Projects a LatLng point onto the closest point along the polyline.
   * Returns the progress `s` (meters from start), the cross-track distance (meters),
   * and the projected LatLng.
   */
  project(p: LatLng): { s: number; distanceM: number; point: LatLng } {
    let best = {
      s: 0,
      distanceM: Infinity,
      point: { ...this.points[0] },
    };

    for (let i = 1; i < this.points.length; i++) {
      const a = this.points[i - 1];
      const b = this.points[i];

      const midLatRad = ((a.lat + b.lat) / 2) * (Math.PI / 180);
      const cosLat = Math.cos(midLatRad);
      const mPerDegLat = 111320;
      const mPerDegLng = 111320 * cosLat;

      const dx = (b.lng - a.lng) * mPerDegLng;
      const dy = (b.lat - a.lat) * mPerDegLat;
      const px = (p.lng - a.lng) * mPerDegLng;
      const py = (p.lat - a.lat) * mPerDegLat;

      const segLenSq = dx * dx + dy * dy;
      let t = 0;
      if (segLenSq > 0) {
        t = Math.min(Math.max((px * dx + py * dy) / segLenSq, 0), 1);
      }

      const projLat = a.lat + (b.lat - a.lat) * t;
      const projLng = a.lng + (b.lng - a.lng) * t;
      const projPoint: LatLng = { lat: projLat, lng: projLng };

      const dist = haversineMeters(p, projPoint);
      if (dist < best.distanceM) {
        const segDist = this.cumDistances[i] - this.cumDistances[i - 1];
        best = {
          s: this.cumDistances[i - 1] + t * segDist,
          distanceM: dist,
          point: projPoint,
        };
      }
    }

    return best;
  }
}
