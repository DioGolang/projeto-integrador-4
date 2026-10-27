import { useState, useEffect, useRef } from "react";
import { logisticsClient } from "../api/logistics-client";
import { createCausalGate, type CausalGate } from "../lib/causal-gate";
import { isAbortOrCanceled } from "../components/common/GlobalErrorHandler";

export type RouteGeometryState = {
  routeId: string;
  revision: bigint;
  leg: string;
  points: { lat: number; lng: number }[];
  distanceMeters: number;
  estimatedDurationMs: bigint;
};

export type DeliveryStreamState = {
  orderId: string;
  status: string;
  sequence: bigint | null;
  currentLocation?: {
    lat: number;
    lng: number;
  };
  delivererId?: string;
  connected: boolean;
  error?: Error;
  route?: RouteGeometryState;
};

export function useDeliveryStream(
  orderId: string,
  // Optional client override for testing
  client: typeof logisticsClient = logisticsClient
): DeliveryStreamState {
  const [state, setState] = useState<DeliveryStreamState>({
    orderId,
    status: "CONNECTING",
    sequence: null,
    connected: false,
  });

  const gate = useRef<CausalGate>(createCausalGate());

  useEffect(() => {
    const abortController = new AbortController();
    let active = true;

    gate.current.reset();

    async function connectStream() {
      // Full reset: nothing from the previous order may leak into this one.
      setState({
        orderId,
        status: "CONNECTING",
        sequence: null,
        connected: false,
        route: undefined,
      });

      try {
        const stream = client.streamOrderStatus(
          { orderId },
          { signal: abortController.signal }
        );

        if (!active || abortController.signal.aborted) return;

        setState((prev) => ({ ...prev, connected: true }));

        for await (const res of stream) {
          if (!active || abortController.signal.aborted) break;

          const seq = res.sequence;

          if (!gate.current.offer(seq)) {
            console.warn(
              `[DeliveryStream] Ignoring outdated sequence: received ${seq}, current ${gate.current.current()}`
            );
            continue;
          }

          let newRoute: RouteGeometryState | undefined = undefined;
          if (res.route && res.route.points && res.route.points.length >= 2) {
            newRoute = {
              routeId: res.route.routeId,
              revision: res.route.revision,
              leg: res.route.leg,
              points: res.route.points.map((p) => ({ lat: p.lat, lng: p.lng })),
              distanceMeters: res.route.distanceMeters,
              estimatedDurationMs: res.route.estimatedDurationMs,
            };
          }

          setState((prev) => ({
            ...prev,
            orderId: res.orderId || prev.orderId,
            status: res.status,
            sequence: seq,
            delivererId: res.delivererId || prev.delivererId,
            currentLocation: res.currentLocation
              ? { lat: res.currentLocation.lat, lng: res.currentLocation.lng }
              : prev.currentLocation,
            route: newRoute ?? prev.route,
          }));

          // Se a entrega foi finalizada (DELIVERED), encerra o streaming de forma limpa
          if (res.status === "DELIVERED") {
            break;
          }
        }
      } catch (err) {
        if (
          !active ||
          abortController.signal.aborted ||
          isAbortOrCanceled(err, abortController.signal)
        ) {
          // Benign abort/cancellation during unmount, order change, or stream termination
          return;
        }

        setState((prev) => ({
          ...prev,
          connected: false,
          error: err instanceof Error ? err : new Error(String(err)),
        }));
      }
    }

    if (orderId) {
      connectStream().catch((err) => {
        if (
          !active ||
          abortController.signal.aborted ||
          isAbortOrCanceled(err, abortController.signal)
        ) {
          return;
        }
        console.error("[DeliveryStream] Unexpected stream failure:", err);
      });
    }

    return () => {
      active = false;
      try {
        abortController.abort();
      } catch {
        // Silently swallow abort errors
      }
    };
  }, [orderId, client]);

  return state;
}
