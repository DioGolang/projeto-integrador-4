"use client";

import { useEffect, useState, useRef, useMemo } from "react";
import Map, { Marker, NavigationControl, Source, Layer } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import { setWorkerUrl } from "maplibre-gl";
import { Navigation, Store, MapPin } from "lucide-react";
import type { Feature, LineString } from "geojson";

import { useDeliveryStream } from "../../hooks/use-delivery-stream";
import {
  TemporalReplayer,
  DEFAULT_POLICY,
  browserScheduler,
  type LatLng,
} from "../../lib/temporal-replayer";

if (typeof window !== "undefined") {
  setWorkerUrl("/maplibre/maplibre-gl-worker.mjs");
}

interface DelivererMapProps {
  orderId: string | null;
}

export function DelivererMap({ orderId }: DelivererMapProps) {
  const streamState = useDeliveryStream(orderId || "");

  const [viewState, setViewState] = useState({
    longitude: -46.6333,
    latitude: -23.5505,
    zoom: 14,
  });

  // Visual position: output of the TemporalReplayer, never the causal state itself.
  const [markerPos, setMarkerPos] = useState<LatLng | null>(null);
  const replayerRef = useRef<TemporalReplayer | null>(null);

  useEffect(() => {
    const lat = localStorage.getItem("merchantLat");
    const lng = localStorage.getItem("merchantLng");
    if (lat && lng) {
      setViewState((prev) => ({
        ...prev,
        latitude: parseFloat(lat),
        longitude: parseFloat(lng),
      }));
    }
  }, []);

  // One replayer per order: a new order must start with a teleport,
  // never animate from the previous order's last visual position.
  useEffect(() => {
    const replayer = new TemporalReplayer(
      browserScheduler,
      setMarkerPos,
      DEFAULT_POLICY,
      process.env.NODE_ENV !== "production"
        ? (entry) => console.debug("[TemporalReplayer]", entry)
        : undefined,
    );
    if (streamState.route && streamState.route.points.length >= 2) {
      replayer.setRoute(streamState.route.points, {
        routeId: streamState.route.routeId,
        revision: streamState.route.revision,
        leg: streamState.route.leg,
        distanceMeters: streamState.route.distanceMeters,
        estimatedDurationMs: streamState.route.estimatedDurationMs,
      });
    }
    replayerRef.current = replayer;
    setMarkerPos(null);

    return () => {
      replayer.dispose();
      replayerRef.current = null;
    };
  }, [orderId]);

  // Synchronize route geometry with the replayer whenever route changes or is assigned
  useEffect(() => {
    const replayer = replayerRef.current;
    if (!replayer) return;

    if (streamState.route && streamState.route.points.length >= 2) {
      replayer.setRoute(streamState.route.points, {
        routeId: streamState.route.routeId,
        revision: streamState.route.revision,
        leg: streamState.route.leg,
        distanceMeters: streamState.route.distanceMeters,
        estimatedDurationMs: streamState.route.estimatedDurationMs,
      });
      if (!replayer.hasPosition) {
        replayer.setTarget(streamState.route.points[0], streamState.sequence);
      }
    } else if (!streamState.route) {
      replayer.setRoute(null);
    }
  }, [streamState.route]);

  // Feed causally-accepted targets to the replayer. Sequence validation already
  // happened in useDeliveryStream; this component never sees a regression.
  useEffect(() => {
    const replayer = replayerRef.current;
    const dest = streamState.currentLocation;
    if (!replayer || !dest) return;

    const isFirst = !replayer.hasPosition;
    replayer.setTarget(dest, streamState.sequence);

    if (isFirst) {
      setViewState((prev) => ({ ...prev, latitude: dest.lat, longitude: dest.lng }));
    }
  }, [streamState.sequence, streamState.currentLocation]);

  // GeoJSON representation of the active route polyline for MapLibre rendering
  const routeGeoJSON = useMemo<Feature<LineString> | null>(() => {
    if (!streamState.route || streamState.route.points.length < 2) {
      return null;
    }
    return {
      type: "Feature",
      properties: {
        routeId: streamState.route.routeId,
        revision: String(streamState.route.revision),
        leg: streamState.route.leg,
      },
      geometry: {
        type: "LineString",
        // MapLibre expects [longitude, latitude] coordinates
        coordinates: streamState.route.points.map((p) => [p.lng, p.lat]),
      },
    };
  }, [streamState.route]);

  // Destination of the current leg (Store for TO_PICKUP, Customer for TO_DROPOFF)
  const legDestination = useMemo(() => {
    if (!streamState.route || streamState.route.points.length < 2) return null;
    return streamState.route.points[streamState.route.points.length - 1];
  }, [streamState.route]);

  const [webglSupported, setWebglSupported] = useState<boolean>(false);

  useEffect(() => {
    try {
      if (
        typeof window !== "undefined" &&
        typeof navigator !== "undefined" &&
        !navigator.webdriver
      ) {
        const canvas = document.createElement("canvas");
        const gl =
          canvas.getContext("webgl") ||
          canvas.getContext("experimental-webgl");
        if (gl) {
          setWebglSupported(true);
        }
      }
    } catch {
      setWebglSupported(false);
    }
  }, []);

  const bounds = useMemo(() => {
    if (!streamState.route || streamState.route.points.length < 2) return null;
    let minLat = Infinity,
      maxLat = -Infinity,
      minLng = Infinity,
      maxLng = -Infinity;
    for (const p of streamState.route.points) {
      if (p.lat < minLat) minLat = p.lat;
      if (p.lat > maxLat) maxLat = p.lat;
      if (p.lng < minLng) minLng = p.lng;
      if (p.lng > maxLng) maxLng = p.lng;
    }
    const padLat = Math.max((maxLat - minLat) * 0.1, 0.001);
    const padLng = Math.max((maxLng - minLng) * 0.1, 0.001);
    return {
      minLat: minLat - padLat,
      maxLat: maxLat + padLat,
      minLng: minLng - padLng,
      maxLng: maxLng + padLng,
    };
  }, [streamState.route]);

  const projectLng = (lng: number) => {
    if (!bounds || bounds.maxLng === bounds.minLng) return 50;
    return ((lng - bounds.minLng) / (bounds.maxLng - bounds.minLng)) * 100;
  };

  const projectLat = (lat: number) => {
    if (!bounds || bounds.maxLat === bounds.minLat) return 50;
    return 100 - ((lat - bounds.minLat) / (bounds.maxLat - bounds.minLat)) * 100;
  };

  const svgPoints = useMemo(() => {
    if (!bounds || !streamState.route || streamState.route.points.length < 2) return "";
    return streamState.route.points
      .map((p) => `${projectLng(p.lng)},${projectLat(p.lat)}`)
      .join(" ");
  }, [bounds, streamState.route]);

  const routeMetrics = useMemo(() => {
    if (!streamState.route || streamState.route.points.length < 2) return null;
    const distKm = (streamState.route.distanceMeters / 1000).toFixed(1);
    const durationMs = Number(streamState.route.estimatedDurationMs);
    const durationMin = Math.round(durationMs / 60000);
    const durationText = durationMin > 0 ? `~${durationMin} min` : "< 1 min";
    const durationSec = durationMs / 1000;
    const speedKmh = durationSec > 0
      ? Math.round((streamState.route.distanceMeters / durationSec) * 3.6)
      : 36;
    return { distKm, durationText, speedKmh };
  }, [streamState.route]);

  return (
    <div className="h-[600px] w-full rounded-lg overflow-hidden border shadow-sm relative">
      {streamState.status === "DELIVERED" ? (
        <div className="absolute top-2 left-1/2 transform -translate-x-1/2 z-10 bg-emerald-950/90 border border-emerald-600 px-4 py-1 rounded-full shadow text-sm font-bold text-emerald-300 flex items-center gap-2 backdrop-blur-sm">
          <span className="w-2 h-2 rounded-full bg-emerald-400" />
          <span>ENTREGA CONCLUÍDA</span>
        </div>
      ) : streamState.connected ? (
        <div className="absolute top-2 left-1/2 transform -translate-x-1/2 z-10 bg-white/90 px-4 py-1 rounded-full shadow text-sm font-bold text-green-700 flex items-center gap-2">
          <span className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
          <span>LIVE</span>
        </div>
      ) : null}

      {streamState.route && (
        <div id="route-badge" className="absolute top-2 right-2 z-10 bg-white/95 px-3 py-2 rounded-lg shadow-md border text-xs font-medium flex flex-col gap-1 backdrop-blur-sm">
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-blue-600 animate-pulse" />
            <span id="route-leg-text" className="font-semibold text-gray-800">
              {streamState.route.leg === "TO_PICKUP"
                ? "Trecho 1: A caminho do restaurante"
                : "Trecho 2: A caminho do cliente"}
            </span>
            <span id="route-revision-text" className="text-gray-400 text-[10px]">
              (v{streamState.route.revision.toString()})
            </span>
          </div>
          {routeMetrics && (
            <div className="flex items-center gap-2 text-[11px] text-gray-600 pl-4">
              <span id="route-distance-text">{routeMetrics.distKm} km</span>
              <span>•</span>
              <span id="route-duration-text" className="text-blue-700 font-semibold">{routeMetrics.durationText}</span>
              <span>•</span>
              <span id="route-speed-text" className="text-gray-500">{routeMetrics.speedKmh} km/h méd.</span>
            </div>
          )}
        </div>
      )}
      
      {webglSupported ? (
        <Map
          {...viewState}
          onMove={(evt) => setViewState(evt.viewState)}
          mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
        >
          <NavigationControl position="top-left" />
          
          {/* Active Route Polyline Layer */}
          {routeGeoJSON && (
            <Source id="active-route" type="geojson" data={routeGeoJSON}>
              {/* Blue casing / glow for high visual contrast against street maps */}
              <Layer
                id="route-line-casing"
                type="line"
                layout={{
                  "line-join": "round",
                  "line-cap": "round",
                }}
                paint={{
                  "line-color": "#1e40af",
                  "line-width": 7,
                  "line-opacity": 0.45,
                }}
              />
              {/* Primary route line */}
              <Layer
                id="route-line-main"
                type="line"
                layout={{
                  "line-join": "round",
                  "line-cap": "round",
                }}
                paint={{
                  "line-color": "#3b82f6",
                  "line-width": 4,
                  "line-opacity": 0.9,
                }}
              />
            </Source>
          )}

          {/* Leg Destination Marker */}
          {legDestination && (
            <Marker
              longitude={legDestination.lng}
              latitude={legDestination.lat}
              anchor="bottom"
            >
              <div
                className={`p-1.5 rounded-full shadow-md border-2 border-white flex items-center justify-center text-white ${
                  streamState.route?.leg === "TO_PICKUP" ? "bg-amber-600" : "bg-emerald-600"
                }`}
                title={
                  streamState.route?.leg === "TO_PICKUP"
                    ? "Ponto de Coleta (Restaurante)"
                    : "Destino da Entrega (Cliente)"
                }
              >
                {streamState.route?.leg === "TO_PICKUP" ? (
                  <Store className="w-4 h-4" />
                ) : (
                  <MapPin className="w-4 h-4" />
                )}
              </div>
            </Marker>
          )}

          {/* Deliverer Position Marker (Driven strictly by TemporalReplayer visual position) */}
          {markerPos && (
            <Marker 
              longitude={markerPos.lng} 
              latitude={markerPos.lat} 
              anchor="bottom"
            >
              <div
                id="deliverer-marker"
                data-testid="deliverer-marker"
                data-lat={markerPos.lat}
                data-lng={markerPos.lng}
                className="bg-blue-600 p-2 rounded-full shadow-lg border-2 border-white"
              >
                <Navigation className="text-white w-6 h-6" />
              </div>
            </Marker>
          )}
        </Map>
      ) : (
        <div className="w-full h-full bg-slate-900 flex flex-col justify-between p-4 relative">
          <div className="text-white text-xs opacity-75 font-mono z-10">
            [Ambiente Headless] Renderizador de Geometria Ativo
          </div>

          {/* SVG Route Polyline */}
          {streamState.route && streamState.route.points.length >= 2 && (
            <svg
              className="w-full h-full absolute inset-0 pointer-events-none p-8"
              viewBox="0 0 100 100"
              preserveAspectRatio="none"
            >
              <polyline
                points={svgPoints}
                fill="none"
                stroke="#60a5fa"
                strokeWidth="3"
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeDasharray="1 1"
              />
              <polyline
                points={svgPoints}
                fill="none"
                stroke="#2563eb"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          )}

          {/* Leg Destination Marker in Fallback */}
          {legDestination && (
            <div
              className="absolute transform -translate-x-1/2 -translate-y-1/2 z-10"
              style={{
                left: `${projectLng(legDestination.lng)}%`,
                top: `${projectLat(legDestination.lat)}%`,
              }}
            >
              <div
                className={`p-1.5 rounded-full shadow-md border-2 border-white flex items-center justify-center text-white ${
                  streamState.route?.leg === "TO_PICKUP" ? "bg-amber-600" : "bg-emerald-600"
                }`}
              >
                {streamState.route?.leg === "TO_PICKUP" ? (
                  <Store className="w-4 h-4" />
                ) : (
                  <MapPin className="w-4 h-4" />
                )}
              </div>
            </div>
          )}

          {/* Deliverer Position Marker in Fallback */}
          {markerPos && (
            <div
              id="deliverer-marker"
              data-testid="deliverer-marker"
              data-lat={markerPos.lat}
              data-lng={markerPos.lng}
              className="absolute transform -translate-x-1/2 -translate-y-1/2 bg-blue-600 p-2 rounded-full shadow-lg border-2 border-white z-20"
              style={{
                left: `${projectLng(markerPos.lng)}%`,
                top: `${projectLat(markerPos.lat)}%`,
              }}
            >
              <Navigation className="text-white w-6 h-6" />
            </div>
          )}
        </div>
      )}
    </div>
  );
}
