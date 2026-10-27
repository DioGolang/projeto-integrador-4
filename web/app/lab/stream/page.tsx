"use client";

import { useState } from "react";
import { useDeliveryStream } from "@/hooks/use-delivery-stream";
import { logisticsClient } from "@/api/logistics-client";
import { StreamOrderStatusResponse } from "@/gen/logistics/v1/api_pb";

// A mock client to feed arbitrary sequences
function createMockClient(events: Partial<StreamOrderStatusResponse>[], delayMs = 500) {
  return {
    ...logisticsClient,
    async *streamOrderStatus(req: any, options: any) {
      for (const ev of events) {
        if (options?.signal?.aborted) break;
        await new Promise((resolve) => setTimeout(resolve, delayMs));
        yield {
          orderId: req.orderId,
          status: ev.status || "IN_TRANSIT",
          sequence: ev.sequence ?? BigInt(0),
          delivererId: ev.delivererId || "mock-driver",
          ...ev,
        } as StreamOrderStatusResponse;
      }
    },
  };
}

function StreamViewer({ orderId, client = logisticsClient }: { orderId: string, client?: any }) {
  const state = useDeliveryStream(orderId, client);

  return (
    <div className="mt-4 p-4 border rounded bg-gray-50">
      <h2 className="text-xl font-bold mb-4">Stream State</h2>
      
      <div className="grid grid-cols-2 gap-4">
        <div><strong>Order ID:</strong></div>
        <div className="font-mono text-sm">{state.orderId}</div>
        
        <div><strong>Connection:</strong></div>
        <div>
          {state.error ? (
            <span className="text-red-600 font-bold">Error: {state.error.message}</span>
          ) : state.connected ? (
            <span className="text-green-600 font-bold">Connected</span>
          ) : (
            <span className="text-yellow-600 font-bold">Connecting...</span>
          )}
        </div>

        <div><strong>Sequence:</strong></div>
        <div className="font-mono bg-blue-100 text-blue-800 font-bold px-2 py-1 inline-block rounded">
          {state.sequence === null ? "null" : state.sequence.toString()}
        </div>

        <div><strong>Status:</strong></div>
        <div>{state.status}</div>

        <div><strong>Deliverer ID:</strong></div>
        <div className="font-mono text-sm">{state.delivererId || "N/A"}</div>

        <div><strong>Location:</strong></div>
        <div className="font-mono text-sm">
          {state.currentLocation 
            ? `${state.currentLocation.lat.toFixed(5)}, ${state.currentLocation.lng.toFixed(5)}` 
            : "N/A"}
        </div>
      </div>
    </div>
  );
}

export default function LabStreamPage() {
  const [orderId, setOrderId] = useState("");
  const [activeOrderId, setActiveOrderId] = useState("");
  const [activeClient, setActiveClient] = useState<any>(logisticsClient);

  const runTest1 = () => {
    setActiveOrderId("test-1");
    setActiveClient(createMockClient([
      { sequence: BigInt(4) },
      { sequence: BigInt(5) },
      { sequence: BigInt(6) },
      { sequence: BigInt(7) },
    ]));
  };

  const runTest2 = () => {
    setActiveOrderId("test-2");
    setActiveClient(createMockClient([
      { sequence: BigInt(5) },
      { sequence: BigInt(7) },
      { sequence: BigInt(6) }, // Should be ignored
    ]));
  };

  const runTest3 = () => {
    setActiveOrderId("order-A");
    setActiveClient(createMockClient([
      { sequence: BigInt(7) }
    ]));
    
    // Switch to order B after a short delay to test reset logic
    setTimeout(() => {
      setActiveOrderId("order-B");
      setActiveClient(createMockClient([
        { sequence: BigInt(3) } // Should be accepted because orderId changed
      ]));
    }, 1500);
  };

  return (
    <div className="p-8 max-w-2xl mx-auto">
      <h1 className="text-2xl font-bold mb-6">6E.8.7.5 — React Hook Monotonicity Lab</h1>
      
      <div className="flex gap-2 mb-6">
        <input 
          type="text" 
          placeholder="Enter Order ID" 
          value={orderId}
          onChange={(e) => setOrderId(e.target.value)}
          className="flex-1 p-2 border rounded"
        />
        <button 
          onClick={() => { setActiveOrderId(orderId); setActiveClient(logisticsClient); }}
          className="bg-blue-600 text-white px-4 py-2 rounded font-bold"
        >
          Connect Live Stream
        </button>
      </div>

      <div className="mb-6 border-t pt-4">
        <h3 className="font-bold mb-2">Automated Tests:</h3>
        <div className="flex gap-2">
          <button onClick={runTest1} className="bg-gray-200 px-3 py-1 rounded text-sm font-semibold hover:bg-gray-300">
            Test 1: 4 → 5 → 6 → 7
          </button>
          <button onClick={runTest2} className="bg-gray-200 px-3 py-1 rounded text-sm font-semibold hover:bg-gray-300">
            Test 2: 5 → 7 → 6
          </button>
          <button onClick={runTest3} className="bg-gray-200 px-3 py-1 rounded text-sm font-semibold hover:bg-gray-300">
            Test 3: Order A (7) → Order B (3)
          </button>
        </div>
      </div>

      {activeOrderId && <StreamViewer orderId={activeOrderId} client={activeClient} />}
    </div>
  );
}
