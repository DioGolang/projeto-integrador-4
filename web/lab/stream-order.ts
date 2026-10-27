import { createPromiseClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { LogisticsService } from "../gen/logistics/v1/api_connect";

async function main() {
  const orderId = process.argv[2];
  if (!orderId) {
    console.error("Please provide an order ID");
    process.exit(1);
  }

  console.log(`Connecting... (Order: ${orderId})`);

  const transport = createConnectTransport({
    baseUrl: "http://localhost:8080",
    httpVersion: "1.1", // or "2" depending on the server, but Go default HTTP/1.1 is fine for connect
  });

  const client = createPromiseClient(LogisticsService, transport);

  try {
    const stream = client.streamOrderStatus({ orderId });

    let lastSequence = BigInt(0); // bigint because int64
    let isFirstMessage = true;

    for await (const msg of stream) {
      const seq = msg.sequence;
      const type = isFirstMessage ? "SNAPSHOT" : "LIVE";
      
      if (seq < lastSequence) {
        console.error(`FAIL: received sequence ${seq} which is < lastSequence ${lastSequence}`);
        process.exit(1);
      }

      console.log(`[${type}] seq=${seq} state=${msg.status}`);

      lastSequence = seq;
      isFirstMessage = false;
    }
  } catch (err) {
    console.error("Stream failed:", err);
  }
}

main();
