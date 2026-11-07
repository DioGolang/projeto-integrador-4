import { createPromiseClient, type Interceptor } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { LogisticsService } from "../gen/logistics/v1/api_connect";

async function getValidToken(): Promise<string | null> {
  if (typeof window === "undefined") return null;

  let token = localStorage.getItem("merchantToken") || localStorage.getItem("driverToken");

  const isInvalid =
    !token ||
    token === "undefined" ||
    token === "null" ||
    token.startsWith("demo-token-") ||
    token === "offline-demo-token" ||
    token.startsWith("token-");

  if (isInvalid) {
    try {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080";
      const res = await fetch(`${apiUrl}/logistics.v1.AuthService/Login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: "loja@elo.com", password: "Merchant@123!" }),
      });
      if (res.ok) {
        const data = await res.json();
        if (data.token) {
          token = data.token;
          localStorage.setItem("merchantToken", data.token);
          localStorage.setItem("merchantId", data.merchantId || "d81bc2a9-63a4-4f3e-8c0c-2a3f4e783d0c");
          localStorage.setItem("merchantLat", (data.merchantLat || -23.5505).toString());
          localStorage.setItem("merchantLng", (data.merchantLng || -46.6333).toString());
        }
      }
    } catch (e) {
      console.warn("[AuthInterceptor] Auto-login failed:", e);
    }
  }

  return token || null;
}

const authInterceptor: Interceptor = (next) => async (req) => {
  const token = await getValidToken();
  if (token) {
    req.header.set("Authorization", `Bearer ${token}`);
  }

  try {
    return await next(req);
  } catch (err: any) {
    // Se o token estiver expirado ou inválido, renova e tenta mais uma vez
    if (
      err?.code === 16 /* CodeUnauthenticated */ ||
      err?.message?.includes("unauthenticated") ||
      err?.message?.includes("token")
    ) {
      if (typeof window !== "undefined") {
        localStorage.removeItem("merchantToken");
        const freshToken = await getValidToken();
        if (freshToken) {
          req.header.set("Authorization", `Bearer ${freshToken}`);
          return await next(req);
        }
      }
    }
    throw err;
  }
};

const transport = createConnectTransport({
  baseUrl: process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080",
  interceptors: [authInterceptor],
});

export const logisticsClient = createPromiseClient(LogisticsService, transport);

