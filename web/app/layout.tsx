import "./globals.css";
import type { Metadata } from "next";
import { GlobalErrorHandler } from "@/components/common/GlobalErrorHandler";

export const metadata: Metadata = {
  title: "VeloxLog • Motor Logístico JIT",
  description: "Despacho JIT, rotas viárias reais OSRM e streaming ConnectRPC",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="pt-BR">
      <head>
        <script
          dangerouslySetInnerHTML={{
            __html: `
              (function() {
                function isAbort(err) {
                  if (!err) return false;
                  if (typeof err === "object") {
                    if (err.name === "AbortError") return true;
                    if (err.code === 1 || err.code === "canceled") return true;
                    var msg = ((err.message || err.rawMessage || "") + "").toLowerCase();
                    if (
                      msg.includes("aborted") ||
                      msg.includes("[canceled]") ||
                      msg.includes("the operation was aborted") ||
                      msg.includes("this operation was aborted") ||
                      msg.includes("abort error") ||
                      msg.includes("user aborted a request") ||
                      msg.includes("request was cancelled")
                    ) {
                      return true;
                    }
                    if (err.cause && isAbort(err.cause)) return true;
                  }
                  if (typeof err === "string") {
                    var str = err.toLowerCase();
                    return str.includes("aborted") || str.includes("[canceled]") || str.includes("the operation was aborted");
                  }
                  return false;
                }

                window.addEventListener("unhandledrejection", function(e) {
                  if (isAbort(e.reason)) {
                    e.preventDefault();
                    if (typeof e.stopImmediatePropagation === "function") {
                      e.stopImmediatePropagation();
                    }
                  }
                }, true);

                window.addEventListener("error", function(e) {
                  if (isAbort(e.error) || isAbort(e.message)) {
                    e.preventDefault();
                    if (typeof e.stopImmediatePropagation === "function") {
                      e.stopImmediatePropagation();
                    }
                  }
                }, true);
              })();
            `,
          }}
        />
      </head>
      <body className="antialiased bg-slate-950 text-slate-100">
        <GlobalErrorHandler />
        {children}
      </body>
    </html>
  );
}
