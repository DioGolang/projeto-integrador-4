"use client";

import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { CreateOrderForm } from "./CreateOrderForm";
import dynamic from "next/dynamic";
import { Package, RotateCw } from "lucide-react";

const DelivererMap = dynamic(() => import("./DelivererMap").then(mod => mod.DelivererMap), { ssr: false });

export function Dashboard() {
  const [activeOrderId, setActiveOrderId] = useState<string | null>(null);
  const [isAuth, setIsAuth] = useState(false);
  const router = useRouter();

  useEffect(() => {
    const token = localStorage.getItem("merchantToken");
    if (!token) {
      router.push("/login");
    } else {
      setIsAuth(true);
      if (typeof window !== "undefined") {
        const params = new URLSearchParams(window.location.search);
        const urlOrderId = params.get("orderId");
        if (urlOrderId) {
          setActiveOrderId(urlOrderId);
          localStorage.setItem("activeOrderId", urlOrderId);
        } else {
          const saved = localStorage.getItem("activeOrderId");
          if (saved) setActiveOrderId(saved);
        }
      }
    }
  }, [router]);

  const handleOrderCreated = (id: string) => {
    setActiveOrderId(id);
    localStorage.setItem("activeOrderId", id);
    if (typeof window !== "undefined") {
      const url = new URL(window.location.href);
      url.searchParams.set("orderId", id);
      window.history.replaceState({}, "", url.toString());
    }
  };

  const handleClearOrder = () => {
    setActiveOrderId(null);
    localStorage.removeItem("activeOrderId");
    if (typeof window !== "undefined") {
      const url = new URL(window.location.href);
      url.searchParams.delete("orderId");
      window.history.replaceState({}, "", url.toString());
    }
  };

  if (!isAuth) {
    return (
      <div className="flex items-center justify-center p-12 text-slate-400">
        <div className="flex items-center gap-2">
          <RotateCw className="w-5 h-5 animate-spin text-cyan-400" />
          <span>Autenticando sessão...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="w-full max-w-7xl flex flex-col gap-4">
      {activeOrderId && (
        <div className="bg-slate-900 border border-cyan-800/60 rounded-xl p-3 px-4 flex flex-wrap items-center justify-between gap-3 shadow-md">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-cyan-950 text-cyan-400 rounded-lg border border-cyan-800">
              <Package className="w-5 h-5" />
            </div>
            <div>
              <div className="text-xs text-slate-400 font-medium">Pedido em Rastreamento Ativo</div>
              <div className="text-sm font-mono font-bold text-white tracking-wide">{activeOrderId}</div>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-emerald-950 text-emerald-300 border border-emerald-800">
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
              Stream Conectado
            </span>
            <button
              onClick={handleClearOrder}
              className="text-xs text-slate-400 hover:text-white px-2.5 py-1 rounded border border-slate-700 hover:border-slate-600 transition-colors"
            >
              Trocar Pedido
            </button>
          </div>
        </div>
      )}

      <div className="w-full grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Painel Esquerdo: Formulário */}
        <div className="lg:col-span-1">
          <CreateOrderForm onOrderCreated={handleOrderCreated} />
        </div>

        {/* Painel Direito: Mapa */}
        <div className="lg:col-span-2">
          <DelivererMap orderId={activeOrderId} />
        </div>
      </div>
    </div>
  );
}
