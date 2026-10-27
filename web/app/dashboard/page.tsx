"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { Dashboard } from "@/components/domain/Dashboard";
import { 
  Zap, 
  Store, 
  LogOut, 
  Home, 
  MapPin, 
  ShieldCheck, 
  Activity, 
  ArrowLeft 
} from "lucide-react";

export default function DashboardPage() {
  const [merchantEmail, setMerchantEmail] = useState<string>("");
  const [merchantName, setMerchantName] = useState<string>("Loja Centro SP");
  const [isReady, setIsReady] = useState(false);
  const router = useRouter();

  useEffect(() => {
    if (typeof window !== "undefined") {
      const token = localStorage.getItem("merchantToken");
      const savedEmail = localStorage.getItem("merchantEmail") || "loja@elo.com";
      const savedName = localStorage.getItem("merchantStoreName") || "Loja Centro SP";
      
      setMerchantEmail(savedEmail);
      setMerchantName(savedName);

      if (!token) {
        // Se não tiver token, configura sessão de demonstração para acesso direto
        localStorage.setItem("merchantToken", "demo-token-" + Date.now());
        localStorage.setItem("merchantId", "d81bc2a9-63a4-4f3e-8c0c-2a3f4e783d0c");
        localStorage.setItem("merchantLat", "-23.5505");
        localStorage.setItem("merchantLng", "-46.6333");
      }
      setIsReady(true);
    }
  }, [router]);

  const handleLogout = () => {
    localStorage.removeItem("merchantToken");
    localStorage.removeItem("activeOrderId");
    router.push("/login");
  };

  if (!isReady) {
    return (
      <div className="min-h-screen bg-slate-900 flex items-center justify-center text-white">
        <div className="flex items-center gap-3">
          <div className="w-5 h-5 border-2 border-cyan-400 border-t-transparent rounded-full animate-spin" />
          <span>Carregando centro de operações...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col">
      {/* Top Bar */}
      <header className="border-b border-slate-800 bg-slate-900/90 backdrop-blur-md sticky top-0 z-30 px-4 sm:px-6 py-3 shadow-lg">
        <div className="max-w-7xl mx-auto flex flex-wrap items-center justify-between gap-4">
          {/* Brand */}
          <div className="flex items-center gap-3">
            <Link 
              href="/" 
              className="flex items-center gap-2 group text-white hover:text-cyan-400 transition-colors"
            >
              <div className="w-9 h-9 rounded-xl bg-gradient-to-tr from-cyan-600 to-blue-600 flex items-center justify-center shadow-lg shadow-cyan-500/20">
                <Zap className="w-5 h-5 text-white" />
              </div>
              <span className="font-extrabold text-lg tracking-tight">VELOX<span className="text-cyan-400">LOG</span></span>
            </Link>
            <span className="hidden sm:inline-block px-2.5 py-0.5 rounded-full text-[11px] font-semibold bg-cyan-950 text-cyan-300 border border-cyan-800">
              Despacho JIT v2
            </span>
          </div>

          {/* Quick Metrics & Store info */}
          <div className="flex items-center gap-4 text-xs">
            <div className="hidden md:flex items-center gap-2 px-3 py-1.5 rounded-lg bg-slate-800/80 border border-slate-700/60">
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
              </span>
              <span className="text-slate-300 font-medium">LiveHub ConnectRPC Online</span>
            </div>

            <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-slate-800/80 border border-slate-700/60">
              <Store className="w-3.5 h-3.5 text-cyan-400" />
              <span className="font-semibold text-slate-200">{merchantName}</span>
              <span className="text-slate-500 hidden sm:inline">({merchantEmail})</span>
            </div>

            {/* Navigation links */}
            <Link
              href="/"
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white transition-colors border border-slate-700/60"
            >
              <Home className="w-3.5 h-3.5" />
              <span className="hidden sm:inline">Home</span>
            </Link>

            <button
              onClick={handleLogout}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-red-950/60 hover:bg-red-900/80 text-red-300 border border-red-800/50 transition-colors"
              title="Encerrar Sessão"
            >
              <LogOut className="w-3.5 h-3.5" />
              <span className="hidden sm:inline">Sair</span>
            </button>
          </div>
        </div>
      </header>

      {/* Main Container */}
      <main className="flex-1 max-w-7xl w-full mx-auto p-4 sm:p-6 lg:p-8 flex flex-col gap-6">
        {/* Banner de Status Operacional */}
        <div className="bg-gradient-to-r from-slate-900 via-slate-800 to-slate-900 rounded-2xl p-5 border border-slate-800 shadow-xl flex flex-wrap items-center justify-between gap-4">
          <div className="space-y-1">
            <h1 className="text-xl sm:text-2xl font-bold text-white tracking-tight flex items-center gap-2">
              Painel de Despacho & Rastreamento em Tempo Real
            </h1>
            <p className="text-xs sm:text-sm text-slate-400">
              Crie pedidos Just-In-Time com raio viário e acompanhe a rota real do motociclista a ~36 km/h.
            </p>
          </div>

          <div className="flex items-center gap-3">
            <div className="text-right">
              <div className="text-[11px] text-slate-400 uppercase tracking-wider font-semibold">Taxa do Marketplace</div>
              <div className="text-lg font-black text-emerald-400">0% <span className="text-xs font-normal text-slate-300">(Economia de 27%)</span></div>
            </div>
            <div className="h-8 w-px bg-slate-700 mx-1" />
            <div className="text-right">
              <div className="text-[11px] text-slate-400 uppercase tracking-wider font-semibold">Motor de Roteamento</div>
              <div className="text-lg font-black text-cyan-400">OSRM Road Geometry</div>
            </div>
          </div>
        </div>

        {/* Dashboard Grid */}
        <div className="flex-1">
          <Dashboard />
        </div>
      </main>

      {/* Footer */}
      <footer className="border-t border-slate-800/80 py-4 px-6 text-center text-xs text-slate-500">
        VeloxLog • Arquitetura de Eventos de Baixa Latência • MapLibre GL & ConnectRPC LiveHub
      </footer>
    </div>
  );
}
