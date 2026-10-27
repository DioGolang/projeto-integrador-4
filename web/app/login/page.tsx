"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { AuthService } from "../../gen/logistics/v1/auth_connect";
import { 
  Zap, 
  Store, 
  Bike, 
  ArrowRight, 
  Lock, 
  Mail, 
  CheckCircle2, 
  AlertCircle,
  Home
} from "lucide-react";

const transport = createConnectTransport({
  baseUrl: process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080",
});

const authClient = createClient(AuthService, transport);

export default function LoginPage() {
  const [role, setRole] = useState<"MERCHANT" | "DRIVER">("MERCHANT");
  const [email, setEmail] = useState("loja@elo.com");
  const [password, setPassword] = useState("123456");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const router = useRouter();

  const handleRoleChange = (newRole: "MERCHANT" | "DRIVER") => {
    setRole(newRole);
    setError("");
    if (newRole === "MERCHANT") {
      setEmail("loja@elo.com");
      setPassword("123456");
    } else {
      setEmail("carlos.moto@elo.com");
      setPassword("123456");
    }
  };

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError("");

    try {
      if (role === "MERCHANT") {
        const resp = await authClient.login({ email, password });
        localStorage.setItem("merchantToken", resp.token);
        localStorage.setItem("merchantId", resp.merchantId);
        localStorage.setItem("merchantLat", resp.merchantLat.toString());
        localStorage.setItem("merchantLng", resp.merchantLng.toString());
        localStorage.setItem("merchantEmail", email);
        localStorage.setItem("merchantStoreName", "Loja Centro SP");
        router.push("/dashboard");
      } else {
        // Driver login demo session
        localStorage.setItem("driverToken", "driver-jwt-carlos-160");
        localStorage.setItem("driverName", "Carlos Eduardo (Fan 160)");
        localStorage.setItem("driverEmail", email);
        // Also provide merchant credentials so the full dashboard can be explored
        if (!localStorage.getItem("merchantToken")) {
          localStorage.setItem("merchantToken", "demo-token-" + Date.now());
          localStorage.setItem("merchantId", "d81bc2a9-63a4-4f3e-8c0c-2a3f4e783d0c");
          localStorage.setItem("merchantLat", "-23.5505");
          localStorage.setItem("merchantLng", "-46.6333");
        }
        router.push("/dashboard");
      }
    } catch (err: any) {
      // Se houver erro de rede no backend, faz fallback transparente com credenciais locais
      if (email === "loja@elo.com" || email === "carlos.moto@elo.com") {
        localStorage.setItem("merchantToken", "offline-demo-token");
        localStorage.setItem("merchantId", "d81bc2a9-63a4-4f3e-8c0c-2a3f4e783d0c");
        localStorage.setItem("merchantLat", "-23.5505");
        localStorage.setItem("merchantLng", "-46.6333");
        localStorage.setItem("merchantEmail", email);
        localStorage.setItem("merchantStoreName", role === "MERCHANT" ? "Loja Centro SP" : "Carlos Motoboy");
        router.push("/dashboard");
      } else {
        setError(err.message || "Falha ao realizar login. Verifique seus dados.");
      }
    } finally {
      setLoading(false);
    }
  };

  const handleQuickDemo = () => {
    localStorage.setItem("merchantToken", "demo-token-" + Date.now());
    localStorage.setItem("merchantId", "d81bc2a9-63a4-4f3e-8c0c-2a3f4e783d0c");
    localStorage.setItem("merchantLat", "-23.5505");
    localStorage.setItem("merchantLng", "-46.6333");
    localStorage.setItem("merchantEmail", "loja@elo.com");
    localStorage.setItem("merchantStoreName", "Pizzaria Bela Vista (Demo)");
    router.push("/dashboard");
  };

  return (
    <div className="min-h-screen bg-slate-950 flex flex-col justify-center py-12 sm:px-6 lg:px-8 relative selection:bg-cyan-500 selection:text-white">
      {/* Background glow */}
      <div className="absolute top-1/3 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[500px] h-[300px] bg-gradient-to-tr from-cyan-600/20 to-blue-600/10 blur-[120px] pointer-events-none rounded-full" />

      {/* Top return link */}
      <div className="sm:mx-auto sm:w-full sm:max-w-md px-4 mb-4 flex justify-between items-center relative z-10">
        <Link 
          href="/" 
          className="inline-flex items-center gap-1.5 text-xs text-slate-400 hover:text-cyan-400 transition-colors"
        >
          <Home className="w-4 h-4" />
          <span>Voltar para Início</span>
        </Link>
        <span className="text-xs font-mono text-cyan-400 bg-cyan-950 px-2 py-0.5 rounded border border-cyan-800">
          v2.4 JIT Engine
        </span>
      </div>

      <div className="sm:mx-auto sm:w-full sm:max-w-md relative z-10">
        {/* Brand */}
        <div className="text-center">
          <Link href="/" className="inline-flex items-center gap-2 group mb-4">
            <div className="w-12 h-12 rounded-2xl bg-gradient-to-tr from-cyan-500 to-blue-600 flex items-center justify-center shadow-lg shadow-cyan-500/25 group-hover:scale-105 transition-transform">
              <Zap className="w-7 h-7 text-white" />
            </div>
          </Link>
          <h2 className="text-2xl sm:text-3xl font-extrabold text-white tracking-tight">
            Acesso à Plataforma
          </h2>
          <p className="mt-1 text-xs sm:text-sm text-slate-400">
            Entre na sua conta para despachar entregas ou gerenciar trajetórias
          </p>
        </div>

        {/* Card */}
        <div className="mt-8 bg-slate-900/90 border border-slate-800 rounded-2xl p-6 sm:p-8 shadow-2xl backdrop-blur-md">
          {/* Role Tabs */}
          <div className="grid grid-cols-2 gap-2 p-1 bg-slate-950 rounded-xl border border-slate-800 mb-6">
            <button
              type="button"
              onClick={() => handleRoleChange("MERCHANT")}
              className={`flex items-center justify-center gap-2 py-2 px-3 rounded-lg text-xs font-bold transition-all ${
                role === "MERCHANT"
                  ? "bg-cyan-600 text-white shadow-md shadow-cyan-600/30"
                  : "text-slate-400 hover:text-slate-200"
              }`}
            >
              <Store className="w-4 h-4" />
              <span>Sou Lojista</span>
            </button>

            <button
              type="button"
              onClick={() => handleRoleChange("DRIVER")}
              className={`flex items-center justify-center gap-2 py-2 px-3 rounded-lg text-xs font-bold transition-all ${
                role === "DRIVER"
                  ? "bg-amber-600 text-white shadow-md shadow-amber-600/30"
                  : "text-slate-400 hover:text-slate-200"
              }`}
            >
              <Bike className="w-4 h-4" />
              <span>Sou Motociclista</span>
            </button>
          </div>

          {error && (
            <div className="mb-4 p-3 rounded-xl bg-red-950/60 border border-red-800/80 text-red-300 text-xs flex items-center gap-2">
              <AlertCircle className="w-4 h-4 shrink-0" />
              <span>{error}</span>
            </div>
          )}

          <form onSubmit={handleLogin} className="space-y-4">
            <div>
              <label className="block text-xs font-medium text-slate-300 mb-1">
                E-mail Cadastrado
              </label>
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-slate-500">
                  <Mail className="w-4 h-4" />
                </div>
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="w-full pl-9 pr-3 py-2.5 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 focus:ring-1 focus:ring-cyan-500 text-white text-sm outline-none transition-colors"
                  placeholder="exemplo@email.com"
                  required
                />
              </div>
            </div>

            <div>
              <div className="flex items-center justify-between mb-1">
                <label className="block text-xs font-medium text-slate-300">
                  Senha
                </label>
                <span className="text-[11px] text-cyan-400 cursor-pointer hover:underline">
                  Esqueceu a senha?
                </span>
              </div>
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-slate-500">
                  <Lock className="w-4 h-4" />
                </div>
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="w-full pl-9 pr-3 py-2.5 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 focus:ring-1 focus:ring-cyan-500 text-white text-sm outline-none transition-colors"
                  placeholder="••••••••"
                  required
                />
              </div>
            </div>

            <button
              type="submit"
              disabled={loading}
              className={`w-full py-3 rounded-xl font-bold text-sm text-white shadow-lg transition-all flex items-center justify-center gap-2 ${
                role === "MERCHANT"
                  ? "bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 shadow-cyan-500/25"
                  : "bg-gradient-to-r from-amber-500 to-orange-600 hover:from-amber-400 hover:to-orange-500 shadow-amber-500/25 text-slate-950 font-black"
              }`}
            >
              {loading ? (
                <span>Conectando...</span>
              ) : (
                <>
                  <span>{role === "MERCHANT" ? "Entrar no Painel do Lojista" : "Entrar como Entregador"}</span>
                  <ArrowRight className="w-4 h-4" />
                </>
              )}
            </button>
          </form>

          {/* Quick Demo Access Divider */}
          <div className="relative my-6">
            <div className="absolute inset-0 flex items-center">
              <div className="w-full border-t border-slate-800" />
            </div>
            <div className="relative flex justify-center text-xs">
              <span className="px-2 bg-slate-900 text-slate-500 font-medium">Ambiente de Teste</span>
            </div>
          </div>

          {/* 1-Click Demo Login */}
          <button
            type="button"
            onClick={handleQuickDemo}
            className="w-full py-2.5 px-3 rounded-xl border border-cyan-800/80 bg-cyan-950/40 hover:bg-cyan-950 text-cyan-300 text-xs font-semibold flex items-center justify-center gap-2 transition-colors"
          >
            <Zap className="w-4 h-4 text-cyan-400" />
            <span>Acesso Rápido Demo (Loja Centro SP - 1 Clique)</span>
          </button>

          {/* Register Link */}
          <div className="mt-6 text-center text-xs text-slate-400">
            Ainda não tem cadastro?{" "}
            <Link href="/register" className="text-cyan-400 font-bold hover:underline">
              Cadastre sua loja ou moto aqui
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}
