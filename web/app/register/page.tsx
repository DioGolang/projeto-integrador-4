"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { 
  Zap, 
  Store, 
  Bike, 
  ArrowRight, 
  Lock, 
  Mail, 
  MapPin, 
  CheckCircle2, 
  AlertCircle,
  Home,
  User,
  Phone,
  CreditCard,
  Building2,
  FileText
} from "lucide-react";

export default function RegisterPage() {
  const [role, setRole] = useState<"MERCHANT" | "DRIVER">("MERCHANT");
  const [loading, setLoading] = useState(false);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState("");
  const router = useRouter();

  // Estados Lojista
  const [storeName, setStoreName] = useState("");
  const [merchantEmail, setMerchantEmail] = useState("");
  const [merchantPassword, setMerchantPassword] = useState("");
  const [merchantPhone, setMerchantPhone] = useState("");
  const [cnpj, setCnpj] = useState("");
  const [category, setCategory] = useState("Restaurante / Alimentação");
  
  // Endereço Lojista com ViaCEP
  const [cep, setCep] = useState("");
  const [addressLine, setAddressLine] = useState("");
  const [addressNumber, setAddressNumber] = useState("");
  const [addressNeighborhood, setAddressNeighborhood] = useState("");
  const [addressCity, setAddressCity] = useState("São Paulo");
  const [addressState, setAddressState] = useState("SP");
  const [lat, setLat] = useState("-23.5505");
  const [lng, setLng] = useState("-46.6333");
  const [isSearchingCep, setIsSearchingCep] = useState(false);

  // Estados Motorista
  const [driverName, setDriverName] = useState("");
  const [driverEmail, setDriverEmail] = useState("");
  const [driverPassword, setDriverPassword] = useState("");
  const [driverPhone, setDriverPhone] = useState("");
  const [cnh, setCnh] = useState("");
  const [vehicleType, setVehicleType] = useState("MOTO");
  const [vehicleModel, setVehicleModel] = useState("Honda CG 160 Fan");
  const [vehiclePlate, setVehiclePlate] = useState("BRA2E19");
  const [pixKey, setPixKey] = useState("");

  const handleCepChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const val = e.target.value.replace(/\D/g, "");
    setCep(val);

    if (val.length === 8) {
      setIsSearchingCep(true);
      setError("");
      try {
        const res = await fetch(`https://viacep.com.br/ws/${val}/json/`);
        const data = await res.json();
        if (data.erro) {
          setError("CEP não encontrado.");
          setIsSearchingCep(false);
          return;
        }

        setAddressLine(data.logradouro || "");
        setAddressNeighborhood(data.bairro || "");
        setAddressCity(data.localidade || "São Paulo");
        setAddressState(data.uf || "SP");

        // Geocodificação aproximada via Nominatim OpenStreetMap
        const query = `${data.logradouro}, ${data.bairro}, ${data.localidade}, Brazil`;
        try {
          const geoRes = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(query)}`);
          const geoData = await geoRes.json();
          if (geoData && geoData.length > 0) {
            setLat(geoData[0].lat);
            setLng(geoData[0].lon);
          }
        } catch {
          // Mantém as coordenadas de São Paulo Centro
        }
      } catch (err: any) {
        setError("Erro ao consultar CEP: " + err.message);
      } finally {
        setIsSearchingCep(false);
      }
    }
  };

  const handleRegisterMerchant = (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError("");

    try {
      const generatedId = "merchant-" + Math.random().toString(36).substring(2, 10);
      localStorage.setItem("merchantToken", "token-" + generatedId);
      localStorage.setItem("merchantId", generatedId);
      localStorage.setItem("merchantStoreName", storeName || "Minha Loja");
      localStorage.setItem("merchantEmail", merchantEmail);
      localStorage.setItem("merchantLat", lat || "-23.5505");
      localStorage.setItem("merchantLng", lng || "-46.6333");
      localStorage.setItem("merchantAddress", `${addressLine}, ${addressNumber} - ${addressNeighborhood}, ${addressCity}`);

      setSuccess(true);
      setTimeout(() => {
        router.push("/dashboard");
      }, 1000);
    } catch (err: any) {
      setError("Erro ao cadastrar lojista.");
    } finally {
      setLoading(false);
    }
  };

  const handleRegisterDriver = (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError("");

    try {
      const generatedId = "driver-" + Math.random().toString(36).substring(2, 10);
      localStorage.setItem("driverToken", "token-" + generatedId);
      localStorage.setItem("driverId", generatedId);
      localStorage.setItem("driverName", driverName || "Entregador");
      localStorage.setItem("driverEmail", driverEmail);
      localStorage.setItem("driverVehicle", `${vehicleType} - ${vehicleModel} (${vehiclePlate})`);
      
      // Também provê sessão para que o motorista explore a tela ao vivo
      if (!localStorage.getItem("merchantToken")) {
        localStorage.setItem("merchantToken", "demo-token-" + Date.now());
        localStorage.setItem("merchantId", "d81bc2a9-63a4-4f3e-8c0c-2a3f4e783d0c");
        localStorage.setItem("merchantLat", "-23.5505");
        localStorage.setItem("merchantLng", "-46.6333");
        localStorage.setItem("merchantStoreName", `Base do Piloto (${driverName})`);
      }

      setSuccess(true);
      setTimeout(() => {
        router.push("/dashboard");
      }, 1000);
    } catch (err: any) {
      setError("Erro ao cadastrar entregador.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-950 flex flex-col justify-center py-12 sm:px-6 lg:px-8 relative selection:bg-cyan-500 selection:text-white">
      {/* Background glow */}
      <div className="absolute top-1/4 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[350px] bg-gradient-to-tr from-cyan-600/20 to-blue-600/10 blur-[140px] pointer-events-none rounded-full" />

      {/* Top Header */}
      <div className="sm:mx-auto sm:w-full sm:max-w-2xl px-4 mb-4 flex justify-between items-center relative z-10">
        <Link 
          href="/" 
          className="inline-flex items-center gap-1.5 text-xs text-slate-400 hover:text-cyan-400 transition-colors"
        >
          <Home className="w-4 h-4" />
          <span>Voltar para Início</span>
        </Link>
        <Link
          href="/login"
          className="text-xs text-cyan-400 hover:underline font-semibold"
        >
          Já tem conta? Entrar
        </Link>
      </div>

      <div className="sm:mx-auto sm:w-full sm:max-w-2xl relative z-10 px-4">
        {/* Brand */}
        <div className="text-center mb-6">
          <Link href="/" className="inline-flex items-center gap-2 group mb-3">
            <div className="w-12 h-12 rounded-2xl bg-gradient-to-tr from-cyan-500 to-blue-600 flex items-center justify-center shadow-lg shadow-cyan-500/25 group-hover:scale-105 transition-transform">
              <Zap className="w-7 h-7 text-white" />
            </div>
          </Link>
          <h2 className="text-2xl sm:text-3xl font-extrabold text-white tracking-tight">
            Criar Nova Conta no VeloxLog
          </h2>
          <p className="mt-1 text-xs sm:text-sm text-slate-400">
            Despacho Just-In-Time com taxa zero de marketplace e rotas viárias reais
          </p>
        </div>

        {/* Card */}
        <div className="bg-slate-900/95 border border-slate-800 rounded-2xl p-6 sm:p-8 shadow-2xl backdrop-blur-md">
          {/* Tabs */}
          <div className="grid grid-cols-2 gap-2 p-1 bg-slate-950 rounded-xl border border-slate-800 mb-6">
            <button
              type="button"
              onClick={() => { setRole("MERCHANT"); setError(""); }}
              className={`flex items-center justify-center gap-2 py-2.5 px-3 rounded-lg text-xs font-bold transition-all ${
                role === "MERCHANT"
                  ? "bg-cyan-600 text-white shadow-md shadow-cyan-600/30"
                  : "text-slate-400 hover:text-slate-200"
              }`}
            >
              <Store className="w-4 h-4" />
              <span>Quero Cadastrar Minha Loja</span>
            </button>

            <button
              type="button"
              onClick={() => { setRole("DRIVER"); setError(""); }}
              className={`flex items-center justify-center gap-2 py-2.5 px-3 rounded-lg text-xs font-bold transition-all ${
                role === "DRIVER"
                  ? "bg-amber-600 text-white shadow-md shadow-amber-600/30"
                  : "text-slate-400 hover:text-slate-200"
              }`}
            >
              <Bike className="w-4 h-4" />
              <span>Quero Entregar de Moto</span>
            </button>
          </div>

          {success && (
            <div className="mb-6 p-4 rounded-xl bg-emerald-950/80 border border-emerald-800 text-emerald-300 text-sm flex items-center gap-3">
              <CheckCircle2 className="w-5 h-5 shrink-0 text-emerald-400" />
              <div>
                <div className="font-bold">Cadastro realizado com sucesso!</div>
                <div className="text-xs text-emerald-400">Redirecionando para o painel de operações...</div>
              </div>
            </div>
          )}

          {error && (
            <div className="mb-6 p-3 rounded-xl bg-red-950/60 border border-red-800 text-red-300 text-xs flex items-center gap-2">
              <AlertCircle className="w-4 h-4 shrink-0" />
              <span>{error}</span>
            </div>
          )}

          {/* Form Lojista */}
          {role === "MERCHANT" && (
            <form onSubmit={handleRegisterMerchant} className="space-y-4">
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Nome da Loja / Razão Social
                  </label>
                  <input
                    type="text"
                    required
                    value={storeName}
                    onChange={(e) => setStoreName(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 text-white text-sm outline-none"
                    placeholder="Ex: Pizzaria Forno Nobre"
                  />
                </div>

                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    CNPJ ou CPF
                  </label>
                  <input
                    type="text"
                    value={cnpj}
                    onChange={(e) => setCnpj(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 text-white text-sm outline-none"
                    placeholder="00.000.000/0001-00"
                  />
                </div>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    E-mail do Administrador
                  </label>
                  <input
                    type="email"
                    required
                    value={merchantEmail}
                    onChange={(e) => setMerchantEmail(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 text-white text-sm outline-none"
                    placeholder="contato@minhaloja.com.br"
                  />
                </div>

                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Senha de Acesso
                  </label>
                  <input
                    type="password"
                    required
                    value={merchantPassword}
                    onChange={(e) => setMerchantPassword(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 text-white text-sm outline-none"
                    placeholder="••••••••"
                  />
                </div>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    WhatsApp para Avisos
                  </label>
                  <input
                    type="text"
                    value={merchantPhone}
                    onChange={(e) => setMerchantPhone(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 text-white text-sm outline-none"
                    placeholder="(11) 99999-9999"
                  />
                </div>

                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Categoria do Estabelecimento
                  </label>
                  <select
                    value={category}
                    onChange={(e) => setCategory(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 text-white text-sm outline-none"
                  >
                    <option>Restaurante / Alimentação</option>
                    <option>Hamburgueria / Lanches</option>
                    <option>Farmácia / Saúde</option>
                    <option>Mercado / Conveniência</option>
                    <option>Bebidas / Adega</option>
                    <option>PetShop</option>
                    <option>Outro</option>
                  </select>
                </div>
              </div>

              {/* Endereço & Ponto de Coleta */}
              <div className="pt-2 border-t border-slate-800 space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-bold text-cyan-400 uppercase tracking-wider">
                    Ponto de Coleta da Loja (GPS)
                  </span>
                  {isSearchingCep && (
                    <span className="text-[11px] text-cyan-400 animate-pulse">
                      Consultando ViaCEP e GPS...
                    </span>
                  )}
                </div>

                <div className="grid grid-cols-3 gap-3">
                  <div className="col-span-1">
                    <label className="block text-xs font-medium text-slate-300 mb-1">CEP</label>
                    <input
                      type="text"
                      maxLength={8}
                      required
                      value={cep}
                      onChange={handleCepChange}
                      className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 text-white text-sm outline-none"
                      placeholder="01001000"
                    />
                  </div>
                  <div className="col-span-2">
                    <label className="block text-xs font-medium text-slate-300 mb-1">Rua / Logradouro</label>
                    <input
                      type="text"
                      required
                      value={addressLine}
                      onChange={(e) => setAddressLine(e.target.value)}
                      className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 text-white text-sm outline-none"
                      placeholder="Preenchido pelo CEP"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-3 gap-3">
                  <div>
                    <label className="block text-xs font-medium text-slate-300 mb-1">Número</label>
                    <input
                      type="text"
                      required
                      value={addressNumber}
                      onChange={(e) => setAddressNumber(e.target.value)}
                      className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 text-white text-sm outline-none"
                      placeholder="123"
                    />
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-slate-300 mb-1">Bairro</label>
                    <input
                      type="text"
                      value={addressNeighborhood}
                      onChange={(e) => setAddressNeighborhood(e.target.value)}
                      className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 text-white text-sm outline-none"
                      placeholder="Bairro"
                    />
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-slate-300 mb-1">Cidade / UF</label>
                    <input
                      type="text"
                      value={`${addressCity} / ${addressState}`}
                      readOnly
                      className="w-full px-3 py-2 rounded-xl bg-slate-950/70 border border-slate-800 text-slate-400 text-sm outline-none"
                    />
                  </div>
                </div>

                {/* GPS Coordinates preview */}
                <div className="grid grid-cols-2 gap-3 text-xs">
                  <div className="bg-slate-950 p-2 rounded-lg border border-slate-800 text-slate-400">
                    <span className="text-[10px] uppercase block font-mono text-cyan-400">Lat Retirada</span>
                    <span className="text-white font-mono">{lat}</span>
                  </div>
                  <div className="bg-slate-950 p-2 rounded-lg border border-slate-800 text-slate-400">
                    <span className="text-[10px] uppercase block font-mono text-cyan-400">Lng Retirada</span>
                    <span className="text-white font-mono">{lng}</span>
                  </div>
                </div>
              </div>

              <button
                type="submit"
                disabled={loading}
                className="w-full mt-4 py-3.5 rounded-xl font-bold text-sm text-white bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 shadow-lg shadow-cyan-500/25 transition-all flex items-center justify-center gap-2"
              >
                {loading ? "Cadastrando Loja..." : "Concluir Cadastro & Abrir Dashboard"}
                <ArrowRight className="w-4 h-4" />
              </button>
            </form>
          )}

          {/* Form Motorista */}
          {role === "DRIVER" && (
            <form onSubmit={handleRegisterDriver} className="space-y-4">
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Nome Completo do Piloto
                  </label>
                  <input
                    type="text"
                    required
                    value={driverName}
                    onChange={(e) => setDriverName(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-amber-500 text-white text-sm outline-none"
                    placeholder="Carlos Eduardo Pereira"
                  />
                </div>

                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Número da CNH
                  </label>
                  <input
                    type="text"
                    required
                    value={cnh}
                    onChange={(e) => setCnh(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-amber-500 text-white text-sm outline-none"
                    placeholder="12345678900"
                  />
                </div>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    E-mail
                  </label>
                  <input
                    type="email"
                    required
                    value={driverEmail}
                    onChange={(e) => setDriverEmail(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-amber-500 text-white text-sm outline-none"
                    placeholder="carlos.moto@gmail.com"
                  />
                </div>

                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Senha
                  </label>
                  <input
                    type="password"
                    required
                    value={driverPassword}
                    onChange={(e) => setDriverPassword(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-amber-500 text-white text-sm outline-none"
                    placeholder="••••••••"
                  />
                </div>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Telefone / WhatsApp
                  </label>
                  <input
                    type="text"
                    value={driverPhone}
                    onChange={(e) => setDriverPhone(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-amber-500 text-white text-sm outline-none"
                    placeholder="(11) 98888-7777"
                  />
                </div>

                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Veículo Principal
                  </label>
                  <select
                    value={vehicleType}
                    onChange={(e) => setVehicleType(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-amber-500 text-white text-sm outline-none"
                  >
                    <option value="MOTO">Motocicleta (Recomendado - 36 km/h)</option>
                    <option value="BIKE">Bicicleta</option>
                    <option value="CARRO">Carro</option>
                  </select>
                </div>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Modelo do Veículo
                  </label>
                  <input
                    type="text"
                    value={vehicleModel}
                    onChange={(e) => setVehicleModel(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-amber-500 text-white text-sm outline-none"
                    placeholder="Ex: Honda CG 160 Fan"
                  />
                </div>

                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Placa do Veículo
                  </label>
                  <input
                    type="text"
                    value={vehiclePlate}
                    onChange={(e) => setVehiclePlate(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-amber-500 text-white text-sm outline-none uppercase"
                    placeholder="BRA2E19"
                  />
                </div>
              </div>

              {/* Chave PIX */}
              <div className="pt-2 border-t border-slate-800">
                <label className="block text-xs font-medium text-slate-300 mb-1">
                  Chave PIX para Recebimento Imediato (Taxa Zero)
                </label>
                <input
                  type="text"
                  required
                  value={pixKey}
                  onChange={(e) => setPixKey(e.target.value)}
                  className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-amber-500 text-white text-sm outline-none"
                  placeholder="CPF, celular, e-mail ou chave aleatória"
                />
              </div>

              <button
                type="submit"
                disabled={loading}
                className="w-full mt-4 py-3.5 rounded-xl font-bold text-sm text-slate-950 bg-gradient-to-r from-amber-400 to-orange-500 hover:from-amber-300 hover:to-orange-400 shadow-lg shadow-amber-500/25 transition-all flex items-center justify-center gap-2"
              >
                {loading ? "Cadastrando Piloto..." : "Concluir Cadastro de Piloto"}
                <ArrowRight className="w-4 h-4" />
              </button>
            </form>
          )}

          {/* Login Link */}
          <div className="mt-6 text-center text-xs text-slate-400">
            Já possui cadastro ativo?{" "}
            <Link href="/login" className="text-cyan-400 font-bold hover:underline">
              Clique aqui para entrar
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}
