"use client";

import { useState } from "react";
import { logisticsClient } from "@/lib/connect-client";
import { PackageSize } from "@/gen/logistics/v1/api_pb";
import { 
  Zap, 
  MapPin, 
  User, 
  Phone, 
  Package, 
  Bike, 
  CheckCircle2, 
  AlertCircle,
  Sparkles
} from "lucide-react";

export function CreateOrderForm({ onOrderCreated }: { onOrderCreated?: (orderId: string) => void }) {
  const [loading, setLoading] = useState(false);
  const [successMsg, setSuccessMsg] = useState("");
  const [errorMsg, setErrorMsg] = useState("");
  
  // Estados para gerenciar o auto-fill do CEP e Coordenadas
  const [customerName, setCustomerName] = useState("");
  const [customerPhone, setCustomerPhone] = useState("");
  const [cep, setCep] = useState("");
  const [addressLine, setAddressLine] = useState("");
  const [addressNumber, setAddressNumber] = useState("");
  const [addressComplement, setAddressComplement] = useState("");
  const [packageSizeStr, setPackageSizeStr] = useState("LARGE");
  
  const [lat, setLat] = useState("");
  const [lng, setLng] = useState("");
  const [isSearchingCep, setIsSearchingCep] = useState(false);

  const fetchGeocoding = async (query: string) => {
    setIsSearchingCep(true);
    try {
      const geoRes = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(query)}`);
      const geoData = await geoRes.json();

      if (geoData && geoData.length > 0) {
        setLat(geoData[0].lat);
        setLng(geoData[0].lon);
        setErrorMsg(""); // Limpa erro se achar
      } else {
        // Fallback para coordenadas válidas de São Paulo caso o nominatim rate limite
        if (!lat) {
          setLat("-23.5653");
          setLng("-46.6515");
        }
      }
    } catch (err: any) {
      if (!lat) {
        setLat("-23.5653");
        setLng("-46.6515");
      }
    } finally {
      setIsSearchingCep(false);
    }
  };

  const handleCepChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const val = e.target.value.replace(/\D/g, "");
    setCep(val);

    if (val.length === 8) {
      setIsSearchingCep(true);
      setErrorMsg("");
      try {
        const cepRes = await fetch(`https://viacep.com.br/ws/${val}/json/`);
        const cepData = await cepRes.json();
        
        if (cepData.erro) throw new Error("CEP não encontrado.");
        
        const streetBase = `${cepData.logradouro}, ${cepData.localidade}, ${cepData.uf}, Brazil`;
        setAddressLine(streetBase);

        // Busca aproximada apenas pela rua
        await fetchGeocoding(streetBase);
      } catch (err: any) {
        setErrorMsg("Erro ao buscar CEP: " + err.message);
        setIsSearchingCep(false);
      }
    }
  };

  // Quando o número perde o foco, tenta refinar a busca no mapa
  const handleNumberBlur = () => {
    if (addressNumber && addressLine) {
      const parts = addressLine.split(",");
      if (parts.length >= 3) {
        const preciseQuery = `${parts[0]}, ${addressNumber}, ${parts[1]}, ${parts[2]}, Brazil`;
        fetchGeocoding(preciseQuery);
      }
    }
  };

  const handleFillSample = () => {
    setCustomerName("Mariana Souza");
    setCustomerPhone("11987654321");
    setCep("01310100");
    setAddressLine("Avenida Paulista, Bela Vista, São Paulo, SP, Brazil");
    setAddressNumber("1000");
    setAddressComplement("Conjunto 42");
    setLat("-23.5653");
    setLng("-46.6515");
    setPackageSizeStr("LARGE");
    setErrorMsg("");
  };

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setLoading(true);
    setSuccessMsg("");
    setErrorMsg("");

    const packageSize = packageSizeStr === "LARGE" ? PackageSize.LARGE : PackageSize.SMALL;

    let fullAddress = addressLine;
    if (addressNumber) fullAddress += `, ${addressNumber}`;
    if (addressComplement) fullAddress += ` - ${addressComplement}`;

    const destLat = parseFloat(lat || "-23.5653");
    const destLng = parseFloat(lng || "-46.6515");

    try {
      const response = await logisticsClient.createOrder({
        customer: { name: customerName, phone: customerPhone },
        destination: { lat: destLat, lng: destLng, address: fullAddress },
        packageSize,
      });

      setSuccessMsg(`Pedido ${response.orderId} criado com sucesso! Frete: R$ ${response.deliveryFee.toFixed(2)}`);
      if (onOrderCreated) {
        onOrderCreated(response.orderId);
      }
      // Reseta os estados
      setCustomerName("");
      setCustomerPhone("");
      setCep(""); 
      setAddressLine(""); 
      setAddressNumber(""); 
      setAddressComplement(""); 
      setLat(""); 
      setLng("");
    } catch (err: any) {
      setErrorMsg("Erro ao criar pedido: " + (err?.message || "Falha na comunicação com a API"));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="bg-slate-900 border border-slate-800 text-slate-100 p-6 rounded-2xl shadow-xl w-full">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 rounded-lg bg-cyan-950 text-cyan-400 flex items-center justify-center border border-cyan-800">
            <Zap className="w-4 h-4" />
          </div>
          <h2 className="text-lg font-bold text-white tracking-tight">Novo Despacho JIT</h2>
        </div>
        <button
          type="button"
          onClick={handleFillSample}
          className="text-xs text-cyan-400 hover:text-cyan-300 bg-cyan-950/60 hover:bg-cyan-950 px-2.5 py-1 rounded-lg border border-cyan-800/80 flex items-center gap-1 transition-colors"
          title="Preencher com dados de teste"
        >
          <Sparkles className="w-3 h-3" />
          <span>Exemplo Rápido</span>
        </button>
      </div>
      
      {successMsg && (
        <div className="mb-4 p-3 bg-emerald-950/80 border border-emerald-800 text-emerald-300 text-xs rounded-xl flex items-center gap-2">
          <CheckCircle2 className="w-4 h-4 shrink-0 text-emerald-400" />
          <span>{successMsg}</span>
        </div>
      )}

      {errorMsg && (
        <div className="mb-4 p-3 bg-red-950/80 border border-red-800 text-red-300 text-xs rounded-xl flex items-center gap-2">
          <AlertCircle className="w-4 h-4 shrink-0 text-red-400" />
          <span>{errorMsg}</span>
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-3.5">
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="block text-xs font-medium text-slate-300 mb-1">Nome do Cliente</label>
            <input 
              required 
              type="text" 
              value={customerName}
              onChange={(e) => setCustomerName(e.target.value)}
              className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 text-white text-xs outline-none" 
              placeholder="Ex: João Silva" 
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-300 mb-1">Telefone / WhatsApp</label>
            <input 
              required 
              type="text" 
              value={customerPhone}
              onChange={(e) => setCustomerPhone(e.target.value)}
              className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 text-white text-xs outline-none" 
              placeholder="11999999999" 
            />
          </div>
        </div>

        <div>
          <label className="block text-xs font-medium text-slate-300 mb-1 flex justify-between">
            <span>CEP do Destinatário</span>
            {isSearchingCep && <span className="text-cyan-400 text-[11px] animate-pulse">Consultando ViaCEP...</span>}
          </label>
          <input 
            required 
            type="text" 
            maxLength={8} 
            value={cep} 
            onChange={handleCepChange} 
            className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 text-white text-xs outline-none" 
            placeholder="Ex: 01310100" 
          />
        </div>

        <div>
          <label className="block text-xs font-medium text-slate-300 mb-1">Logradouro / Bairro</label>
          <input 
            required 
            type="text" 
            value={addressLine} 
            onChange={(e) => setAddressLine(e.target.value)} 
            className="w-full px-3 py-2 rounded-xl bg-slate-950/80 border border-slate-800 text-slate-300 text-xs outline-none" 
            placeholder="Preenchido automaticamente pelo CEP" 
          />
        </div>

        <div className="grid grid-cols-3 gap-2">
          <div className="col-span-1">
            <label className="block text-xs font-medium text-slate-300 mb-1">Número</label>
            <input 
              required 
              type="text" 
              value={addressNumber} 
              onBlur={handleNumberBlur} 
              onChange={(e) => setAddressNumber(e.target.value)} 
              className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 text-white text-xs outline-none" 
              placeholder="123" 
            />
          </div>
          <div className="col-span-2">
            <label className="block text-xs font-medium text-slate-300 mb-1">Complemento</label>
            <input 
              type="text" 
              value={addressComplement} 
              onChange={(e) => setAddressComplement(e.target.value)} 
              className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 text-white text-xs outline-none" 
              placeholder="Apto 45 Bloco B" 
            />
          </div>
        </div>

        <div className="grid grid-cols-2 gap-2 text-[11px]">
          <div className="bg-slate-950 p-2 rounded-lg border border-slate-800 text-slate-400">
            <span className="text-[10px] uppercase block font-mono text-cyan-400">Lat Destino</span>
            <span className="text-white font-mono">{lat || "-23.5653"}</span>
          </div>
          <div className="bg-slate-950 p-2 rounded-lg border border-slate-800 text-slate-400">
            <span className="text-[10px] uppercase block font-mono text-cyan-400">Lng Destino</span>
            <span className="text-white font-mono">{lng || "-46.6515"}</span>
          </div>
        </div>

        <div>
          <label className="block text-xs font-medium text-slate-300 mb-1">Tamanho & Requisito Veicular</label>
          <select 
            value={packageSizeStr}
            onChange={(e) => setPackageSizeStr(e.target.value)}
            className="w-full px-3 py-2 rounded-xl bg-slate-950 border border-slate-800 focus:border-cyan-500 text-white text-xs outline-none"
          >
            <option value="LARGE">📦 Grande (Requer Moto • 36 km/h)</option>
            <option value="SMALL">🎒 Pequeno (Bag de Bike ou Moto)</option>
          </select>
        </div>

        <button 
          type="submit" 
          disabled={loading || isSearchingCep} 
          className="w-full py-3 rounded-xl bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 text-white font-bold text-xs shadow-lg shadow-cyan-500/25 transition-all flex items-center justify-center gap-2 disabled:opacity-50"
        >
          {loading ? (
            <span>Alocando Motociclista Atômico...</span>
          ) : (
            <>
              <Bike className="w-4 h-4" />
              <span>Despachar Motociclista JIT</span>
            </>
          )}
        </button>
      </form>
    </div>
  );
}
