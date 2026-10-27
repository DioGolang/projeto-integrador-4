"use client";

import Link from "next/link";
import Image from "next/image";
import { 
  Zap, 
  Store, 
  Navigation, 
  ShieldCheck, 
  ArrowRight, 
  TrendingUp, 
  MapPin, 
  Cpu, 
  DollarSign, 
  Clock, 
  Layers, 
  CheckCircle2, 
  Radio, 
  Smartphone,
  ChevronRight
} from "lucide-react";

export default function HomePage() {
  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col selection:bg-cyan-500 selection:text-white">
      {/* Top Navbar */}
      <header className="sticky top-0 z-50 bg-slate-950/80 backdrop-blur-lg border-b border-slate-800/80">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Link href="/" className="flex items-center gap-2 group">
              <div className="w-10 h-10 rounded-xl bg-gradient-to-tr from-cyan-500 to-blue-600 flex items-center justify-center shadow-lg shadow-cyan-500/25 group-hover:scale-105 transition-transform">
                <Zap className="w-6 h-6 text-white" />
              </div>
              <span className="font-extrabold text-xl tracking-tight text-white">
                VELOX<span className="text-cyan-400">LOG</span>
              </span>
            </Link>
            <span className="hidden md:inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-cyan-950/80 text-cyan-300 border border-cyan-800/80">
              <Radio className="w-3 h-3 text-cyan-400 animate-pulse" />
              Despacho JIT Atômico
            </span>
          </div>

          {/* Desktop Nav */}
          <nav className="hidden md:flex items-center gap-6 text-sm font-medium text-slate-300">
            <a href="#como-funciona" className="hover:text-cyan-400 transition-colors">Como Funciona</a>
            <a href="#lojistas" className="hover:text-cyan-400 transition-colors">Para Lojistas</a>
            <a href="#motoristas" className="hover:text-cyan-400 transition-colors">Para Motociclistas</a>
            <a href="#tecnologia" className="hover:text-cyan-400 transition-colors">Tecnologia</a>
          </nav>

          {/* Action CTAs */}
          <div className="flex items-center gap-3">
            <Link
              href="/login"
              className="text-sm font-semibold text-slate-300 hover:text-white px-3 py-1.5 rounded-lg transition-colors"
            >
              Entrar
            </Link>
            <Link
              href="/register"
              className="hidden sm:inline-flex text-sm font-semibold text-cyan-300 bg-cyan-950/60 hover:bg-cyan-900 border border-cyan-700/60 px-3.5 py-1.5 rounded-lg transition-all"
            >
              Criar Conta
            </Link>
            <Link
              href="/dashboard"
              className="inline-flex items-center gap-1.5 text-sm font-semibold text-white bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 px-4 py-1.5 rounded-lg shadow-md shadow-cyan-500/20 transition-all hover:scale-[1.02]"
            >
              <span>Abrir Live Dash</span>
              <ArrowRight className="w-4 h-4" />
            </Link>
          </div>
        </div>
      </header>

      {/* Hero Section */}
      <section className="relative overflow-hidden pt-12 pb-20 lg:pt-20 lg:pb-28">
        {/* Glow ambient background */}
        <div className="absolute top-1/4 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[350px] bg-gradient-to-tr from-cyan-600/20 to-blue-600/10 blur-[130px] pointer-events-none rounded-full" />

        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 relative z-10">
          <div className="grid grid-cols-1 lg:grid-cols-12 gap-12 items-center">
            {/* Left Column: Copy */}
            <div className="lg:col-span-7 space-y-6 text-center lg:text-left">
              <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-full bg-slate-900 border border-cyan-800/80 text-xs text-cyan-300 shadow-sm">
                <span className="w-2 h-2 rounded-full bg-cyan-400 animate-ping" />
                <span>Zero Comissões Predatórias • Roteamento Viário Real</span>
              </div>

              <h1 className="text-4xl sm:text-5xl lg:text-6xl font-black tracking-tight text-white leading-tight">
                Elimine os <span className="text-transparent bg-clip-text bg-gradient-to-r from-red-400 to-amber-400">27% de taxas</span> dos marketplaces.
                <br />
                <span className="text-transparent bg-clip-text bg-gradient-to-r from-cyan-400 via-sky-300 to-blue-500">
                  Despacho direto com moto em tempo real.
                </span>
              </h1>

              <p className="text-lg sm:text-xl text-slate-300 max-w-2xl mx-auto lg:mx-0 font-light leading-relaxed">
                Um motor logístico Just-In-Time desenhado para o comércio de bairro.
                Alocação atômica em milissegundos, geometria real pela malha viária (OSRM),
                pacing a <strong className="text-cyan-300 font-semibold">36 km/h</strong> e telemetria contínua via ConnectRPC.
              </p>

              {/* CTAs */}
              <div className="pt-2 flex flex-wrap items-center justify-center lg:justify-start gap-4">
                <Link
                  href="/register"
                  className="px-6 py-3.5 rounded-xl font-bold text-white bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 shadow-lg shadow-cyan-500/25 flex items-center gap-2 transition-all hover:scale-105"
                >
                  <Store className="w-5 h-5" />
                  <span>Cadastrar Loja ou Piloto</span>
                  <ChevronRight className="w-4 h-4" />
                </Link>

                <Link
                  href="/dashboard"
                  className="px-6 py-3.5 rounded-xl font-bold text-slate-200 bg-slate-900/90 hover:bg-slate-800 border border-slate-700/80 flex items-center gap-2 transition-all hover:border-cyan-500/60"
                >
                  <Navigation className="w-5 h-5 text-cyan-400" />
                  <span>Ver Demonstração Ao Vivo</span>
                </Link>
              </div>

              {/* Trust bullets */}
              <div className="pt-4 flex flex-wrap items-center justify-center lg:justify-start gap-6 text-xs text-slate-400">
                <div className="flex items-center gap-1.5">
                  <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                  <span>Sem mensalidade de adesão</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <CheckCircle2 className="w-4 h-4 text-cyan-400" />
                  <span>Geometria viária real (sem retas cegas)</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <CheckCircle2 className="w-4 h-4 text-amber-400" />
                  <span>Repasse instantâneo via PIX</span>
                </div>
              </div>
            </div>

            {/* Right Column: Hero Graphic */}
            <div className="lg:col-span-5 relative">
              <div className="relative mx-auto max-w-md lg:max-w-none">
                <div className="relative rounded-2xl overflow-hidden border border-cyan-500/30 shadow-2xl shadow-cyan-950/50 group">
                  <img
                    src="/images/hero_logistics_delivery.jpg"
                    alt="Simulação 3D de Rota Logística e Despacho de Moto em São Paulo"
                    className="w-full h-auto object-cover group-hover:scale-105 transition-transform duration-700"
                  />
                  <div className="absolute inset-0 bg-gradient-to-t from-slate-950/90 via-slate-950/20 to-transparent pointer-events-none" />

                  {/* Overlaid Badges */}
                  <div className="absolute bottom-4 left-4 right-4 flex items-center justify-between bg-slate-900/90 backdrop-blur-md p-3 rounded-xl border border-slate-700/80 text-xs">
                    <div className="flex items-center gap-2.5">
                      <div className="w-3 h-3 rounded-full bg-emerald-500 animate-ping" />
                      <div>
                        <div className="font-semibold text-white">Trajetória Real Ativa</div>
                        <div className="text-[11px] text-cyan-400 font-mono">São Paulo Centro • Pacing 36 km/h</div>
                      </div>
                    </div>
                    <span className="px-2.5 py-1 rounded bg-cyan-950 text-cyan-300 font-mono text-[11px] border border-cyan-800">
                      OSRM Ready
                    </span>
                  </div>
                </div>

                {/* Floating pill 1 */}
                <div className="absolute -top-4 -right-4 bg-slate-900/95 border border-cyan-500/50 p-2.5 px-4 rounded-xl shadow-xl flex items-center gap-2 backdrop-blur-sm text-xs font-semibold text-slate-200 hidden sm:flex">
                  <Zap className="w-4 h-4 text-cyan-400" />
                  <span>Despacho Atômico &lt; 15ms</span>
                </div>

                {/* Floating pill 2 */}
                <div className="absolute -bottom-4 -left-4 bg-slate-900/95 border border-emerald-500/50 p-2.5 px-4 rounded-xl shadow-xl flex items-center gap-2 backdrop-blur-sm text-xs font-semibold text-slate-200 hidden sm:flex">
                  <TrendingUp className="w-4 h-4 text-emerald-400" />
                  <span>Economia de até 27% por pedido</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Proof Metrics Banner */}
      <section className="border-y border-slate-800 bg-slate-900/60 py-12">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-8 text-center">
            <div className="space-y-1">
              <div className="text-3xl sm:text-4xl font-black text-emerald-400 tracking-tight">0%</div>
              <div className="text-sm font-semibold text-white">Taxa de Intermediação</div>
              <p className="text-xs text-slate-400">Todo o lucro fica com seu restaurante e o piloto</p>
            </div>

            <div className="space-y-1">
              <div className="text-3xl sm:text-4xl font-black text-cyan-400 tracking-tight">&lt; 15ms</div>
              <div className="text-sm font-semibold text-white">Bloqueio Concorrente</div>
              <p className="text-xs text-slate-400">SELECT FOR UPDATE e integridade causal estrita</p>
            </div>

            <div className="space-y-1">
              <div className="text-3xl sm:text-4xl font-black text-blue-400 tracking-tight">100%</div>
              <div className="text-sm font-semibold text-white">Curvatura Viária OSRM</div>
              <p className="text-xs text-slate-400">Linha azul que segue ruas e avenidas reais</p>
            </div>

            <div className="space-y-1">
              <div className="text-3xl sm:text-4xl font-black text-amber-400 tracking-tight">36 km/h</div>
              <div className="text-sm font-semibold text-white">Velocidade Média de Moto</div>
              <p className="text-xs text-slate-400">Replayer temporal com estimativa física de percurso</p>
            </div>
          </div>
        </div>
      </section>

      {/* How it Works / Architecture */}
      <section id="como-funciona" className="py-20 bg-slate-950">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 space-y-12">
          <div className="text-center max-w-3xl mx-auto space-y-3">
            <span className="text-xs font-bold text-cyan-400 tracking-wider uppercase bg-cyan-950/60 px-3 py-1 rounded-full border border-cyan-800">
              Engenharia P2P Descentralizada
            </span>
            <h2 className="text-3xl sm:text-4xl font-extrabold text-white tracking-tight">
              Como funciona o ciclo de despacho de ponta a ponta
            </h2>
            <p className="text-slate-400 text-sm sm:text-base">
              Do clique no botão até a entrega final na mão do cliente, cada etapa é garantida por eventos imutáveis e telemetria contínua.
            </p>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-12 gap-8 items-center">
            {/* Visual Architecture Image */}
            <div className="lg:col-span-6 rounded-2xl overflow-hidden border border-slate-800 shadow-xl bg-slate-900">
              <img
                src="/images/network_dispatch_flow.jpg"
                alt="Fluxo de Rede e Despacho Assíncrono com LiveHub"
                className="w-full h-auto object-cover"
              />
            </div>

            {/* Stepper Cards */}
            <div className="lg:col-span-6 space-y-4">
              <div className="p-4 rounded-xl bg-slate-900/90 border border-slate-800 hover:border-cyan-500/50 transition-colors flex gap-4">
                <div className="w-10 h-10 rounded-lg bg-cyan-950 text-cyan-400 flex items-center justify-center font-bold text-base shrink-0 border border-cyan-800">
                  1
                </div>
                <div>
                  <h3 className="font-bold text-white text-base">Criação do Pedido & Auto-Geocodificação</h3>
                  <p className="text-xs text-slate-400 mt-1 leading-relaxed">
                    O lojista informa apenas o CEP e número do cliente. O sistema consulta automaticamente o ViaCEP e geocodifica via OpenStreetMap Nominatim, calculando o ponto exato de entrega.
                  </p>
                </div>
              </div>

              <div className="p-4 rounded-xl bg-slate-900/90 border border-slate-800 hover:border-cyan-500/50 transition-colors flex gap-4">
                <div className="w-10 h-10 rounded-lg bg-blue-950 text-blue-400 flex items-center justify-center font-bold text-base shrink-0 border border-blue-800">
                  2
                </div>
                <div>
                  <h3 className="font-bold text-white text-base">Alocação Atômica & Publicação no Outbox</h3>
                  <p className="text-xs text-slate-400 mt-1 leading-relaxed">
                    O Postgres executa a transação atômica bloqueando o entregador com <code className="text-cyan-300">FOR UPDATE</code>. O evento de domínio é gravado na tabela outbox e publicado de forma assíncrona no RabbitMQ.
                  </p>
                </div>
              </div>

              <div className="p-4 rounded-xl bg-slate-900/90 border border-slate-800 hover:border-cyan-500/50 transition-colors flex gap-4">
                <div className="w-10 h-10 rounded-lg bg-sky-950 text-sky-400 flex items-center justify-center font-bold text-base shrink-0 border border-sky-800">
                  3
                </div>
                <div>
                  <h3 className="font-bold text-white text-base">Cálculo de Geometria Viária via OSRM</h3>
                  <p className="text-xs text-slate-400 mt-1 leading-relaxed">
                    O simulador consome o evento, consulta o motor OSRM e extrai a polyline exata da rota pelas ruas de São Paulo, emitindo o evento <code className="text-cyan-300">DeliveryRouteAssigned</code> com distância e duração estimada.
                  </p>
                </div>
              </div>

              <div className="p-4 rounded-xl bg-slate-900/90 border border-slate-800 hover:border-cyan-500/50 transition-colors flex gap-4">
                <div className="w-10 h-10 rounded-lg bg-emerald-950 text-emerald-400 flex items-center justify-center font-bold text-base shrink-0 border border-emerald-800">
                  4
                </div>
                <div>
                  <h3 className="font-bold text-white text-base">LiveHub Stream & Animação a 36 km/h</h3>
                  <p className="text-xs text-slate-400 mt-1 leading-relaxed">
                    O frontend recebe as coordenadas via ConnectRPC streaming e o <code className="text-emerald-300">TemporalReplayer</code> move suavemente o ícone da moto ao longo da rota em velocidade realística.
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* For Merchants & For Drivers */}
      <section id="lojistas" className="py-20 bg-slate-900/50 border-t border-slate-800">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 space-y-16">
          {/* Lojistas */}
          <div className="grid grid-cols-1 lg:grid-cols-12 gap-12 items-center">
            <div className="lg:col-span-6 space-y-6">
              <span className="text-xs font-bold text-cyan-400 tracking-wider uppercase bg-cyan-950/80 px-3 py-1 rounded-full border border-cyan-800">
                Para o Lojista
              </span>
              <h2 className="text-3xl sm:text-4xl font-extrabold text-white tracking-tight">
                Recupere o controle da sua operação e pare de pagar pedágio.
              </h2>
              <p className="text-slate-300 text-base leading-relaxed">
                Quando você vende R$ 10.000,00 no mês, R$ 2.700,00 somem em taxas dos apps tradicionais. 
                Com o VeloxLog, você realiza o despacho direto para entregadores parceiros com tarifa fixa ou repasse transparente.
              </p>

              <div className="space-y-3 text-sm">
                <div className="flex items-start gap-3">
                  <div className="p-1 rounded bg-cyan-950 text-cyan-400 mt-0.5 border border-cyan-800">
                    <CheckCircle2 className="w-4 h-4" />
                  </div>
                  <div>
                    <strong className="text-white">Painel Web JIT:</strong> Crie um despacho em menos de 10 segundos preenchendo apenas CEP e número.
                  </div>
                </div>

                <div className="flex items-start gap-3">
                  <div className="p-1 rounded bg-cyan-950 text-cyan-400 mt-0.5 border border-cyan-800">
                    <CheckCircle2 className="w-4 h-4" />
                  </div>
                  <div>
                    <strong className="text-white">Rastreamento Transparente:</strong> Veja exatamente onde a moto está, a velocidade média e os minutos restantes para entrega.
                  </div>
                </div>

                <div className="flex items-start gap-3">
                  <div className="p-1 rounded bg-cyan-950 text-cyan-400 mt-0.5 border border-cyan-800">
                    <CheckCircle2 className="w-4 h-4" />
                  </div>
                  <div>
                    <strong className="text-white">Preço Justo de Frete:</strong> Cálculo geométrico com base na distância real percorrida na malha viária.
                  </div>
                </div>
              </div>

              <div className="pt-2">
                <Link
                  href="/register"
                  className="inline-flex items-center gap-2 px-5 py-3 rounded-xl bg-cyan-600 hover:bg-cyan-500 text-white font-bold text-sm shadow-lg shadow-cyan-600/20 transition-all"
                >
                  <span>Cadastrar Minha Loja Gratuitamente</span>
                  <ArrowRight className="w-4 h-4" />
                </Link>
              </div>
            </div>

            <div id="motoristas" className="lg:col-span-6 space-y-6">
              <div className="rounded-2xl overflow-hidden border border-slate-800 shadow-xl bg-slate-900 group">
                <img
                  src="/images/driver_courier_fleet.jpg"
                  alt="Frota de Motociclistas e App de Despacho com Repasse Instantâneo PIX"
                  className="w-full h-auto object-cover group-hover:scale-105 transition-transform duration-700"
                />
              </div>
            </div>
          </div>

          {/* Motociclistas */}
          <div className="grid grid-cols-1 lg:grid-cols-12 gap-12 items-center">
            <div className="lg:col-span-6 order-2 lg:order-1 space-y-4">
              <div className="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-4">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-bold text-amber-400 tracking-wider uppercase bg-amber-950/60 px-3 py-1 rounded-full border border-amber-800">
                    Para o Entregador
                  </span>
                  <span className="text-xs font-mono text-emerald-400 font-bold bg-emerald-950 px-2 py-0.5 rounded border border-emerald-800">
                    PIX Instantâneo
                  </span>
                </div>

                <h3 className="text-2xl font-bold text-white">
                  O valor integral da sua corrida no seu bolso.
                </h3>

                <p className="text-slate-300 text-sm leading-relaxed">
                  Sem punições por rejeitar corridas abusivas, sem retenção de saldo por 15 dias e sem intermediários comendo 30% da sua gasolina.
                </p>

                <div className="grid grid-cols-2 gap-3 pt-2">
                  <div className="p-3 rounded-xl bg-slate-950 border border-slate-800 text-center">
                    <div className="text-emerald-400 font-black text-xl">100%</div>
                    <div className="text-slate-400 text-xs">Repasse do Frete</div>
                  </div>
                  <div className="p-3 rounded-xl bg-slate-950 border border-slate-800 text-center">
                    <div className="text-cyan-400 font-black text-xl">36 km/h</div>
                    <div className="text-slate-400 text-xs">Pacing Calculado</div>
                  </div>
                </div>

                <Link
                  href="/register"
                  className="block text-center w-full py-3 rounded-xl bg-gradient-to-r from-amber-500 to-orange-600 hover:from-amber-400 hover:to-orange-500 text-slate-950 font-bold text-sm shadow-lg shadow-amber-500/20 transition-all"
                >
                  Cadastrar Como Entregador
                </Link>
              </div>
            </div>

            <div className="lg:col-span-6 order-1 lg:order-2 space-y-4">
              <h3 className="text-2xl sm:text-3xl font-extrabold text-white tracking-tight">
                Alocação inteligente que respeita quem está na rua.
              </h3>
              <p className="text-slate-300 text-base leading-relaxed">
                Nossa engine não usa linha reta euclidiana para escolher quem faz a entrega. A alocação considera a malha viária real de São Paulo, sentido de vias e acessos expressos, garantindo que você nunca receba pedidos do outro lado de um rio ou viaduto intransitável.
              </p>
              <div className="flex items-center gap-3 text-xs text-slate-400">
                <ShieldCheck className="w-5 h-5 text-cyan-400 shrink-0" />
                <span>Cadastro verificado com CNH, placa de moto e chave PIX de titularidade confirmada.</span>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Tech Stack Banner */}
      <section id="tecnologia" className="py-20 bg-slate-950">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 space-y-12">
          <div className="text-center max-w-3xl mx-auto space-y-3">
            <span className="text-xs font-bold text-blue-400 tracking-wider uppercase bg-blue-950/60 px-3 py-1 rounded-full border border-blue-800">
              Stack de Engenharia
            </span>
            <h2 className="text-3xl sm:text-4xl font-extrabold text-white tracking-tight">
              Construído com padrões de sistemas distribuídos modernos
            </h2>
            <p className="text-slate-400 text-sm">
              Sem polling ineficiente. Sem inconsistência eventual descontrolada.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            <div className="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-3">
              <div className="w-10 h-10 rounded-xl bg-cyan-950 text-cyan-400 flex items-center justify-center border border-cyan-800">
                <Cpu className="w-5 h-5" />
              </div>
              <h3 className="font-bold text-white text-base">Transactional Outbox</h3>
              <p className="text-xs text-slate-400 leading-relaxed">
                Garantia estrita de que nenhum evento é perdido: o pedido e o evento residem na mesma transação ACID do Postgres.
              </p>
            </div>

            <div className="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-3">
              <div className="w-10 h-10 rounded-xl bg-blue-950 text-blue-400 flex items-center justify-center border border-blue-800">
                <Layers className="w-5 h-5" />
              </div>
              <h3 className="font-bold text-white text-base">RabbitMQ Broker</h3>
              <p className="text-xs text-slate-400 leading-relaxed">
                Mensageria de alta performance desacoplando a ingestão do cálculo pesado de rotas do simulador.
              </p>
            </div>

            <div className="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-3">
              <div className="w-10 h-10 rounded-xl bg-sky-950 text-sky-400 flex items-center justify-center border border-sky-800">
                <Radio className="w-5 h-5" />
              </div>
              <h3 className="font-bold text-white text-base">ConnectRPC Streams</h3>
              <p className="text-xs text-slate-400 leading-relaxed">
                Protocolo gRPC/HTTP-2 moderno substituindo websockets frágeis com contratos protobuf tipados fim a fim.
              </p>
            </div>

            <div className="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-3">
              <div className="w-10 h-10 rounded-xl bg-emerald-950 text-emerald-400 flex items-center justify-center border border-emerald-800">
                <Navigation className="w-5 h-5" />
              </div>
              <h3 className="font-bold text-white text-base">MapLibre GL & OSRM</h3>
              <p className="text-xs text-slate-400 leading-relaxed">
                Renderização acelerada por GPU no navegador sincronizada com a malha viária aberta do OpenStreetMap.
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* Call to Action Banner */}
      <section className="py-16 bg-gradient-to-b from-slate-900 to-slate-950 border-t border-slate-800">
        <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 text-center space-y-8">
          <h2 className="text-3xl sm:text-4xl font-extrabold text-white tracking-tight">
            Pronto para testar a logística em tempo real?
          </h2>
          <p className="text-slate-400 max-w-2xl mx-auto text-base">
            Abra a demonstração com um clique, crie um pedido de entrega em São Paulo e veja a moto percorrer a rota curva pela malha viária ao vivo.
          </p>

          <div className="flex flex-wrap items-center justify-center gap-4">
            <Link
              href="/dashboard"
              className="px-8 py-4 rounded-xl font-bold text-white bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 shadow-xl shadow-cyan-500/25 flex items-center gap-2 transition-all hover:scale-105"
            >
              <Zap className="w-5 h-5" />
              <span>Acessar Dashboard Operacional</span>
            </Link>

            <Link
              href="/register"
              className="px-8 py-4 rounded-xl font-bold text-slate-200 bg-slate-900 hover:bg-slate-800 border border-slate-700 flex items-center gap-2 transition-all"
            >
              <span>Cadastrar Novo Usuário</span>
            </Link>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t border-slate-800/80 py-8 px-4 sm:px-6 lg:px-8 text-center text-xs text-slate-500">
        <div className="max-w-7xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-4">
          <div className="flex items-center gap-2">
            <div className="w-6 h-6 rounded-lg bg-cyan-600 flex items-center justify-center text-white font-bold text-xs">
              V
            </div>
            <span className="font-semibold text-slate-400">VELOXLOGISTICS</span>
            <span>• Projeto Integrador IV</span>
          </div>

          <div className="flex items-center gap-4 text-slate-400">
            <Link href="/" className="hover:text-cyan-400">Home</Link>
            <Link href="/login" className="hover:text-cyan-400">Login</Link>
            <Link href="/register" className="hover:text-cyan-400">Cadastro</Link>
            <Link href="/dashboard" className="hover:text-cyan-400">Dashboard</Link>
          </div>
        </div>
      </footer>
    </div>
  );
}
