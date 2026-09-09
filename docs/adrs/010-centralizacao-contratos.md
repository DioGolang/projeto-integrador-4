# ADR-010: Centralização de Contratos via Protobuf e Connect RPC

## Status
Aceito

## Contexto
Temos um backend em Go (fortemente tipado) e um frontend em Next.js (TypeScript). Garantir que ambos os lados utilizem as mesmas tipagens sem redundância e com alta performance é crucial. Avaliamos abordagens baseadas em JSON (OpenAPI/Swagger), porém buscamos uma alternativa superior em tipagem forte, versionamento e eficiência de rede (menor payload).

Além disso, nativamente o gRPC tradicional exige a configuração de um proxy (ex: Envoy) para rodar no browser (Next.js client-side), adicionando complexidade de infraestrutura.

## Decisão
Adotaremos **Protocol Buffers (Protobuf) + Connect RPC** como nossa fonte única de verdade e protocolo de comunicação.

1. **A Fonte da Verdade (`.proto`):** Todo o contrato da API (serviços, mensagens, streams) será estritamente definido em arquivos `.proto` gerenciados pela ferramenta **Buf**.
2. **Framework RPC (Connect):** Utilizaremos o [Connect RPC](https://connectrpc.com/) (mantido pela Buf). Diferente do gRPC tradicional, o Connect suporta 3 protocolos simultaneamente: gRPC, gRPC-Web e o protocolo Connect (HTTP/1.1 ou HTTP/2 com payloads JSON ou Binário).
3. **Backend (Go):** Geraremos os handlers via `protoc-gen-connect-go`. O Connect RPC no Go gera handlers estritos e roda sobre o `net/http` padrão, eliminando a dependência pesada do `grpc-go`.
4. **Frontend (Next.js):** Geraremos os clientes via `@connectrpc/connect-query`. Isso integra o contrato Protobuf diretamente com o **TanStack React Query**, permitindo buscar dados de forma tipada, reativa e sem proxies.
5. **Streaming Nativo:** A comunicação real-time (RF04/RNF06) utilizará **Server Streaming** do próprio RPC, substituindo a necessidade de implementarmos SSE (Server-Sent Events) manualmente, pois o cliente Connect lida com streams no browser de forma nativa e elegante.

## Consequências

**Positivas:**
- **Single Source of Truth de Alto Nível:** O arquivo `.proto` dita as regras. O código Go e TS é gerado a partir dele.
- **Alta Performance:** Payloads menores e serialização muito mais rápida (Protobuf) quando comparado a JSON/REST tradicional.
- **Integração Perfeita com React:** O gerador de TypeScript do Connect exporta hooks nativos para o React Query.
- **Dispensa Proxy:** Roda perfeitamente no browser sem Envoy.
- **Validação de Quebras:** A ferramenta `buf` possui `buf breaking`, garantindo no CI que uma mudança no contrato não quebrará clientes antigos.

**Negativas / trade-offs:**
- Curva de aprendizado inicial da sintaxe Protobuf para quem só trabalhou com REST/JSON.
- Depuração de payloads binários no Network Tab do Chrome exige plugins adicionais (embora o Connect suporte JSON via HTTP nativamente para facilitar o debug em dev).
