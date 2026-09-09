# Contratos de API (Protobuf)

Este documento define a interface de comunicação entre o Frontend (Next.js/Mobile) e o Backend (Go). Em vez de REST/JSON, nossa fonte da verdade é o **Protocol Buffers** usando **Connect RPC**.

Os contratos abaixo representam os arquivos `.proto` que estarão na raiz do projeto (ex: na pasta `proto/logistics/v1/`).

*Nota de Segurança (ADR-008):* O Connect RPC em Go e TS permite passagem de *interceptors* (semelhante a middlewares). O token JWT continuará sendo passado no Header de Autorização (`Authorization: Bearer <token>`) de forma transparente pelo client.

## Arquivo de Definição Principal: `api.proto`

```protobuf
syntax = "proto3";
package logistics.v1;
option go_package = "projeto-integrador-4/gen/logistics/v1";

// O Serviço Principal do Motor Logístico
service LogisticsService {
  // RF01: Criação de pedido pelo lojista (Unary RPC)
  rpc CreateOrder(CreateOrderRequest) returns (CreateOrderResponse);
  
  // RF03: Aceite de corrida pelo entregador (Unary RPC)
  rpc AcceptDelivery(AcceptDeliveryRequest) returns (AcceptDeliveryResponse);
  
  // RF06: Atualização de localização (Unary RPC)
  // Pode ser convertido para Client Streaming no futuro se a frota crescer muito
  rpc UpdateLocation(UpdateLocationRequest) returns (UpdateLocationResponse);
  
  // RF04/RNF06: Atualização em tempo real (Server Streaming RPC)
  // Substitui a abordagem de SSE descrita anteriormente
  rpc StreamOrderStatus(StreamOrderStatusRequest) returns (stream StreamOrderStatusResponse);
}

// ---------------------------------------------------------
// Mensagens: Criação de Pedidos
// ---------------------------------------------------------

message CreateOrderRequest {
  CustomerInfo customer = 1;
  Coordinates destination = 2;
  repeated OrderItem items = 3;
  PackageSize package_size = 4;
}

enum PackageSize {
  PACKAGE_SIZE_UNSPECIFIED = 0;
  PACKAGE_SIZE_SMALL = 1;  // Cabe na bag de bicicleta
  PACKAGE_SIZE_LARGE = 2;  // Exige moto (ex: múltiplas pizzas)
}

message CustomerInfo {
  string name = 1;
  string phone = 2; // Obrigatório para o checkout hospedado (payment.md)
}

message Coordinates {
  double lat = 1;
  double lng = 2;
  string address = 3; // Opcional, para complementar
}

message OrderItem {
  string name = 1;
  int32 quantity = 2;
  double unit_price = 3;
}

message CreateOrderResponse {
  string order_id = 1;
  string status = 2;
  double delivery_fee = 3; // Frete calculado pelo backend (reputation-pricing.md)
  string created_at = 4; // ISO 8601 ou google.protobuf.Timestamp
}

// ---------------------------------------------------------
// Mensagens: Aceite de Entrega
// ---------------------------------------------------------

message AcceptDeliveryRequest {
  string order_id = 1;
  // deliverer_id vem do JWT, logo não pertence ao request body!
}

message AcceptDeliveryResponse {
  bool success = 1;
  string status = 2;
  string accepted_at = 3;
}

// ---------------------------------------------------------
// Mensagens: Localização de Frota
// ---------------------------------------------------------

message UpdateLocationRequest {
  Coordinates current_position = 1;
  // deliverer_id extraído do JWT
}

message UpdateLocationResponse {
  bool success = 1;
}

// ---------------------------------------------------------
// Mensagens: Streaming de Status
// ---------------------------------------------------------

message StreamOrderStatusRequest {
  // O request abre a conexão. O merchant_id é lido do JWT se for lojista.
  // Pode aceitar filtros opcionais.
}

message StreamOrderStatusResponse {
  string order_id = 1;
  string status = 2; // "CREATED", "DISPATCHED", "ACCEPTED", etc.
  optional string deliverer_name = 3;
  string updated_at = 4;
}
```

## Consequências Arquiteturais (Substituindo HTTP REST)

1. **Geração de Código:** Ao rodar `buf generate`, o frontend receberá tipagens perfeitas e clientes Connect para fazer as chamadas, e o backend gerará a interface `LogisticsServiceHandler` que a nossa camada HTTP em Go será obrigada a implementar.
2. **Semântica:** Substituímos respostas ambíguas HTTP (ex: 409 Conflict vs 400 Bad Request) pelo modelo rico de erros do gRPC/Connect (`connect.CodeAlreadyExists`, `connect.CodeInvalidArgument`), trazendo previsibilidade e tratamento via `catch` nativo no TypeScript.
