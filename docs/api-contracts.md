# Contratos de API (Protobuf)

Este documento define a interface de comunicação entre o Frontend (Next.js/Mobile) e o Backend (Go). Em vez de REST/JSON, nossa fonte da verdade é o **Protocol Buffers** usando **Connect RPC**.

Os contratos abaixo representam os arquivos `.proto` que estarão na raiz do projeto (ex: na pasta `proto/logistics/v1/` e `proto/auth/v1/`).

*Nota de Segurança (ADR-008 e ADR-018):* O Connect RPC em Go e TS permite passagem de *interceptors* (semelhante a middlewares). O token JWT de acesso e metadados de sessão continuarão sendo passados nos Headers de forma transparente pelo client, implementando um modelo **Zero Trust** com Multi-Factor Authentication e Device Fingerprinting.

## Arquivo de Definição Principal: `logistics/v1/api.proto`

```protobuf
syntax = "proto3";
package logistics.v1;

option go_package = "projeto-integrador-4/api/gen/logistics/v1";

import "google/protobuf/timestamp.proto";

// O Serviço Principal do Motor Logístico
service LogisticsService {
  // RF01: Criação de pedido pelo lojista (Unary RPC)
  rpc CreateOrder(CreateOrderRequest) returns (CreateOrderResponse);
  
  // RF03: Aceite de corrida pelo entregador (Unary RPC)
  rpc AcceptDelivery(AcceptDeliveryRequest) returns (AcceptDeliveryResponse);
  
  // RF06: Atualização de localização (Unary RPC)
  // Client Streaming futuro se a frota crescer muito
  rpc UpdateLocation(UpdateLocationRequest) returns (UpdateLocationResponse);
  
  // RF04/RNF06: Atualização em tempo real (Server Streaming RPC)
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
  string phone = 2; // Obrigatório para o checkout hospedado
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
  double delivery_fee = 3; // Frete calculado dinamicamente pelo backend
  google.protobuf.Timestamp created_at = 4; 
}

// ---------------------------------------------------------
// Mensagens: Aceite de Entrega
// ---------------------------------------------------------

message AcceptDeliveryRequest {
  string order_id = 1;
  // deliverer_id é injetado pelo backend via JWT Interceptor (Segurança Zero Trust)
}

message AcceptDeliveryResponse {
  bool success = 1;
  string status = 2;
  google.protobuf.Timestamp accepted_at = 3;
}

// ---------------------------------------------------------
// Mensagens: Localização de Frota
// ---------------------------------------------------------

message UpdateLocationRequest {
  Coordinates current_position = 1;
  // deliverer_id injetado via JWT
}

message UpdateLocationResponse {
  bool success = 1;
}

// ---------------------------------------------------------
// Mensagens: Streaming de Status
// ---------------------------------------------------------

message StreamOrderStatusRequest {
  // O request abre a conexão. 
  // O token JWT identifica o merchant_id ou deliverer_id.
}

message StreamOrderStatusResponse {
  string order_id = 1;
  string status = 2; // "CREATED", "DISPATCHED", "ACCEPTED", etc.
  optional string deliverer_name = 3;
  google.protobuf.Timestamp updated_at = 4;
}
```

## Arquivo de Definição de Autenticação: `auth/v1/auth.proto` (Implementação ADR-018)

```protobuf
syntax = "proto3";
package auth.v1;

option go_package = "projeto-integrador-4/api/gen/auth/v1";

import "google/protobuf/timestamp.proto";

// Serviço de Autenticação e Gestão de Sessões (Zero Trust)
service AuthService {
  // Login inicial (com suporte a Device Fingerprinting)
  rpc Login(LoginRequest) returns (LoginResponse);

  // Rotação do Refresh Token (Stateful & Hash-based)
  rpc RefreshToken(RefreshTokenRequest) returns (RefreshTokenResponse);

  // Revogação de sessão granular ou total
  rpc Logout(LogoutRequest) returns (LogoutResponse);

  // Fluxos de MFA (Multi-Factor Authentication)
  rpc SetupMFA(SetupMFARequest) returns (SetupMFAResponse);
  rpc VerifyMFA(VerifyMFARequest) returns (VerifyMFAResponse);
}

message LoginRequest {
  string email = 1;
  string password = 2;
  string device_fingerprint = 3; // Hash único do hardware/OS/browser
  string ip_address = 4;
}

message LoginResponse {
  string access_token = 1; // JWT stateless de vida curta
  string refresh_token = 2; // Token opaco de vida longa
  bool requires_mfa = 3; // True se for um device novo ou usuário com MFA ativado
  google.protobuf.Timestamp expires_at = 4;
}

message RefreshTokenRequest {
  string refresh_token = 1;
  string device_fingerprint = 2; // Validação estrita do device amarrado ao token
}

message RefreshTokenResponse {
  string access_token = 1;
  string refresh_token = 2;
  google.protobuf.Timestamp expires_at = 3;
}

message LogoutRequest {
  // Se device_id não for informado, a revogação ocorre para a sessão do token atual
  optional string device_id = 1; 
}

message LogoutResponse {
  bool success = 1;
}

message SetupMFARequest {
  // Inicializa o processo de TOTP
}

message SetupMFAResponse {
  string secret = 1;
  string qr_code_url = 2;
}

message VerifyMFARequest {
  string code = 1; // Código de 6 dígitos gerado pelo Authenticator
  string device_fingerprint = 2;
}

message VerifyMFAResponse {
  string access_token = 1;
  string refresh_token = 2;
  google.protobuf.Timestamp expires_at = 3;
}
```

## Consequências Arquiteturais (Substituindo HTTP REST)

1. **Geração de Código:** Ao rodar `buf generate`, o frontend receberá tipagens perfeitas e clientes Connect para fazer as chamadas, e o backend gerará as interfaces (`LogisticsServiceHandler`, `AuthServiceHandler`) que a nossa camada HTTP em Go será obrigada a implementar.
2. **Semântica de Erros:** Substituímos respostas ambíguas HTTP (ex: 409 Conflict vs 400 Bad Request) pelo modelo rico de erros do gRPC/Connect (`connect.CodeAlreadyExists`, `connect.CodeInvalidArgument`), trazendo previsibilidade e tratamento via `catch` nativo no TypeScript.
3. **Segurança Avançada:** A adição do `AuthService` concretiza as diretrizes da **ADR-018**, habilitando tokens de curta duração, Rotação de Refresh Tokens estritamente acoplados aos Fingerprints dos dispositivos, e revogação em tempo real (Mitigação de Account Takeover).
