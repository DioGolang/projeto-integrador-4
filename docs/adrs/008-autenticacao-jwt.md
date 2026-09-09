# ADR-008: Autenticação e Autorização baseada em JWT

## Status
Aceito

## Contexto
O sistema possui dois perfis principais de usuários (Atores) interagindo com a mesma API REST:
1. **Lojista (Merchant):** Acessa via painel Web (Next.js). Pode criar pedidos e acompanhar status (SSE).
2. **Entregador (Deliverer):** Acessa via App Mobile. Pode enviar localização e aceitar pedidos.

O design inicial dos contratos de API assumia a passagem de identificadores (`merchant_id`, `deliverer_id`) diretamente no payload (corpo do JSON). Isso representa uma falha de segurança grave (CWE-285: Improper Authorization), pois um usuário mal-intencionado poderia forjar requisições em nome de outros.

## Decisão
Adotamos **JSON Web Tokens (JWT)** como mecanismo unificado de Autenticação e Autorização (Stateless Security):

1. **Geração e Armazenamento:**
   - **Frontend Web (Next.js):** Tokens mantidos em cookies `HttpOnly` para mitigar ataques XSS.
   - **App Mobile:** Tokens armazenados no *Secure Storage* nativo do dispositivo e passados no header `Authorization: Bearer <token>`.

2. **Middleware Go (Contexto):**
   - Um middleware global interceptará as requisições protegidas, validará a assinatura (chave simétrica HMAC ou assimétrica RSA/JWKS) e extrairá as **Claims** (ex: `sub` que contém o UUID do usuário e `role` que contém "MERCHANT" ou "DELIVERER").
   - Esses dados serão injetados no `context.Context` do request Go.

3. **Remoção de Identificadores do Payload:**
   - Os handlers HTTP passarão a ler a "identidade" de quem faz a requisição diretamente do `context.Context`, ignorando ou removendo os campos de ID que antes vinham no JSON.

## Consequências

**Positivas:**
- **Segurança (Zero Trust):** Impossibilita que um entregador aceite uma corrida forjando o ID de outro entregador.
- **Escalabilidade (Stateless):** A API Go não precisa consultar o banco de dados em toda requisição apenas para validar se o token é válido, aliviando a carga do banco.
- **Segregação de Perfis (RBAC básico):** A presença da claim `role` permite bloquear rotas inteiras no middleware (ex: rejeitar entregador tentando acessar `POST /orders`).

**Negativas / trade-offs:**
- **Revogação de Tokens:** Sendo stateless, um JWT não pode ser revogado facilmente antes de expirar. Requer implementação de *Short-lived Access Tokens* (ex: 15 minutos) combinados com *Refresh Tokens*, aumentando a complexidade do cliente e da API.
- Requer ajuste nos contratos de API já definidos (remoção dos IDs do *Request Body*).
