# ADR-018: Autenticação Avançada e Gestão de Sessões (Zero Trust)

## Status
Aceito

## Contexto
O nosso Motor Logístico exige um nível de segurança bancário (RNF09). Uma das maiores vulnerabilidades em sistemas de *gig economy* (aplicativos de entrega) é o aluguel de contas e o roubo de sessões (Account Takeover). 

O uso de um JWT tradicional, 100% *stateless* e de vida longa, é insuficiente. Se um token JWT for roubado, não temos como revogá-lo facilmente até que ele expire. Além disso, precisamos saber se um entregador está repentinamente logando de um dispositivo não reconhecido (indicativo de conta alugada ou dispositivo roubado).

## Decisão
Decidimos implementar uma arquitetura de identidade e gestão de sessões blindada baseada em estado parcial no banco de dados, compondo os seguintes pilares:

1. **Multi-Factor Authentication (MFA):**
   Suporte nativo a TOTP (ex: Google Authenticator). A chave secreta (`mfa_secret`) será armazenada na tabela principal de usuários (`users`).

2. **Gestão de Dispositivos (Device Fingerprinting):**
   Toda tentativa de login deve enviar um *hash* único do dispositivo (combinação de hardware, OS e browser/app). A tabela `devices` mapeará quais dispositivos pertencem a quais usuários. Dispositivos novos ou não confiáveis passarão por fluxos de segurança adicionais (ex: forçar o MFA ou um *Liveness Check* via selfie).

3. **Refresh Token Rotation (Com Hashing):**
   - O **Access Token** (JWT) será estritamente *stateless* e de vida curta (ex: 15 minutos).
   - O **Refresh Token** será opaco (uma string aleatória forte) e de vida longa, mas será rigorosamente **stateful**. 
   - Por segurança, **jamais armazenaremos o Refresh Token em texto puro** na tabela `sessions`. Armazenaremos apenas o hash criptográfico (`refresh_token_hash`). Se o banco vazar (SQL Injection, etc), os tokens inativos não poderão ser usados por um atacante.
   - Cada Refresh Token estará obrigatoriamente amarrado a um `device_id` específico. Se o token for usado por um IP ou fingerprint diferente, ele será revogado imediatamente (revogação em cascata).

4. **Tracking Comportamental (`user_login_state`):**
   Registraremos o último IP, User Agent e Timestamp de login na tabela primária para detectar mudanças bruscas de padrão (ex: login em São Paulo e 5 minutos depois login na Bahia).

## Consequências

- **Positivas:**
  - Capacidade de **Revogação Granular:** O administrador ou o próprio usuário pode desconectar apenas um dispositivo específico que foi roubado (ex: limpar remotamente a sessão do celular), mantendo a sessão do computador intacta.
  - Rastreabilidade perfeita em casos de disputa de fraude (comprovação via *Device Fingerprint* e IP).
  - Mitigação absoluta de roubo de Refresh Tokens através do armazenamento por Hash.
- **Negativas:**
  - **Aumento de I/O no Banco:** A cada 15 minutos (quando o Access Token expira), a aplicação fará um *hit* no Postgres (Tabela `sessions`) para validar o Refresh Token e rotacioná-lo. Isso exigiu a criação de índices otimizados em `refresh_token_hash` e `user_id` para não matar a performance do banco.
  - **Complexidade do Cliente:** O App Mobile e o Web frontend precisarão ter a lógica robusta para extrair o *Device Fingerprint* silenciosamente e gerenciar a rotação de tokens em plano de fundo de forma transparente para o entregador.
