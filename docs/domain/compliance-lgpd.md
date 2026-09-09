# Domínio: Compliance e Proteção de Dados (LGPD)

O sistema coleta geolocalização contínua do entregador, biometria facial (liveness — ver `security-zero-trust.md`) e documento de identidade (CNH) — dados sensíveis sob a Lei Geral de Proteção de Dados (Lei 13.709/2018), não apenas boa prática de segurança operacional.

---

## Base Legal e Consentimento

- No onboarding do entregador (ver `security-zero-trust.md`), consentimento explícito e granular é coletado separadamente para:
  (a) uso de geolocalização contínua durante turnos `ONLINE`;
  (b) uso de biometria facial (liveness) e documento (CNH) para verificação de identidade.
- Consentimento registrado com timestamp e versão dos termos aceitos.

## Retenção e Expurgo de Dados Sensíveis

- **Selfies de liveness:** retenção limitada (ex: 90 dias), suficiente para auditoria de fraude, com expurgo automático depois — exceto selfies vinculadas a um caso aberto em `SUSPECTED_THEFT` ou `UNDER_DISPUTE` (ver `security-zero-trust.md`/`payment.md`), retidas até o encerramento do caso.
- **CNH:** retida enquanto a conta do entregador estiver ativa; expurgada dentro de um prazo definido após desativação da conta, salvo obrigação legal de retenção mais longa.

## Minimização de Dados do Cliente Final

- O telefone do cliente (capturado no RF01, ver `order-lifecycle.md`) é usado exclusivamente para o fluxo de pagamento e comunicação daquele pedido específico — não reaproveitado para marketing ou repassado a terceiros sem novo consentimento explícito.

## Direitos do Titular

- Canal definido (ex: e-mail de contato/dashboard) para entregadores e clientes finais solicitarem acesso, correção ou exclusão dos próprios dados pessoais, respeitando os prazos da LGPD para resposta.
- Portal de autoatendimento completo é opcional para o MVP; o processo manual de atendimento a essas solicitações não é.
