# Domínio: Segurança Zero Trust (Security)

Cobre o requisito não-funcional de segurança como prioridade não-negociável, geofencing/PIN na confirmação de entrega, prova de vida (liveness), o fluxo de suspeita de fraude, e a barreira de onboarding do entregador.

---

## RNF09 — Segurança é Requisito Não-Negociável

> Segurança é requisito não-funcional obrigatório em todas as fases do projeto, incluindo o MVP. Identidade do entregador, geofencing, idempotência financeira e prevenção de fraude nunca são escopo cortável por prazo ou custo. Qualquer redução de escopo sob pressão de cronograma deve preservar integralmente este documento.

## Onboarding do Entregador (Barreira de Entrada)

- Antes de qualquer possibilidade de ficar `ONLINE` (ver FSM em `dispatch-fleet.md`), o cadastro inicial exige: CNH válida (foto + dados), documento do veículo (CRLV), e uma **selfie de referência**.
- Validação automática básica de formato/vencimento da CNH, seguida de **aprovação manual** (fila de revisão) antes da ativação da conta — não é self-service instantâneo, dado o RNF09.
- A selfie de onboarding é o padrão-ouro contra o qual toda verificação de liveness futura (abaixo) é comparada.

## Prova de Vida (Liveness)

- Selfie *in-app* (câmera nativa, proibido upload de galeria) **obrigatória em toda transição para `ONLINE`**, sem exceção de valor de pedido — contas alugadas são um risco de identidade que independe do valor da corrida aceita.
- Verificação periódica aleatória durante turnos longos (ex: a cada 4h ou N corridas), mitigando troca de motorista após o check-in inicial.
- Upload mudo para bucket (S3); o lojista visualiza CNH lado a lado com a foto atual no dashboard antes da coleta.

## Confirmação de Entrega (Geofencing + PIN)

- O botão "Confirmar Entrega" fica invisível até duas barreiras serem transpostas:
  1. **Geofencing:** GPS do entregador deve provar estar em raio de 50 metros da coordenada do cliente (PostGIS/Haversine).
  2. **PIN Antifraude:** PIN de 4 dígitos fornecido pelo cliente final, inserido pelo entregador para finalizar a entrega.
- Bloqueio por suspeita de fraude após 3 tentativas incorretas de PIN.
- **Paradoxo do Geofencing vs. Realidade Física (Urban Canyon):** Em áreas densas (prédios altos, condomínios), o GPS do celular sofre refração e pode oscilar. Para evitar travar a entrega (Graceful Degradation): se o entregador inserir o **PIN correto**, mas o GPS estiver fora da zona estrita de 50m (mas dentro de um limite flexível de **até 300m**), o sistema aceita a entrega, mas grava uma *flag de desvio de rota* na tabela do pedido para auditoria e impacto leve no score do entregador, se reincidente.
- O mesmo princípio de geofencing se aplica ao check-in na loja (`AT_STORE`, SLA de no-show — ver `order-lifecycle.md`): check-in autodeclarado sem prova de GPS não é aceito.

## Fluxo de Suspeita de Fraude (`SUSPECTED_THEFT`)

- Cancelamento pelo entregador após `IN_TRANSIT` (mercadoria já recolhida) aciona `SUSPECTED_THEFT`.
- A conta do entregador é **suspensa automaticamente e de forma imediata**, bloqueada para novas corridas — não é apenas um alerta passivo.
- Aciona fila de revisão manual por um operador, com notificação imediata ao lojista e ao cliente.
- Mesma estrutura operacional (fila de revisão manual) é reaproveitada pelo fluxo de disputa pós-entrega (`UNDER_DISPUTE`, ver `payment.md`).

## Idempotência como Requisito de Segurança

- Idempotência do webhook de pagamento (ver `payment.md`) e das ações críticas de aceite/conclusão (ver `order-lifecycle.md`) são tratadas aqui como extensão do RNF09: falhas de idempotência abrem superfícies de fraude ou dupla cobrança, não são apenas bugs de engenharia.
