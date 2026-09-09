# ADR-007: Pagamento Antecipado via Checkout Hospedado com Split (Pix + Cartão), sem Aplicativo do Cliente

## Status
Aceito

## Contexto

O modelo inicial previa pagamento presencial na entrega, com o entregador cobrando via Pix (QR estático) ou uma maquininha física emprestada do lojista. Essa abordagem expôs três problemas estruturais, não apenas de UX:

1. **Fraude de cartão na ponta:** o entregador tem acesso físico ao meio de cobrança na porta do cliente, abrindo espaço para valores adulterados, golpes de maquininha e disputas sem fonte de verdade auditável.
2. **Dead Miles:** uma maquininha pertencente ao lojista cria uma obrigação de retorno ao ponto de origem após cada entrega, anulando as estratégias de roteamento contínuo (chaining/heatmaps) descritas na seção de Dead Miles.
3. **Reconciliação manual do split** (quanto vai para o lojista, quanto vai para o entregador) fica sujeita a erro humano se depender de comprovantes soltos em vez de um fluxo automatizado.

Não há orçamento/escopo para desenvolver um aplicativo de cliente final no MVP, então a cobrança precisa acontecer sem exigir instalação de app pelo cliente.

## Decisão

O pagamento passa a ser **antecipado (pré-despacho)**, via **Checkout Hospedado** de um PSP com suporte a **split de pagamento (marketplace payment)** e suporte simultâneo a Pix e Cartão (ex: Mercado Pago, Pagar.me, Asaas — a ser confirmado em spike técnico).

Fluxo:
1. Lojista registra o pedido (RF01), agora incluindo obrigatoriamente o telefone do cliente.
2. O backend cria uma cobrança no PSP (Pix + Cartão habilitados) com split pré-configurado: uma fração para a conta do lojista, o valor do frete integralmente para a conta do entregador (ver 1.2 do documento de regras de negócio — split é resolvido pelo PSP, não pela aplicação).
3. O backend expõe o link/QR de duas formas, sem exigir app do cliente:
   - **Pedido de balcão:** QR Code exibido no tablet/monitor do lojista, cliente escaneia com a câmera nativa do celular (abre no navegador).
   - **Pedido remoto (telefone/WhatsApp do lojista):** link enviado via WhatsApp Business API (canal primário) com SMS como fallback.
4. O PSP notifica o backend via webhook (`payment.approved`) quando o pagamento é confirmado — Pix ou Cartão, mesmo contrato de evento.
5. Idempotência do webhook: `INSERT ... ON CONFLICT DO NOTHING` pelo identificador único da transação do PSP, exatamente como já especificado para Pix na seção 3.2 do documento de regras — agora cobrindo também cartão.
6. Só após `payment.approved`, o backend publica `OrderReadyForDispatch` via Outbox, acionando o Fan-out (ADR-001). Um pedido não pago nunca é visível para o motor de despacho nem para os entregadores.

O entregador deixa de portar qualquer meio de cobrança — físico ou digital. Ele apenas coleta e entrega.

## Consequências

**Positivas:**
- Elimina estruturalmente a fraude de cartão na ponta e o golpe de valor adulterado: o valor é travado no checkout no momento da criação do pedido, nunca digitado por um humano na porta do cliente.
- Resolve o Dead Miles por completo, sem necessidade de qualquer solução intermediária de custódia de equipamento — o entregador nunca carrega nada além do pedido.
- Split automático via PSP remove reconciliação manual entre lojista e entregador.
- PCI compliance do processamento de cartão fica inteiramente sob responsabilidade do PSP (checkout hospedado) — o backend nunca toca em dado de cartão.
- Reaproveita o padrão de idempotência de webhook já desenhado para Pix (ADR-005/seção 3.2), agora generalizado para qualquer método de pagamento.

**Negativas / trade-offs:**
- Introduz latência entre criação do pedido e despacho (`AWAITING_PAYMENT`), diferente do modelo "despacha na hora" original — decisão consciente, alinhada ao modelo operacional de players como iFood/Rappi.
- Depende de o cliente ter smartphone com internet e WhatsApp/SMS acessível. Casos sem esse acesso exigem um fluxo manual de exceção (pagamento em dinheiro registrado pelo lojista, fora do domínio de garantias digitais do sistema, com log de auditoria).
- Custo de mensageria via WhatsApp Business API (cobrança por conversa) — mitigado por SMS como fallback mais barato.
- Acopla o sistema a um PSP específico que suporte split simultâneo de Pix e Cartão — critério de seleção a ser validado em spike técnico antes da implementação.
- Necessita de um novo estado (`AWAITING_PAYMENT`) e um mecanismo de expiração (RF05 já cobre padrão similar para `DISPATCHED`; será replicado para pagamento não confirmado em tempo hábil).
