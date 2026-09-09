# Regras de Negócio Core (Domain Rules)

> **⚠️ AVISO DE MIGRAÇÃO:**
> Este documento monolítico foi **decomposto** em áreas de domínio especializadas para facilitar a manutenção, conforme plano de arquitetura. As regras de negócio agora vivem de forma granular e independente.

Por favor, consulte os documentos específicos por domínio:

1. **[Ciclo de Vida do Pedido (Order Lifecycle)](domain/order-lifecycle.md):** Máquina de estados, RF01–RF05, aceite concorrente, múltiplos itens.
2. **[Pagamento (Payment)](domain/payment.md):** Modelo financeiro, idempotência de webhook, estorno/disputa.
3. **[Despacho e Frota (Dispatch & Fleet)](domain/dispatch-fleet.md):** Fan-out, FSM do entregador, mitigação de Dead Miles, raio máximo.
4. **[Segurança Zero Trust (Security)](domain/security-zero-trust.md):** RNF09, geofencing/PIN, liveness, fluxo de suspeita de roubo.
5. **[Compliance e LGPD (Data Protection)](domain/compliance-lgpd.md):** Base legal, retenção e expurgo de dados sensíveis.
6. **[Reputação e Precificação (Reputation & Pricing)](domain/reputation-pricing.md):** Score simétrico (lojista e entregador), cálculo de frete dinâmico.

*O referencial teórico completo do MVP do Motor Logístico encontra-se 100% estabilizado nas documentações acima.*
