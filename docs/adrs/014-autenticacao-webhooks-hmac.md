# ADR-014: Autenticação Server-to-Server para Webhooks (PSP)

## Status

Aceito

## Contexto

O ADR-007 estabelece o recebimento de webhooks do PSP (ex: Mercado Pago, Stripe) para confirmar pagamentos pré-despacho. O ADR-008 estabelece o uso de JWT para as rotas da API. No entanto, provedores de pagamento externos não possuem nosso JWT para se autenticarem. Deixar o endpoint `/webhooks/payment` aberto sem autenticação expõe a plataforma a ataques de injeção de falsos pagamentos.

## Decisão

O endpoint de recebimento de webhooks será **isento do middleware de JWT** e protegido exclusivamente pela validação de assinatura criptográfica (**HMAC - Hash-based Message Authentication Code**).

1. **Chave Compartilhada (Secret):** O provedor de pagamento fornecerá uma chave secreta (Webhook Secret) que será carregada no backend Go estritamente via variável de ambiente (prática *12-Factor App*).
2. **Validação do Payload no Go:**
   - O middleware de webhook lerá os *Headers* da requisição (ex: `x-signature`).
   - O backend lerá o corpo bruto (*Raw Body* em bytes) da requisição HTTP.
   - Usando a biblioteca padrão `crypto/hmac` e `crypto/sha256`, o Go aplicará a chave secreta sobre os bytes do corpo para gerar um hash.
   - O request só será processado se o hash gerado pelo Go for idêntico ao hash enviado pelo PSP no *Header*.

## Consequências

**Positivas:**
- Elimina a vetor de ataque de falsificação de pagamentos (Zero Trust externo).
- Não exige whitelisting de IPs (que frequentemente mudam na infraestrutura de nuvem dos provedores de pagamento).

**Negativas / trade-offs:**
- **Armadilha do Golang (Gotcha):** Em Go, a leitura do `r.Body` em bytes consome o *stream* de dados. Após calcular o HMAC, o desenvolvedor é obrigado a recriar o buffer (`r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))`) para que o handler subsequente consiga fazer o `json.Unmarshal`. Se esquecer disso, o parse do JSON falhará com erro de EOF (*End of File*).
