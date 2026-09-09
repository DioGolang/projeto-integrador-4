# ADR-017: Proteção contra Falhas de Terceiros (Circuit Breaker)

## Status

Aceito

## Contexto

A arquitetura do motor logístico depende de integrações com APIs externas de terceiros para o seu funcionamento completo. O ADR-007 delega a criação do pagamento pré-despacho para um gateway (PSP). Outras dependências futuras podem incluir disparo de SMS/WhatsApp e APIs de roteamento (ex: OSRM/Google Maps). 
Se a API do PSP sofrer uma instabilidade (ex: demorar 30 segundos para responder com um *Timeout*), e o lojista continuar clicando em "Criar Pedido", o nosso backend abrirá centenas de conexões bloqueantes. Em Go, cada requisição prenderá uma *Goroutine* e um socket TCP por 30 segundos. Em minutos, o servidor esgotará seus recursos (CPU, RAM, Sockets) sofrendo um colapso total por Efeito Dominó (*Cascading Failure*).

## Decisão

Toda integração síncrona com APIs externas (HTTP/gRPC) deverá obrigatoriamente ser encapsulada pelo padrão **Circuit Breaker** (Disjuntor), utilizando implementações consagradas no ecossistema Go, como o pacote `sony/gobreaker`.

1. **Configuração de Limiares:** O disjuntor será configurado com limites de falha (ex: 5 falhas consecutivas ou 20% de taxa de erro em uma janela de tempo).
2. **Abertura do Circuito (Fast Failure):** Se o limite for ultrapassado, o disjuntor "abre". Novas chamadas à API externa serão rejeitadas imediatamente (falha rápida), devolvendo um erro amigável ao cliente (ex: "Serviço de pagamento temporariamente indisponível") sem sequer tentar a requisição de rede. Isso poupa as *Goroutines* e portas TCP da nossa infraestrutura.
3. **Half-Open e Auto-Recuperação:** Após o tempo de resfriamento (*cooldown*), o disjuntor passa para o estado "Meio-Aberto" e tenta deixar passar uma requisição de teste. Se for sucesso, o circuito fecha (opera normalmente). Se falhar, reabre.

## Consequências

**Positivas:**
- Resiliência estrutural: a lentidão ou queda de um sistema terceiro não consegue derrubar a nossa infraestrutura.
- Melhor experiência para o usuário final em caso de instabilidade, que recebe um feedback imediato em vez de ficar esperando uma tela de *loading* infinita.

**Negativas / trade-offs:**
- Requer *tuning* cuidadoso de *timeouts* e limites de falha, para que o disjuntor não abra prematuramente durante oscilações normais de rede (*flaps*).
- Adiciona uma leve sobrecarga matemática em memória para manter o contador de sucesso/falhas.
