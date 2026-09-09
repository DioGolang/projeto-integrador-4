# ADR-016: Gestão de Segredos e Configurações (12-Factor App)

## Status

Aceito

## Contexto

O sistema possui chaves críticas de infraestrutura e negócio: chave privada para assinar o JWT, credenciais do PostgreSQL, senhas do cluster RabbitMQ e chaves de API do gateway de pagamento (PSP Secret). Se essas chaves forem fixadas no código ("hardcoded") ou comitadas no repositório Git, o projeto falhará em requisitos básicos de segurança, expondo a arquitetura a vazamentos graves.

## Decisão

Adotaremos a metodologia **12-Factor App** para a gestão estrita de configurações e segredos.

1. **Injeção via Ambiente:** Todas as credenciais, secrets e configurações mutáveis de ambiente (ex: URLs de conexão, thresholds) devem ser passadas obrigatoriamente via Variáveis de Ambiente (`ENV_VARS`).
2. **Abstração em Go:** Utilizaremos pacotes idiomáticos em Go (ex: `kelseyhightower/envconfig` ou `viper`) para mapear as variáveis de ambiente em estruturas (`structs`) fortemente tipadas durante o processo de *bootstrap* do aplicativo (`main.go`). Se uma variável obrigatória estiver ausente, a aplicação deve falhar imediatamente (`panic/log.Fatal`) no momento do boot (fail-fast), impedindo execuções em estado inconsistente.
3. **Infraestrutura Local:** O arquivo `docker-compose.yml` e um arquivo `.env` (ignorado pelo `.gitignore`) serão responsáveis por injetar as variáveis nos contêineres e serviços locais.

## Consequências

**Positivas:**
- Elimina o risco de exposição acidental de credenciais no repositório.
- A mesma imagem Docker da API pode ser promovida pelos ambientes (Staging, Produção) alterando apenas as variáveis injetadas.
- Código 100% agnóstico à origem da configuração.

**Negativas / trade-offs:**
- Complexidade adicional no *onboarding* de novos desenvolvedores, que precisarão de um `.env.example` claro para conseguir inicializar a infraestrutura local.
