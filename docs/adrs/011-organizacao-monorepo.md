# ADR-011: Organização do Código em Monorepo

## Status
Aceito

## Contexto
O projeto consiste em um Backend escrito em Go e um Frontend escrito em Next.js (TypeScript). Com a adoção do **Protocol Buffers** e **Connect RPC** (ADR-010), ambas as pontas do sistema dependem visceralmente do mesmo arquivo de contrato (a fonte única de verdade: os arquivos `.proto`).

Se dividirmos o projeto em múltiplos repositórios (Multi-repo), teríamos que lidar com o complexo versionamento e sincronização dos arquivos `.proto`. O Backend poderia gerar um contrato na versão `v1.2` e o Frontend poderia estar apontando para a `v1.1`, gerando quebras em tempo de execução ou exigindo a publicação de bibliotecas via NPM e Go Modules privados (excesso de complexidade operacional para um projeto acadêmico).

## Decisão
Adotaremos a estratégia de **Monorepo**. Todo o ecossistema do Motor Logístico viverá dentro deste único repositório Git.

A estrutura de diretórios raiz será dividida da seguinte forma:

- `/proto`: Contém os arquivos agnósticos `.proto` e as configurações do Buf.
- `/api`: O *Monolito Modular* em Go (Backend).
- `/web`: O painel do lojista em Next.js (Frontend).
- `/docs`: Documentações arquiteturais e de decisão.
- `docker-compose.yml`: Infraestrutura comum (Postgres, RabbitMQ, Redis).

## Consequências

**Positivas:**
- **Sincronização Perfeita:** Quando uma Pull Request propõe uma alteração no arquivo `.proto`, a mesma PR já conterá a implementação no Backend em Go e a atualização no cliente TypeScript. O *build* de CI testará tudo atomicamente.
- **Onboarding Facilitado:** Com apenas um `git clone` e um `docker-compose up`, o novo desenvolvedor tem a plataforma inteira rodando na sua máquina, sem precisar caçar repositórios satélites.
- **Refatoração Global:** O *Lefthook* garantirá o *Lint* simultâneo de ambas as linguagens.

**Negativas / trade-offs:**
- **Histórico de Commits Misturado:** O `git log` conterá commits tanto de Frontend quanto de Backend (mitigado pelo uso estrito de Conventional Commits, ex: `feat(api): ...` ou `fix(web): ...`).
- **Pipeline de CI mais Lento:** O GitHub Actions precisará baixar dependências do Node e do Go no mesmo pipeline (mitigado pelo uso de *paths-filter* no CI, rodando testes de TS apenas se a pasta `/web` for alterada).
