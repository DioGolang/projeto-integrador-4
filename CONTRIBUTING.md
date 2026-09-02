# Guia de Contribuição (Contributing)

Bem-vindo(a) ao repositório do **Motor Logístico**! 

Para mantermos a consistência da arquitetura que desenhamos para o projeto, centralizaremos as entregas via Pull Requests (PRs). Este documento define o fluxo de trabalho para adicionar *features* e corrigir *bugs* de forma organizada.

> **Importante:** Como dividimos as tarefas, combinamos que farei a revisão final e o merge das PRs na branch `main`. Isso garante que tudo continue rodando redondo para a nossa apresentação!

---

## 1. Padrão de Nomenclatura de Branches

Nunca comite diretamente na branch `main`. Todo trabalho deve ser feito em uma branch temporária seguindo o padrão abaixo (kebab-case):

- **Features (Novas funcionalidades):** `feat/descricao-curta`
  - *Exemplo:* `feat/add-order-creation`
- **Correções (Bugfixes):** `fix/descricao-do-bug`
  - *Exemplo:* `fix/radius-calculation`
- **Tarefas Técnicas / Refatorações:** `chore/descricao-da-tarefa`
  - *Exemplo:* `chore/update-lefthook`
- **Documentação:** `docs/descricao`
  - *Exemplo:* `docs/add-api-contracts`

---

## 2. Padrão de Commits

Utilizamos a convenção do **[Conventional Commits](https://www.conventionalcommits.org/)** combinada com **Gitmojis** (opcional, mas recomendado) para facilitar a leitura do histórico.

**Estrutura:** `<tipo>: <descrição breve em minúsculas>`

**Tipos permitidos:**
- `feat:` (nova feature)
- `fix:` (correção de bug)
- `docs:` (documentação)
- `chore:` (atualização de ferramentas, dependências, etc)
- `test:` (adição ou correção de testes)
- `refactor:` (refatoração de código sem impacto de negócio)

**Exemplos aceitos:**
- `:sparkles: feat: adiciona endpoint de aceite de corrida`
- `:bug: fix: corrige null pointer na geolocalização`
- `:wrench: chore: atualiza dependências do connect-rpc`

---

## 3. Fluxo de Trabalho (O Passo a Passo)

Siga este fluxo rigorosamente ao iniciar uma nova tarefa:

1. **Atualize sua máquina:**
   ```bash
   git checkout main
   git pull origin main
   ```
2. **Crie a branch seguindo o padrão:**
   ```bash
   git checkout -b feat/sua-feature
   ```
3. **Desenvolva e Respeite o Lefthook:**
   - Faça suas alterações no código.
   - Antes do commit, o nosso hook (`lefthook.yml`) vai interceptar a ação. Ele exige que o código passe no **Lint** e nos **Testes**.
   - Para evitar surpresas, sempre rode manualmente antes:
     ```bash
     make lint-go
     make test
     ```
4. **Comite o código:**
   ```bash
   git add .
   git commit -m ":sparkles: feat: sua feature adicionada"
   ```
5. **Faça o push para o repositório remoto:**
   ```bash
   git push origin feat/sua-feature
   ```
6. **Abra o Pull Request (PR):**
   - Acesse o GitHub e abra o PR apontando para a `main`.
   - Adicione uma descrição clara do que foi feito e do porquê.
   - Aguarde a minha avaliação e aprovação para o merge.

---

## 4. Regras de Ouro do Pull Request

Para garantir que seu PR seja aprovado rapidamente:
- **Mantenha-o pequeno:** PRs gigantescos (ex: +1500 linhas) demoram para ser revisados e têm alta chance de conter bugs ocultos. Quebre entregas grandes em PRs menores.
- **Não ignore testes:** Se você criou uma regra de domínio no backend, anexe o teste unitário (cobertura exigida no pacote `domain` é de 100%).
- **Cuidado com I/O:** Nunca adicione queries de banco na camada de HTTP ou de Negócio. Respeite as regras descritas no [Guia de Arquitetura](docs/architecture.md).
