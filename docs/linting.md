# Padrões de Código e Linting

Este documento estabelece as regras de análise estática (Linting) e formatação para garantir qualidade, segurança e uniformidade no código de toda a plataforma, do frontend ao banco de dados.

## 1. Backend (Golang)

O padrão oficial da comunidade Go é rigoroso. Utilizaremos o **`golangci-lint`** como orquestrador, configurado através de um arquivo `.golangci.yml` na raiz.

### Principais Linters Ativos:
- **`errcheck`:** Quebra o build se houver retorno de erro ignorado (ex: chamar uma função do banco e não validar o `error`).
- **`gosec`:** Varredura de segurança (Security Scanner) procurando por vulnerabilidades comuns (SQL Injection, vazamento de credenciais, geração fraca de tokens).
- **`revive` / `stylecheck`:** Garante o estilo idiomático do Go, forçando comentários em funções exportadas e nomenclatura correta de variáveis.
- **`goimports` / `gofumpt`:** Vai além do `gofmt` clássico, organizando os imports alfabeticamente e separando pacotes da *standard library* dos pacotes de terceiros.
- **`gocyclo` / `errname`:** Limita a complexidade ciclomática (funções gigantes cheias de `if/else`) e força o padrão idiomático para nomes de erros (ex: `ErrNotFound`).

**Como rodar localmente:**
```bash
# Executa a verificação completa no backend
make lint-go
```

## 2. Frontend (Next.js / TypeScript)

Para o código cliente, unimos a análise estática rigorosa do TypeScript com o Prettier para evitar discussões sobre estilo (tabs vs spaces).

### Ferramentas Ativas:
- **`ESLint`:** Usando a configuração rigorosa `eslint-config-next` + `@typescript-eslint/recommended`.
- **`Prettier`:** Formatação de código imposta automaticamente antes do commit (no-tabs, trailing commas, single quotes).
- **`tsc --noEmit`:** Checagem estrita de tipos no TypeScript, impedindo o build caso existam propriedades não mapeadas no React ou chamadas RPC inválidas.

**Como rodar localmente:**
```bash
# Executa o linting e tenta corrigir problemas de formatação automaticamente
npm run lint -- --fix
```

## 3. Contratos de API (Protobuf)

Sendo o Protobuf a nossa Fonte Única de Verdade (ADR-010), ele também precisa de regras estritas para evitar "espaguete de contratos". Utilizaremos o utilitário do **Buf**.

### Verificações do Buf (`buf.yaml`):
- **`buf lint`:** Garante que os arquivos `.proto` sigam o [Guia de Estilo do Protobuf](https://protobuf.dev/programming-guides/style/), como `PascalCase` para Mensagens e Serviços, e `snake_case` para os campos. Impede a mistura de padrões.
- **`buf breaking`:** Extremamente crítico. Compara a versão atual do `.proto` com a branch `main`. Se houver deleção de campos, troca de tipos (ex: `int32` para `string`), ou remoção de RPCs, **o CI quebra**, impedindo que a equipe de backend introduza código que quebraria os clientes antigos ou o App Mobile.

**Como rodar localmente:**
```bash
# Executa lint de estilo e verifica mudanças destrutivas
make lint-proto
```

## 4. Repositório e Integração Contínua (CI)

- **Lefthook (Git Hooks):** A trava oficial do projeto. É uma ferramenta ultrarrápida escrita em Go para gerenciar *git hooks*. Configuramos o `lefthook.yml` para rodar o linting e os testes paralelamente durante o `git commit`. Se o código estiver sujo ou com testes quebrados, **o commit é bloqueado na sua máquina**.
- **EditorConfig (`.editorconfig`):** Arquivo raiz definindo uso universal de *LF (Line Feed)* e tamanho de indentação.
- **GitHub Actions:** O pipeline de CI agirá como a última camada de defesa. O *Merge* de um Pull Request será bloqueado se os linters ou testes falharem na esteira.
