# Git_Rotina_Versionamento.md — Rotina Diária de Versionamento

> **Projeto:** MedidorTokens — Extensão MedidorTokens (Antigravity IDE 2.6.0+)  
> **Repositório:** https://github.com/rogeriocenteno/MedidorTokens (**PÚBLICO**)  
> **Identidade Git obrigatória:** `Rogério Centeno <rogeriocenteno@users.noreply.github.com>`  
> **Branch de trabalho:** `develop` (sempre trabalhar nesta branch)  
> **Branch de produção/estável:** `main` (atualizar apenas em marcos importantes — fim de etapa ou nova versão `.vsix` validada)

---

## 📋 Verificação Inicial (Boa Prática)

Antes de começar a trabalhar, verifique em qual branch está:

```powershell
git status
git branch
```

Você deve estar sempre na branch `develop`. Caso não esteja:

```powershell
git checkout develop
```

---

## ✅ Rotina ao Final de Cada Etapa de Desenvolvimento

### Passo 1 — Ver o que foi alterado
```powershell
git status
```

### Passo 2 — Adicionar as alterações
```powershell
git add .
```

### Passo 3 — ⚠️ CONFIRMAR o que será commitado (OBRIGATÓRIO)
```powershell
git diff --stat --cached
```
> Revise a lista de arquivos. Confirme que **nenhum arquivo sensível ou temporário** está incluído e que **não há vazamento de dados privados para o repositório público**:
> - `credenciais.md` / `.env` — credenciais e variáveis sensíveis
> - `backup_altimeter_0.0.4/` / `*.bak*` — backups locais incrementais
> - `altimeter-*.vsix` / `medidortokens-0.0.*.vsix` — pacotes legados
> - `Logo*.jpg` / `transcript*.jsonl` / `*.log` — mídias locais e logs de sessão
> - Caminhos absolutos da máquina local (`C:\Users\<usuario>\...`, drives locais), e-mails pessoais reais ou menções a outros projetos privados

#### Auditoria Pré-Commit (Obrigatória em Repositório Público)
```powershell
node -e "const fs=require('fs'),cp=require('child_process'); const files=cp.execSync('git diff --cached --name-only').toString().trim().split(/\r?\n/).filter(Boolean); const re=/[a-z0-9._%+-]+@gmail\.com|Users[\\\/][a-z0-9_-]+/i; console.log(files.filter(f=>fs.existsSync(f)&&!f.endsWith('.vsix')&&!f.endsWith('.png')&&re.test(fs.readFileSync(f,'utf8'))));"
```
*(Deve retornar `[]` antes de prosseguir com o `git commit`).*

### Passo 4 — Commitar com mensagem descritiva
```powershell
git commit -m "feat: descrição clara do que foi feito"
```

### Passo 5 — Enviar para o GitHub
```powershell
git push
```

---

## 🏷️ Convenção de Mensagens de Commit

Use sempre um dos prefixos abaixo para identificar o tipo de alteração:

| Prefixo | Quando usar | Exemplo |
|---------|-------------|---------|
| `feat:` | Nova funcionalidade ou suporte a modelo | `feat: adiciona coleta automática de cotas via API local da IDE` |
| `fix:` | Correção de bug | `fix: corrige cálculo de desgaste do modelo Argon com peso de cache` |
| `docs:` | Alteração em documentação | `docs: atualiza Inicio.md e LEIA-ME.md com Etapa 12` |
| `refactor:` | Refatoração sem mudança visível | `refactor: extrai parseResetTime em _autoSyncQuotasFromIDE` |
| `style:` | Ajuste visual/CSS nos painéis Webview | `style: ajusta espaçamento do botão Auto no painel lateral` |
| `pkg:` | Atualização de build, `.vsix` ou scripts de empacotamento | `pkg: regera medidortokens-0.1.1-agy2.6-fix.vsix após atualização` |
| `chore:` | Tarefas de manutenção ou configuração | `chore: configura .gitignore para ignorar backups .bak locais` |
| `plan:` | Atualização de plano/documentação de planejamento | `plan: registra nova etapa no Plano_Melhorias.md` |

---

## 🚀 Rotina de Marco Importante (Atualizar `main`)

Execute ao concluir uma etapa validada ou fechar uma nova versão `.vsix` do MedidorTokens:

```powershell
# 1. Garantir que develop está atualizada e sem arquivos pendentes
git checkout develop
git status

# 2. Se houver arquivos pendentes, commitar antes (com confirmação)
git add .
git diff --stat --cached   # ⚠️ CONFIRME antes de prosseguir
git commit -m "chore: commit final antes de atualizar main"
git push

# 3. Mudar para main e incorporar as alterações de develop
git checkout main
git merge develop

# 4. Confirmar o que será enviado para main
git log origin/main..HEAD --oneline   # ⚠️ REVISE os commits antes de prosseguir

# 5. Enviar main atualizada para o GitHub
git push origin main

# 6. Voltar para develop para continuar o desenvolvimento
git checkout develop
```

---

## 🔍 Comandos Úteis do Dia a Dia

```powershell
# Ver histórico de commits (resumido)
git log --oneline -10

# Ver diferenças antes de commitar
git diff

# Desfazer alterações em um arquivo específico (antes de commitar)
git checkout -- dist_altimeter_corrigido/extension.js

# Ver quais arquivos estão ignorados pelo .gitignore
git status --ignored

# Ver o que está no repositório remoto
git remote -v
```

---

## ⚠️ Regras Importantes

- **Nunca trabalhar diretamente na branch `main`** — todo desenvolvimento vai em `develop`
- **Repositório Público — tolerância zero com dados privados:** nunca commitar caminhos locais absolutos com nome de usuário (`C:\Users\<usuario>\...`), e-mails pessoais (usar sempre `rogeriocenteno@users.noreply.github.com`) nem nomes de outros projetos privados
- **Nunca commitar `credenciais.md` ou `.env`** — estão protegidos pelo `.gitignore`, mas confirme sempre
- **Nunca commitar a pasta `backup_altimeter_0.0.4/` nem arquivos `*.bak*`** — os backups locais servem para rollback rápido na máquina, enquanto o histórico completo já fica no Git
- **Nunca commitar pacotes `.vsix` antigos (`altimeter-*.vsix`) nem arquivos `transcript*.jsonl` ou `Logo*.jpg`**
- **SEMPRE validar sintaxe (`node -c`) e sincronizar o `.vsix` (`node gerar_pacote.js`) antes de commitar alterações de código**
- **SEMPRE executar a auditoria pré-commit e `git diff --stat --cached` antes de `git commit`** para confirmar visualmente o que será versionado

---

## 🔗 Integração com o Fluxo de Trabalho do Projeto

O versionamento faz parte do fluxo obrigatório definido em [Inicio.md](./Inicio.md) e [Plano_Melhorias.md](./Plano_Melhorias.md):

### Quando commitar (rotina diária)
- **Após cada etapa validada** no Antigravity IDE com **`Developer: Reload Window`** e confirmada pelo usuário
- **Ao final de cada sessão de trabalho**, mesmo que novas melhorias estejam em andamento
- Use mensagens descritivas — evite commits genéricos como `"atualização"` ou `"fix"`

### Antes de commitar alterações de código
Sempre execute na pasta raiz do projeto (`<pasta-do-projeto>`):
```powershell
# 1. Validar sintaxe do código principal
node -c "./dist_altimeter_corrigido/extension.js"

# 2. Sincronizar com a IDE e atualizar medidortokens-0.1.1-agy2.6-fix.vsix
node "./gerar_pacote.js"
```

### Quando atualizar a `main` (rotina de marco)
- **Após concluir e validar uma etapa completa** registrada em [Plano_Melhorias.md](./Plano_Melhorias.md)
- **Sempre que uma nova versão estável do pacote `.vsix`** estiver pronta para distribuição
- Seguir sempre a sequência: commit em `develop` → merge em `main` → push `main` → voltar para `develop`
- Ao fechar nova versão estável, criar a tag/Release no GitHub e publicar o novo `.vsix` no Open VSX Registry (`npx --yes ovsx publish`) seguindo [`dist_altimeter_corrigido/Pub_OpenVSX.md`](./dist_altimeter_corrigido/Pub_OpenVSX.md):

```powershell
$tok = (Read-Host "Cole o Token do Open VSX e aperte Enter").Trim(); npx --yes ovsx publish ./medidortokens-<VERSAO>-agy2.6-fix.vsix -p $tok; Remove-Variable tok -ErrorAction SilentlyContinue
```

### Publicação Automatizada da Release no GitHub (GitHub CLI — `gh`)
Com o **GitHub CLI (`gh`)** instalado e autenticado (`gh auth login --hostname github.com --git-protocol ssh --web` — escolha `Skip` se a chave SSH já estiver cadastrada no GitHub), a criação da Release e o anexo do `.vsix` são feitos diretamente pelo terminal:

```powershell
# 1. Criar tag anotada em main e enviar develop, main e a tag
git tag -a v<VERSAO> -m "MedidorTokens v<VERSAO> - <resumo>"
git push origin develop main v<VERSAO>

# 2. Criar e publicar a Release anexando o .vsix (notas em UTF-8 fora do repositório)
gh release create v<VERSAO> ./medidortokens-<VERSAO>-agy2.6-fix.vsix --repo rogeriocenteno/MedidorTokens --title "MedidorTokens v<VERSAO> - <resumo>" --notes-file "<caminho_temporario_notas_utf8.md>" --latest

# 3. Validar a Release publicada e o anexo
gh release view v<VERSAO> --repo rogeriocenteno/MedidorTokens --json tagName,name,isDraft,isPrerelease,publishedAt,url,assets
```

> **Boas práticas no Windows PowerShell 5.1:**
> - Use `-` (hífen ASCII) em `--title` para evitar problemas de codificação no console, mantendo acentos e formatação Markdown em UTF-8 no arquivo passado em `--notes-file` (gravado fora do repositório, ex.: pasta temporária/scratch, para não sujar o `git status`).
> - Caso o terminal integrado ainda não reconheça `gh` logo após a instalação, use `& "$env:ProgramFiles\GitHub CLI\gh.exe"` ou reinicie a IDE.
> - Em execuções automatizadas com sandbox, o `gh` exige acesso fora do sandbox (`BypassSandbox: true`) para ler `%APPDATA%\GitHub CLI\config.yml` e acessar `api.github.com`.
