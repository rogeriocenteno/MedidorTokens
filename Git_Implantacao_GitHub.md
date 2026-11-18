# Git_Implantacao_GitHub.md — Guia de Implantação de Versionamento via GitHub

> **Projeto:** MedidorTokens — Extensão MedidorTokens (Monitoramento de Tokens e Cotas para Antigravity IDE 2.6.0+)  
> **Baseado no guia padrão de versionamento Git/GitHub do autor.**  
> **Implantação MedidorTokens:** 18/11/2026.

---

## Pré-requisitos

- [x] Conta GitHub ativa (https://github.com) — conta `rogeriocenteno`
- [x] Git instalado na máquina

### Verificar se Git está instalado
```powershell
git --version
```

### Verificar identidade Git configurada
```powershell
git config --global user.name
git config --global user.email
```

> ✅ **Já configurado nesta máquina** com `Rogerio Centeno <rogeriocenteno@users.noreply.github.com>`

---

## FASE 1 — Autenticação SSH com GitHub

> ✅ **Autenticação SSH configurada** para acesso ao GitHub sem necessidade de gerar nova chave por repositório.

| Parâmetro       | Valor                                              |
|-----------------|-----------------------------------------------------|
| Tipo            | ED25519                                            |
| Arquivo privado | `%USERPROFILE%\.ssh\id_ed25519`                    |
| Arquivo público | `%USERPROFILE%\.ssh\id_ed25519.pub`                |
| Cadastrada em   | https://github.com/settings/keys                  |
| Nome no GitHub  | Projetos - Desenvolvimento                         |

### Verificar a conexão (opcional — para confirmar que ainda funciona)
```powershell
ssh -T git@github.com
```
Resposta esperada:
```
Hi rogeriocenteno! You've successfully authenticated, but GitHub does not provide shell access.
```

---

## FASE 2 — Repositório no GitHub

> ✅ **Repositório `MedidorTokens` já criado** em `https://github.com/rogeriocenteno/MedidorTokens`.

Caso precise conferir os parâmetros ou recriar em outro momento:
1. Acesse: **https://github.com/new**
2. Configurações do repositório:
   - **Repository name:** `MedidorTokens`
   - **Description:** `Extensão MedidorTokens para monitoramento de tokens e cotas em tempo real no Antigravity IDE 2.6.0+`
   - **Visibility:** ✅ **Public** (Repositório Público — exige sanitização total de caminhos locais, e-mails e segredos em todos os commits e ativação de Secret Protection / Push Protection em Settings → Advanced Security)
   - ❌ Não inicialize com arquivos adicionais caso vá conectar um histórico local existente
3. URL SSH de conexão:
   ```text
   git@github.com:rogeriocenteno/MedidorTokens.git
   ```

---

## FASE 3 — O `.gitignore` (já criado)

> ✅ **Já criado** neste projeto. Ver arquivo [.gitignore](./.gitignore) na raiz.

O `.gitignore` protege:

| Categoria | O que está protegido |
|-----------|---------------------|
| Credenciais e chaves | `credenciais.md`, `.env`, `.env.*`, `*credentials*.json`, `*token*.json`, `*secret*.json` |
| Backups locais | `backup_altimeter_0.0.4/`, `backups/`, `*.bak`, `*.bak*`, `PRE_RESTORE_*` |
| Pacotes antigos | `altimeter-*.vsix`, `medidortokens-0.0.*.vsix`, `*.zip` |
| Node.js e logs locais | `node_modules/`, `npm-debug.log*`, `*.log`, `transcript*.jsonl`, `*.pbtxt` |
| Sistema e temporários | `.DS_Store`, `Thumbs.db`, `desktop.ini`, `nul`, `.idea/`, `*.swp`, `*.tmp`, `scratch/` |

> ⚠️ **Arquivos oficiais do projeto** (`dist_altimeter_corrigido/extension.js`, `dist_altimeter_corrigido/icon.png`, `dist_altimeter_corrigido/Pub_OpenVSX.md`, `gerar_pacote.js`, `instalar_correcao_altimeter.bat`, `medidortokens-0.1.0-agy2.6-fix.vsix` e os arquivos `.md` da raiz) **são versionados** — apenas backups locais `.bak`, logs/transcripts e pacotes legados são ignorados.

---

## FASE 4 — Validação Prévia do Pacote (Específico MedidorTokens)

Antes do primeiro commit, confirme que o código JavaScript está válido e sincronizado com o instalador `.vsix`:

```powershell
# Validar sintaxe do código principal
node -c "./dist_altimeter_corrigido/extension.js"

# Sincronizar e garantir que o pacote medidortokens-0.1.0-agy2.6-fix.vsix está atualizado
node "./gerar_pacote.js"
```

---

## FASE 5 — Inicializar o Git Local e Primeiro Commit

Execute dentro da pasta raiz do projeto:

```powershell
# Entrar na pasta do projeto
cd "<pasta-do-projeto>"

# Inicializar repositório Git
git init

# Definir branch principal como 'main'
git branch -M main

# Adicionar todos os arquivos (respeitando .gitignore)
git add .

# ⚠️ CONFIRMAR o que será commitado (OBRIGATÓRIO)
git diff --stat --cached
# Revise a lista. Confirme que os itens abaixo NÃO aparecem:
# - backup_altimeter_0.0.4/ e arquivos *.bak*
# - altimeter-*.vsix / medidortokens-0.0.*.vsix
# - credenciais.md / .env / *token*.json
# - transcript*.jsonl / *.log

# Criar o primeiro commit
git commit -m "feat: primeiro commit — extensão MedidorTokens v0.1.0 (rogeriocenteno) para Antigravity IDE 2.6.0+"
```

---

## FASE 6 — Conectar ao GitHub e Fazer o Push

```powershell
# Adicionar o repositório remoto (URL SSH da Fase 2)
git remote add origin git@github.com:rogeriocenteno/MedidorTokens.git

# Enviar para o GitHub
git push -u origin main
```

---

## FASE 7 — Criar a Branch de Desenvolvimento (`develop`)

```powershell
# Criar e mudar para a branch develop
git checkout -b develop

# Enviar para o GitHub
git push -u origin develop
```

A partir de agora:
- **`develop`** → branch do dia a dia (todo desenvolvimento e ajuste novo)
- **`main`** → branch estável, atualizada em marcos importantes (fim de etapa ou nova versão `.vsix` validada)

---

## Resultado Final Esperado

Após as 7 fases, você terá:

| Item | Status |
|------|--------|
| Autenticação SSH com GitHub | ✅ Configurada permanentemente |
| Repositório `MedidorTokens` no GitHub | ✅ Criado |
| `.gitignore` protegendo backups `.bak` e dados sensíveis | ✅ Ativo |
| Código validado com `node -c` e pacote `.vsix` atualizado | ✅ Confirmado |
| Primeiro commit com código, scripts e documentação | ✅ Feito |
| Branch `main` no GitHub | ✅ Enviada |
| Branch `develop` criada e ativa | ✅ Pronta |

---

## Checklist de Segurança (Validar Antes do Primeiro Push)

```powershell
# Verificar que credenciais.md NÃO está sendo rastreado
git status --short | Select-String "credenciais"

# Verificar que .env NÃO está sendo rastreado
git status --short | Select-String "\.env"

# Verificar que backups .bak NÃO estão sendo rastreados
git status --short | Select-String "backup_altimeter|\.bak"

# Verificar que pacotes .vsix antigos NÃO estão sendo rastreados
git status --short | Select-String "0\.0\.4|0\.0\.5|0\.0\.6"

# Verificar que logs/transcripts locais NÃO estão sendo rastreados
git status --short | Select-String "transcript|\.log|\.pbtxt"

# Verificar que não há caminhos locais com usuário ou e-mails pessoais nos arquivos rastreados
git grep -iE "gmail\.com|C:\\\\Users\\\\" -- ':!*.vsix' ':!*.png'
```

Nenhum resultado = ✅ seguro para commitar.

---

## Comandos de Referência Rápida

```powershell
git status                    # Ver estado atual
git add .                     # Adicionar tudo
git add dist_altimeter_corrigido/extension.js   # Adicionar arquivo específico
git commit -m "mensagem"      # Commitar
git push                      # Enviar para GitHub
git pull                      # Baixar do GitHub
git log --oneline -10         # Ver últimos 10 commits
git branch                    # Listar branches
git checkout develop          # Mudar para branch develop
git checkout main             # Mudar para branch main
git merge develop             # Incorporar develop na branch atual
```
