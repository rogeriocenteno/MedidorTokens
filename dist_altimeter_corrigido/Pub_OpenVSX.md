# Guia de Publicação no Open VSX Registry (e VS Code Marketplace)

**Projeto:** MedidorTokens  
**Última atualização:** 18/11/2026 (America/Sao_Paulo, UTC-3)

> [!CAUTION]
> **Segurança e Credenciais:**
> - **NUNCA** salve tokens de acesso do Open VSX, senhas da Eclipse Foundation, PATs do GitHub, chaves privadas ou dados pessoais (como e-mail particular ou caminhos locais com nome de usuário) neste arquivo ou em qualquer arquivo do projeto.
> - Tokens de publicação devem existir apenas no gerenciador de senhas (ou cofre seguro), na memória temporária da sessão do terminal ou nos *Secrets* / *Trusted Publishers (OIDC)* do repositório.
> - Este arquivo é versionado no repositório público do projeto (`https://github.com/rogeriocenteno/MedidorTokens`), portanto deve manter rigorosamente zero exposição de tokens, senhas ou caminhos locais privados.

---

## 1. Dados Oficiais Utilizados na Publicação do MedidorTokens

Todos os metadados abaixo são **públicos** e foram utilizados na publicação inicial da extensão no Open VSX Registry:

| Campo | Valor Oficial Utilizado | Observação |
| :--- | :--- | :--- |
| **Display Name (`displayName`)** | `MedidorTokens` | Nome amigável exibido na loja e na IDE |
| **Name (`name` / ID técnico)** | `medidortokens` | Sem pontos, espaços ou maiúsculas (regra obrigatória do `package.json`) |
| **Publisher / Namespace** | `rogeriocenteno` | Idêntico ao usuário GitHub para facilitar comprovação de propriedade |
| **ID Completo da Extensão** | `rogeriocenteno.medidortokens` | Formato `<publisher>.<name>` |
| **Versão Inicial Publicada** | `0.1.0` | Versionamento semântico (`MAJOR.MINOR.PATCH`) |
| **Arquivo `.vsix` Publicado** | `medidortokens-0.1.0-agy2.6-fix.vsix` | Tamanho: `293.972` bytes (~`287 KB`) |
| **Autoria e E-mail Público Git** | `Rogério Centeno <rogeriocenteno@users.noreply.github.com>` | Uso do e-mail `noreply` público do GitHub |
| **Repositório GitHub (Público)** | `https://github.com/rogeriocenteno/MedidorTokens` | Commit de referência da v0.1.0: `3b6eb42` |
| **Release Oficial v0.1.0** | `https://github.com/rogeriocenteno/MedidorTokens/releases/tag/v0.1.0` | Contém o `.vsix` anexado nos *Assets* |
| **Página Pública no Open VSX** | `https://open-vsx.org/extension/rogeriocenteno/medidortokens` | URL pública da extensão após publicação |
| **Issue de Claim do Namespace** | `https://github.com/EclipseFdn/open-vsx.org/issues/15942` | Solicitação oficial de verificação de propriedade do namespace `rogeriocenteno` |

### Pré-requisitos Técnicos no Pacote (`.vsix` / `package.json`)

Antes de gerar e publicar um `.vsix`, confirme que a extensão atende aos requisitos do Open VSX:
- `publisher` e `name` em letras minúsculas, sem ponto no `name` (`medidortokens`).
- `version` válida (`0.1.0`) e atualizada a cada publicação.
- `repository` apontando para o repositório público oficial.
- `license` declarada e arquivo `LICENSE` presente na raiz/pacote.
- `icon` em formato **PNG** válido (ex.: `256x256` px; no projeto, convertido para `./dist_altimeter_corrigido/icon.png` — formato JPEG/WebP não é aceito corretamente pelo catálogo).
- README claro na extensão sem caminhos locais privados ou dados sensíveis.

---

## 2. Passo a Passo Completo de Publicação Inicial (Do Zero)

### Fase 1 — Conta Open VSX + Conta Eclipse Foundation + Acordo de Publicador

1. Acesse `https://open-vsx.org` e faça login utilizando sua conta do **GitHub**.
2. Crie uma conta na **Eclipse Foundation** em `https://accounts.eclipse.org/user/register` (recomendado usar o mesmo e-mail verificado do GitHub).
3. Nas configurações de perfil do Open VSX (`https://open-vsx.org/user-settings/profile`):
   - Clique em **Log in with Eclipse** (ou vincule sua conta Eclipse) para associar o registro de publicador.
   - Confirme na Eclipse Foundation a opção **Link your GitHub account** com o mesmo usuário GitHub (`rogeriocenteno`).
4. Ainda na página de perfil do Open VSX (`https://open-vsx.org/user-settings/profile`), clique em **Show Publisher Agreement** e depois em **Agree** para aceitar o *Eclipse Open VSX Publisher Agreement*.

---

### Fase 2 — Criação e Reivindicação de Propriedade do Namespace (`Claim Ownership`)

Um namespace recém-criado funciona para publicar imediatamente após aceitar o acordo, porém exibe um ícone de alerta até que a equipe do Open VSX aprove a verificação de propriedade (*verified namespace*).

1. Acesse `https://open-vsx.org/user-settings/namespaces`.
2. Crie o namespace com exatamente o nome do `publisher` definido no `package.json`: `rogeriocenteno`.
3. Se o namespace aparecer sem verificação (símbolo de alerta), abra uma Issue de **Claim Ownership** no repositório `EclipseFdn/open-vsx.org`:
   - **Namespace:** `rogeriocenteno`
   - **Ownership:** marque `The namespace is not currently owned on Open VSX`.
   - **Account Age:** marque `The GitHub ID making this request has at least 12 months of public history` **apenas se** sua conta GitHub tiver mais de 12 meses de histórico público (caso contrário, deixe desmarcada).
   - **Option 3 (Not a VS Code Marketplace Publisher):**
     - Marque `The namespace is not a publisher account on the VS Code Marketplace`.
     - Em *Chose one of the following*, marque `The namespace matches the GitHub ID making this request`.
   - **Claim evidence (modelo utilizado na Issue `#15942`):**

```markdown
The namespace 'rogeriocenteno' matches my GitHub ID (@rogeriocenteno): https://github.com/rogeriocenteno
Extension repository: https://github.com/rogeriocenteno/MedidorTokens
Commit URL: https://github.com/rogeriocenteno/MedidorTokens/commit/3b6eb42
```

> [!NOTE]
> Mesmo enquanto a Issue `#15942` aguarda triagem da equipe Eclipse (`team:operations`), já é possível gerar token e publicar a primeira versão da extensão pelo terminal.

---

### Fase 3 — Geração Segura do Access Token no Open VSX

1. Acesse `https://open-vsx.org/user-settings/tokens`.
2. Clique em **Generate new token**.
3. Preencha os campos seguindo o princípio do menor privilégio:
   - **Description (optional):** `MedidorTokens` (identificação de onde o token será usado).
   - **Namespace (optional):** selecione `rogeriocenteno` (restringe o token apenas a este namespace).
   - **Extension (optional):** deixe vazio (para cobrir futuras extensões do namespace) ou selecione `medidortokens` após a primeira publicação, se desejar restringir ainda mais.
   - **Publishing only:** **marque esta opção** para que o token possa apenas publicar extensões, sem permissão para criar namespaces, apagar extensões ou usar endpoints administrativos.
4. Clique em **Generate Token**, copie o valor gerado (`<SEU_TOKEN_AQUI>`) imediatamente e guarde-o apenas em seu gerenciador de senhas (ele não será exibido novamente).

> [!TIP]
> Para automações de CI/CD no GitHub Actions, o Open VSX também oferece **Trusted Publishers (OIDC)** nas configurações da conta (*Access Tokens* -> *Set up a trusted publisher*), dispensando tokens de longa duração.

---

### Fase 4 — Publicação via Terminal (CLI `ovsx`) sem Gravar Token em Disco

No Windows PowerShell, a partir da pasta raiz do projeto:

1. **Copie o token** gerado para a área de transferência (`Ctrl+C`) sem colá-lo em nenhum arquivo.
2. **Opção A — Publicação direta lendo da área de transferência (PowerShell):**

```powershell
npx --yes ovsx publish ./medidortokens-0.1.0-agy2.6-fix.vsix -p (Get-Clipboard).Trim()
```

3. **Opção B — Usando variável temporária apenas na sessão atual:**

```powershell
$tok = (Get-Clipboard).Trim()
npx --yes ovsx publish ./medidortokens-0.1.0-agy2.6-fix.vsix -p $tok
Remove-Variable tok -ErrorAction SilentlyContinue
```

4. **Saída esperada de sucesso:**

```text
🚀  Published rogeriocenteno.medidortokens v0.1.0
```

5. **Limpe a área de transferência após concluir** (ex.: copie um texto neutro ou execute `Set-Clipboard $null`) e valide a página pública em:
   - `https://open-vsx.org/extension/rogeriocenteno/medidortokens`

---

## 3. Rotina para Publicar Atualizações Futuras (`v0.1.1`, `v0.2.0`, etc.)

Sempre que houver uma nova versão do **MedidorTokens**, siga exatamente esta sequência a partir da raiz do projeto:

1. **Atualizar versão:** edite `targetVersion` em `./gerar_pacote.js` (ex.: `'0.1.1'` ou `'0.2.0'`).
2. **Validar sintaxe JavaScript da extensão:**

```powershell
node -c ./dist_altimeter_corrigido/extension.js
```

3. **Gerar novo pacote `.vsix` e atualizar cópia local na IDE:**

```powershell
node ./gerar_pacote.js
```

4. **Auditoria pré-commit (higiene de segurança):** verifique `git status` e garanta que nenhum arquivo rastreado contenha tokens, credenciais, e-mail pessoal ou caminhos absolutos locais com usuário.
5. **Versionamento Git (`develop` -> `main`) e Release no GitHub:**
   - Faça commit das alterações na branch `develop`.
   - Integre em `main`, crie a tag anotada correspondente (ex.: `v0.1.1`) e envie branches e tag ao repositório remoto (`origin`).
   - Crie a Release correspondente no GitHub anexando o novo arquivo `./medidortokens-<VERSAO>-agy2.6-fix.vsix`.
6. **Publicar no Open VSX:**
   - Gere ou recupere o token `Publishing only` restrito ao namespace `rogeriocenteno`.
   - Execute no PowerShell (substituindo `<VERSAO>` pela versão atual, ex.: `0.1.1`):

```powershell
npx --yes ovsx publish ./medidortokens-<VERSAO>-agy2.6-fix.vsix -p (Get-Clipboard).Trim()
```

7. **Higiene final:** limpe a área de transferência (`Set-Clipboard $null`) e remova variáveis temporárias de token caso tenha usado alguma.

---

## 4. Publicação Opcional no Visual Studio Marketplace (Microsoft VS Code)

Caso queira também distribuir a extensão na loja oficial da Microsoft (VS Code tradicional), além do Open VSX (usado por Antigravity, VSCodium, Cursor, Eclipse Theia, etc.):

1. Acesse o portal de gerenciamento com uma conta Microsoft / Azure DevOps:
   - `https://marketplace.visualstudio.com/manage`
2. Crie o *Publisher* com o mesmo identificador `rogeriocenteno` (caso disponível).
3. Clique em **+ New extension** -> **Visual Studio Code**.
4. Faça o upload do pacote `.vsix` gerado (ex.: `medidortokens-0.1.0-agy2.6-fix.vsix`), ou utilize a CLI oficial (`@vscode/vsce`) para automatizar futuras atualizações.
