# Guia de Inicialização do Projeto — Extensão MedidorTokens (Antigravity IDE 2.6.0+)

> **Documento de Orientação para Agentes e Desenvolvedores**  
> **Última atualização:** 19/11/2026 (Versão `0.1.3`)  
> **Autor / Mantenedor:** Rogério Centeno (`rogeriocenteno`)  
> **Fuso horário obrigatório do projeto:** `America/Sao_Paulo` (`UTC-3`)  
> **Idioma oficial de interface, código e documentação:** Português do Brasil (`pt-BR`)  
> **Visibilidade do repositório:** **PÚBLICO** (`https://github.com/rogeriocenteno/MedidorTokens`) — **NUNCA incluir dados pessoais, e-mails privados ou caminhos absolutos da máquina local**

---

## 1. Visão Geral do Projeto

Este projeto mantém, aprimora e distribui a extensão **MedidorTokens** (`rogeriocenteno.medidortokens`, baseada originalmente em `pqub.altimeter`), responsável por monitorar em tempo real o consumo de tokens de Inteligência Artificial (Entrada Nova, Lido do Cache, Saída, Contexto, Chamadas de API e Janelas/Cotas 5h e 7d) no **Antigravity IDE**.

### Localizações Principais
* **Pasta deste repositório/workspace:**  
  `./` (raiz do repositório)
* **Instalação ativa da extensão na IDE do usuário:**  
  `%USERPROFILE%\.antigravity-ide\extensions\rogeriocenteno.medidortokens-0.1.3\`
* **Arquivo JavaScript compilado em execução:**  
  `%USERPROFILE%\.antigravity-ide\extensions\rogeriocenteno.medidortokens-0.1.3\dist\extension.js`
* **Logs de sessão do Antigravity lidos pela extensão:**  
  `%USERPROFILE%\.gemini\antigravity\brain\<cascade-id>\.system_generated\logs\transcript.jsonl`
* **Página Oficial no Open VSX Registry:**  
  `https://open-vsx.org/extension/rogeriocenteno/medidortokens`
* **Release Oficial v0.1.3 no GitHub:**  
  `https://github.com/rogeriocenteno/MedidorTokens/releases/tag/v0.1.3`

---

## 2. Histórico da Correção para Antigravity IDE 2.6.0+

Na versão 2.6.0 do Antigravity IDE, o processo legado `language_server_windows_x64.exe` foi substituído pelo serviço nativo em background (`antigravity.exe daemon`). Isso quebrou a versão original `0.0.4` da extensão com o erro:
```text
Language Server not found
```

### O que foi corrigido no núcleo:
1. **`EnvironmentScanner` (`qe`):** Detecta o ambiente do Antigravity 2.6.0 e redireciona a busca para a pasta de dados do usuário em `~/.gemini/antigravity/brain/`.
2. **`LSClient` (`tt`):** Lê os arquivos `transcript.jsonl` diretamente do disco via streaming incremental, extraindo dados de tokens (`input_tokens`, `output_tokens`, `cache_read_tokens`) e transições de modelo via tag `<USER_SETTINGS_CHANGE>`.

---

## 3. Resumo das Melhorias Implementadas (Etapas 1 a 15)

| Etapa | Funcionalidade | Descrição Técnica |
| :---: | :--- | :--- |
| **01** | **Cores Dinâmicas para Modelos** | `Proxy` em `ra()` e `MODEL_COLORS` gerando cores HSL/Hex contrastadas e exclusivas para qualquer modelo futuro sem cair no cinza padrão `#6B7280`. |
| **02** | **Legendas DOM Nativas** | Legendas em HTML de alto contraste abaixo de cada gráfico de pizza na barra lateral, com nomes, valores formatados (`K`/`M`) e percentuais visíveis sem precisar passar o mouse. |
| **03** | **Detalhamento de Cache e Alerta de Contexto** | Separação visual discreta entre `↳ Entrada Nova` e `↳ Lido do Cache`. Destaque de Contexto: normal (<120k), amarelo/alerta (120k–180k) e vermelho/perigo (>180k). |
| **04** | **Tradução pt-BR e Datas UTC-3** | Tradução completa da UI lateral e janela de estatísticas. Conversão de datas ISO para `America/Sao_Paulo` (`UTC-3`) e exibição no formato `DD/MM/AAAA`. |
| **05** | **Atualização Automática Leve (10s)** | Checagem leve de `mtime` em disco a cada 10 segundos (<1 ms de CPU) com retorno antecipado quando o arquivo não foi alterado. |
| **06** | **Seletor de Sessões + Cache Multi-Sessão** | Menu dropdown no topo permitindo alternar entre `Sessão Atual`, `Hoje (Todas as Sessões)` e sessões passadas. `oa` (`SessionCache`) atualizado com `Map` retendo dados em memória para troca em 0 ms. |
| **07** | **Barra de Status Inferior (Status Bar)** | Indicador no rodapé da IDE (`$(graph) 34.7M in \| 163.8K out`) com tooltip multi-linhas e clique para abrir estatísticas. Desacoplado da visibilidade da aba, continuando a atualizar mesmo com Explorer/Git abertos. |
| **08** | **Filtro de Período e Exportação CSV** | Filtros rápidos (`8 Dias`, `15 Dias`, `30 Dias`, `Tudo`) na janela de estatísticas completas e botão `📥 Exportar CSV` com codificação `UTF-8 com BOM`, separador `;`, datas `DD/MM/AAAA` e totais diários. |
| **09** | **Janelas & Cotas (5h / 7d) + Multi-Conta** | Monitoramento das janelas móveis de `5h` e `7d` agrupadas por família (`Gemini Models`, `Claude & GPT`, `Gemini 4 Argon`), seletor de conta, formulário de referência de `%` e tempo de renovação (`resetAt`), badges de contagem regressiva e rollover automático de ciclo (`_resolveQuotaRef`). |
| **10** | **Alinhamento Nativo e Reciclagem sem Congelamento** | Exibição em `% Restante` (idêntico à tela nativa do Antigravity), estimativa de desgaste dinâmico em tempo real (`_estimateLiveQuota`), calibração do modelo Argon com peso de cache reduzido, rollover contínuo sem travar em 100% e normalização dos nomes de modelos. |
| **11** | **Suporte a Contas Free / Starter Quota e Gestão de Contas** | Alternância com 1 clique entre **Google AI Pro** e **Antigravity Starter Quota (Free)** por conta, ocultação dinâmica dos limites de 5h e Argon para contas Starter, formulário simplificado de cotas para Starter, taxas de desgaste calibradas por plano e botões `+` e `×` para adicionar e remover contas. |
| **12** | **Modelo `Orchestrator (Argon)` e Coleta Automática de Cotas** | Registro do modelo `Orchestrator (Argon)` (`gemini-pro-orchestrator-g1-pro`) agrupado na família Argon (`_getQuotaGroup`), e sincronização automática em segundo plano (`_autoSyncQuotasFromIDE`, throttle 30s + botão `🔄 Auto`) via `google.google-antigravity` / `LanguageServerService` (`GetUserStatus` e `RetrieveUserQuotaSummary`) atualizando `% Restante`, `resetAt`, e-mail da conta ativa e plano (`Pro`/`Free`). |
| **13** | **Identidade MedidorTokens e Publicação no Open VSX (v0.1.1)** | Renomeação completa para `MedidorTokens` (`rogeriocenteno.medidortokens`), conversão do ícone oficial `256x256` PNG, redução de `engines.vscode` para `^1.80.0` (compatibilidade com qualquer build do Antigravity IDE), adição das tags de busca (`medidor`, `medidortokens`, `tokens`, `cotas`) e guia de publicação em `dist_altimeter_corrigido/Pub_OpenVSX.md`. |
| **14** | **Suporte ao Modelo `Gemini 4 Neon` (v0.1.2)** | Registro das variantes `Gemini 4 Neon (Extra)`, `(High)`, `(Medium)`, `(Low)` e base em `sa` e `saSlugMap` com paleta rosa/magenta neon dedicada (`#C2185B`–`#F48FB1`), ordenação logo após `Gemini 4 Argon` e agrupamento automático na cota `Gemini Models` (`5h / 7d`, disponível em contas pagas). |
| **15** | **Suporte ao Modelo `Orchestrator (Neon)` (v0.1.3)** | Registro de `Orchestrator (Neon)` (`gemini-pro-orchestrator-neon`) em `sa`, `saSlugMap` e `at()` com cor dedicada `#D946EF`, ordenação `0.5` (sem colisão com `Orchestrator (Argon)`) e agrupamento na cota `Gemini Models` (`5h / 7d`) em `_getQuotaGroup` e `_autoSyncQuotasFromIDE`. |

---

## 4. Mapa da Arquitetura em `dist/extension.js`

O arquivo `dist/extension.js` é um bundle minificado gerado via esbuild. Ao realizar modificações, consulte este mapa de símbolos:

| Símbolo | Linha Aprox. | Função / Responsabilidade |
| :--- | :---: | :--- |
| `qe` | ~28 | `EnvironmentScanner` — localiza a pasta `brain/` e porta/token do ambiente. |
| `tt` | ~61 | `LSClient` — lê `transcript.jsonl`, extrai tokens e modelos. |
| `oa` | ~61 | `SessionCache` — gerencia o cache em memória multi-sessão (`cacheMap`). |
| `sa`, `ra()`, `MODEL_COLORS` | ~95 | Tabela de modelos conhecidos e Proxy gerador de cores para novos modelos. |
| `nt` | ~95 | `StatsCalculator` — calcula totais (`totalInput`, `totalOutput`, `totalCacheRead`, `lastContextSize`, `modelBreakdown`). |
| `it` | ~95 | `TrajectoryService` — paginação de dados (`fetchSessionData`, `fetchAllData`, `fetchSortedConversations`). |
| `Tt` | ~95 | `DailyAggregator` — agrupador diário, conversor de fuso horário `America/Sao_Paulo` e formatação `DD/MM/AAAA`. |
| `xr` | ~95 | Template HTML/CSS/JS da janela completa de estatísticas (`ca.createOrShow`). |
| `ca` | ~821 | `StatisticsPanel` — controlador da janela de estatísticas e receptor de mensagens (`generateStats`, `days`). |
| `hr` | ~821 | Template HTML/CSS do painel lateral (`la`). |
| `la` | ~1232 | `SidebarProvider` — controlador da barra lateral, dropdown de sessões, janelas de cotas (`5h`/`7d`), multi-conta, pooling de 10s e atualizador do `statusBarItem`. Métodos-chave: `_getQuotaGroup`, `_resolveQuotaRef`, `_checkAndPersistRollovers`, `_autoSyncQuotasFromIDE` (coleta automática de cotas/conta/plano, ~1533), `_estimateLiveQuota`, `_computeWindowStats`, `_fetchCurrentSessionStats`, `_updateHtml`. |
| `Ru(t)` | ~2320 | `activate` — função de ativação da extensão, registro do Webview, comandos e criação do `statusBarItem`. |

> [!WARNING]
> **Cuidado com Template Literals em `xr`:**  
> A variável `var xr = \`<!doctype html>...\`` é uma template literal string gigantesca. Qualquer uso de backticks (\`) ou `${}` não escapados dentro do JavaScript interno de `xr` causa erro fatal de sintaxe durante o carregamento do Node.js. Sempre utilize concatenação de strings padrão (`+`) e aspas simples/duplas dentro do script de `xr`.

---

## 5. Estrutura de Arquivos do Projeto

```text
./
│
├── README.md                             <-- Apresentação pública do repositório
├── LICENSE                               <-- Licença MIT
├── Inicio.md                             <-- Este guia (leitura inicial obrigatória)
├── Plano_Melhorias.md                    <-- Planejamento detalhado das etapas e status
├── LEIA-ME.md                            <-- Manual de instalação para o usuário final
├── Git_Implantacao_GitHub.md             <-- Guia de implantação inicial do repositório GitHub
├── Git_Rotina_Versionamento.md           <-- Rotina diária de commits, branches e marco em main
├── .gitignore                            <-- Proteção contra versionamento de backups e dados sensíveis
│
├── medidortokens-0.1.3-agy2.6-fix.vsix   <-- Pacote instalador oficial gerado (Versão 0.1.3)
├── gerar_pacote.js                       <-- Script Node.js que sincroniza e compila o .vsix
├── instalar_correcao_altimeter.bat       <-- Script batch para instalação rápida no Windows
│
├── dist_altimeter_corrigido\
│   ├── extension.js                      <-- Cópia espelhada do extension.js para o VSIX
│   ├── icon.png                          <-- Ícone oficial 256x256 PNG da extensão
│   └── Pub_OpenVSX.md                    <-- Guia completo de publicação no Open VSX Registry e VS Code Marketplace
│
└── backup_altimeter_0.0.4\               <-- Backups incrementais de segurança
    ├── backup_antes_etapa_01.js.bak
    ├── ...
    ├── backup_versao_0.0.5.js.bak
    ├── backup_antes_etapa_10_cotas_nativas.js.bak
    ├── backup_antes_suporte_contas_free.js.bak
    ├── backup_antes_calibracao_fina_free.js.bak
    ├── backup_antes_modelo_orchestrator_argon.js.bak
    └── backup_antes_coleta_automatica_cotas.js.bak
```

---

## 6. Fluxo de Trabalho Obrigatório para Novas Alterações

Ao iniciar qualquer nova tarefa ou manutenção neste projeto, siga estritamente estes passos:

### Passo 1: Criar Backup Prévio
Nunca altere `extension.js` sem criar uma cópia de segurança nomeada:
```powershell
node -e "require('fs').copyFileSync('./dist_altimeter_corrigido/extension.js','./backup_altimeter_0.0.4/backup_antes_<nome_da_alteracao>.js.bak')"
```
> A edição é feita em `dist_altimeter_corrigido\extension.js` (workspace); o `gerar_pacote.js` sincroniza com a instalação da IDE.

### Passo 2: Fazer a Alteração Cirúrgica
* Altere apenas o trecho estritamente necessário.
* Nunca altere partes funcionais alheias à solicitação.
* Mantenha textos e mensagens em **Português do Brasil (`pt-BR`)**.
* Use fuso horário **`America/Sao_Paulo` (`UTC-3`)** e formato de data **`DD/MM/AAAA`** para a interface.

### Passo 3: Validar a Sintaxe com Node.js
Antes de empacotar, garanta que não há erros de compilação:
```bash
node -c "./dist_altimeter_corrigido/extension.js"
```
*(Deve retornar código de saída 0 sem nenhum erro)*.

### Passo 4: Sincronizar e Regerar o `.vsix`
Execute o gerador oficial do projeto (copia `dist_altimeter_corrigido\extension.js` para `%USERPROFILE%\.antigravity-ide\extensions\rogeriocenteno.medidortokens-0.1.3\dist\extension.js`, atualiza metadados/registro na IDE e gera o `.vsix`). Como grava fora do workspace, rode com `BypassSandbox: true`:
```powershell
node "./gerar_pacote.js"
```

### Passo 5: Instruir o Usuário para Testes
Solicite ao usuário executar o recarregamento da janela no Antigravity IDE:
> Pressione **`Ctrl + Shift + P`** (ou `F1`) e execute **`Developer: Reload Window`**.

### Passo 6: Versionar no Git (`develop`) e Publicar
Após validação do usuário, siga o fluxo de versionamento em [`Git_Rotina_Versionamento.md`](./Git_Rotina_Versionamento.md) na branch `develop` (consulte [`Git_Implantacao_GitHub.md`](./Git_Implantacao_GitHub.md) caso ainda vá configurar o ambiente Git). Sempre revise `git diff --stat --cached` antes do `git commit`, promova para `main` ao concluir marcos importantes e utilize [`dist_altimeter_corrigido/Pub_OpenVSX.md`](./dist_altimeter_corrigido/Pub_OpenVSX.md) para publicar novas versões no Open VSX Registry.

#### Política Obrigatória de Segurança para Repositório Público
Como este repositório é **público**, todo arquivo alterado e todo commit **DEVEM** obedecer estritamente às regras abaixo:
* **Zero caminhos absolutos locais:** Nunca incluir caminhos com nome de usuário ou diretórios específicos da máquina (ex.: `C:\Users\<usuario>\...`, `d:\...`); usar sempre `%USERPROFILE%`, `~`, `os.homedir()`, `__dirname` ou caminhos relativos (`./`).
* **Zero dados privados:** Nunca incluir e-mails pessoais reais, tokens, chaves, senhas, logs de sessão ou nomes de outros projetos privados em código, documentação ou mensagens de commit.
* **Identidade de commit pública:** Manter o e-mail de autor Git configurado no repositório como `rogeriocenteno@users.noreply.github.com`.
* **Auditoria pré-commit obrigatória:** Executar sempre a verificação automatizada de dados sensíveis e revisar `git diff --stat --cached` antes de cada `git commit`.

---

## 7. Dicas do Ambiente Windows e Sandbox

1. **Erro `CreateProcessSecurityEnvironment`:**  
   Em ambientes Windows com sandboxing restrito, se o comando falhar com erro de criação de ambiente, utilize `BypassSandbox: true` mantendo o mesmo `toolSummary`.
2. **Pacote `.vsix`:**  
   O arquivo `medidortokens-0.1.3-agy2.6-fix.vsix` é um zip no padrão Open VSIX. O `gerar_pacote.js` monta o pacote com `zlib` nativo do Node.js (sem `archiver` e sem `vsce`).
3. **Repositório Git:**  
   `https://github.com/rogeriocenteno/MedidorTokens` (público). Trabalhar sempre em `develop`; `main` só em marcos. Ver `Git_Rotina_Versionamento.md`. No sandbox, `git push` exige `BypassSandbox: true` (SSH).
4. **Reload da IDE:**  
   Após gerar o pacote, o usuário deve executar `Developer: Reload Window`.

