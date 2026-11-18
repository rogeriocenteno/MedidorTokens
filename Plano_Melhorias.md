# Plano de Melhorias — Extensão Altimeter (Antigravity IDE 2.6.0+)

**Versão atual:** `0.1.0` | **Autor / Mantenedor:** Rogério Centeno (`rogeriocenteno`)  
**Data de criação do plano:** 28/10/2026  
**Fuso horário padrão do projeto:** `UTC-3` (`America/Sao_Paulo`)  
**Idioma padrão da interface e documentação:** Português do Brasil (`pt-BR`)  
**Arquivo-alvo principal:** `./dist_altimeter_corrigido/extension.js` (sincronizado pelo `gerar_pacote.js` com `%USERPROFILE%\.antigravity-ide\extensions\rogeriocenteno.altimeter-0.1.0\dist\extension.js`)  
**Pasta de backups e pacotes do projeto:** `./` (raiz do projeto)  
**Repositório:** `https://github.com/rogeriocenteno/MedidorTokens` (branch de trabalho `develop`)

---

## Diretrizes Obrigatórias de Execução
1. **Execução Incremental (Uma melhoria por vez):** Cada etapa deste plano deve ser implementada isoladamente e testada com `Developer: Reload Window` antes de avançar para a próxima etapa.
2. **Preservação de Código e Backups:** Nunca alterar partes do código que não façam parte da etapa solicitada. Antes de iniciar qualquer etapa, salvar uma cópia numerada do arquivo `extension.js` anterior (ex.: `backup_etapa_01.js.bak`).
3. **Padrão de Datas e Fuso Horário:**
   - **Exibição ao usuário (UI, tabelas, gráficos, exportação):** Formato Brasileiro `DD/MM/AAAA` (ou `DD/MM/AAAA HH:mm`), convertido para o fuso horário `America/Sao_Paulo` (`UTC-3`).
   - **Processamento interno e chaves de ordenação:** Formato Americano `YYYY-MM-DD`.
4. **Suporte a Caracteres Acentuados (`UTF-8`):** Garantir codificação `UTF-8` correta para caracteres acentuados (`á, é, í, ó, ú, ã, õ, â, ê`) e `ç/Ç` tanto no HTML das Webviews quanto na exportação de arquivos.

---

## Mapa da Arquitetura Atual em `dist/extension.js`

| Símbolo / Classe | Linha Aprox. | Responsabilidade no Código |
| :--- | :--- | :--- |
| `qe` (`EnvironmentScanner`) | Linha 8 | Detecta o ambiente do Antigravity IDE 2.6.0+ e aponta para `~/.gemini/antigravity/brain`. |
| `tt` (`LSClient`) | Linha 15 | Lê os arquivos `transcript.jsonl`, extrai tokens (`input_tokens`, `output_tokens`, `cache_read_tokens`) e mudanças de modelo (`<USER_SETTINGS_CHANGE>`). |
| `oa` (`SessionCache`) | Linha 15 | Armazena em memória o cache de estatísticas da sessão ativa (`cascadeId`, `lastModifiedTime`, `nextMetaOffset`). |
| `sa`, `at()`, `ra()`, `kt()`, `fr()` | Linha 15 | Registro de modelos, normalização de nomes, mapa de cores (`displayName -> hex`) e ordem de exibição. |
| `nt` (`StatsCalculator`) | Linha 15 | Calcula os totais da sessão (`totalCalls`, `totalInput`, `totalOutput`, `totalCacheRead`, `lastContextSize`, `modelBreakdown`). |
| `it` (`TrajectoryService`) | Linha 15 | Lista e ordena todas as conversas (`fetchSortedConversations`) e busca dados incrementais (`fetchSessionData`). |
| `Tt` (`DailyAggregator`) | Linha 15 | Filtra conversas por data e agrega o consumo diário por modelo para a tela de estatísticas. |
| `xr` + `ca` (`StatisticsPanel`) | Linhas 16–598 | Template HTML/JS e controlador da janela completa **Altimeter Statistics** (`altimeter.openStatistics`). |
| `hr` + `la` (`SidebarProvider`) | Linhas 598–857 | Template HTML/CSS/JS e controlador do painel lateral **Altimeter: Token Stats** (`altimeter.mainView`). |
| `Ru()` (`activate`) | Linha 857 | Função de ativação da extensão, registro da Webview lateral e dos comandos. |

---

## Detalhamento Técnico das 12 Etapas de Melhoria (todas concluídas)

---

### Etapa 1: Cores Dinâmicas Automáticas para Futuros Modelos
- **Status:** ✅ Concluído
- **Objetivo:** Garantir que qualquer novo modelo lançado futuramente no Antigravity IDE receba automaticamente uma cor exclusiva e bem contrastada (evitando cair no cinza `#6B7280`).
- **Onde alterar:** Funções `ra()`, `Rn` (Linha 15 e Linha 729) e script de `xr` (Linha 316 e Linha 409).
- **Como implementar:**
  1. Criar função auxiliar `getModelColor(modelName)`:
     - Se existir em `Rn[modelName]`, retorna a cor cadastrada em `sa`.
     - Caso contrário, calcula um hash numérico da string `modelName` e gera uma cor HSL/Hex vibrante (ex.: paleta pré-definida de 16 cores distintas ou `hsl(hash % 360, 70%, 55%)`).
  2. Substituir `Rn[x.displayName] || An` na linha 733/735 e `MODEL_COLORS[model] || '#6B7280'` na linha 316 por essa função.
- **Como testar:** Simular um nome de modelo não listado em `sa` e verificar se a bolinha e os gráficos exibem cor própria.

---

### Etapa 2: Legenda e Percentuais Visíveis nos Gráficos de Pizza (`Token Distribution`)
- **Status:** ✅ Concluído
- **Objetivo:** Fazer com que os gráficos de pizza da barra lateral exibam legenda clara com o nome do modelo, quantidade abreviada (`K`/`M`) e percentual (`%`), evitando que conversas com 1 único modelo pareçam apenas um círculo sem texto.
- **Onde alterar:** Configuração `pieConfig` dentro de `la._updateHtml` (Linhas 786–819 de `dist/extension.js`).
- **Como implementar:**
  1. Alterar `plugins.legend` de `{ display: false }` para exibir legenda compacta na parte inferior (`position: 'bottom'`, `labels: { color: '#ccc', boxWidth: 10, font: { size: 10 } }`) ou renderizar um resumo compacto logo abaixo de cada `<canvas>`.
  2. Incluir no tooltip e na legenda tanto a porcentagem (`%`) quanto o total formatado de tokens (`formatNumber(val)`).
- **Como testar:** Abrir a seção **Token Distribution** com 1 ou mais modelos e verificar a exibição dos percentuais e valores sem precisar passar o mouse.

---

### Etapa 3: Detalhamento de `Entrada Nova` vs. `Lido do Cache` + Alerta de Contexto
- **Status:** ✅ Concluído
- **Objetivo:** Separar claramente no painel lateral quanto do `Input` foi processamento novo (`input_tokens`) e quanto foi reaproveitado do cache (`cache_read_tokens`), além de sinalizar visualmente quando o contexto (`lastContextSize`) estiver alto.
- **Onde alterar:** Método `la._updateHtml` (Linhas 733–754 de `dist/extension.js`) e estilos CSS em `hr` (Linhas 605–714).
- **Como implementar:**
  1. Em `la._updateHtml`, temos `c.totalInput` (entrada nova sem cache), `c.totalCacheRead` (lido do cache) e `d = c.totalInput + c.totalCacheRead` (entrada total).
  2. Adicionar sub-linhas discretas abaixo de `Tokens de Entrada`:
     - `↳ Entrada Nova:` `${c.totalInput.toLocaleString('pt-BR')}`
     - `↳ Cache Lido:` `${c.totalCacheRead.toLocaleString('pt-BR')}`
  3. No campo `Context`, aplicar classe CSS de destaque:
     - Verde/Azul normal até `120.000` tokens.
     - Amarelo/Âmbar (`#F9AB00`) entre `120.000` e `180.000` tokens.
     - Laranja/Vermelho (`#FF6B35`) acima de `180.000` tokens (próximo da compactação automática).
- **Como testar:** Verificar no painel lateral se a soma de `Entrada Nova + Cache Lido` bate exatamente com o `Input Total`.

---

### Etapa 4: Tradução Completa para Português do Brasil + Datas em `DD/MM/AAAA` (`UTC-3`)
- **Status:** ✅ Concluído (Aguardando verificação do usuário)
- **Objetivo:** Padronizar toda a interface em Português do Brasil e formatar todas as datas visíveis no padrão `DD/MM/AAAA` considerando o fuso `America/Sao_Paulo` (`UTC-3`), mantendo `YYYY-MM-DD` internamente.
- **Onde alterar:**
  - Classe `Tt` (Linha 15): métodos `formatDate`, `extractDateFromItem`, `aggregateByDay`.
  - Template `xr` (Linhas 16–598): cabeçalhos, botões, textos de carregamento e tabelas da tela de estatísticas.
  - Template `hr` e classe `la` (Linhas 598–857): rótulos do painel lateral.
- **Como implementar:**
  1. **Conversão para `UTC-3` (`America/Sao_Paulo`) na agregação diária (`Tt`):**
     - Ao converter o timestamp ISO (`created_at`, que vem em UTC `Z` no `transcript.jsonl`), usar `new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Sao_Paulo', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date(timestamp))` para gerar a chave interna `YYYY-MM-DD` exata no horário de Brasília (`UTC-3`), evitando que conversas entre `21:00` e `23:59` (horário de Brasília) sejam lançadas no dia seguinte (UTC)!
  2. **Exibição `DD/MM/AAAA` para o usuário:**
     - Criar função `formatDateBR(isoDateStr)` que converte `"YYYY-MM-DD"` para `"DD/MM/AAAA"` nos eixos dos gráficos e nas colunas `Data ⇕` das tabelas, mantendo o atributo `data-sort="YYYY-MM-DD"` para que a ordenação das colunas continue funcionando perfeitamente.
  3. **Tradução dos textos da UI (`pt-BR`):**
     - `Input Tokens` → `Tokens de Entrada`
     - `Output Tokens` → `Tokens de Saída`
     - `API Calls` → `Chamadas de API`
     - `Context` → `Contexto Atual`
     - `Cache Efficiency` → `Eficiência de Cache`
     - `Models` → `Modelos`
     - `Token Distribution` → `Distribuição de Tokens`
     - `DAILY TOTALS (Aggregated)` → `TOTAIS DIÁRIOS (Consolidado)`
- **Como testar:** Abrir o painel lateral e a janela de estatísticas e validar acentuação, datas em `DD/MM/AAAA` e contabilização correta das chamadas feitas após as 21h (`UTC-3`).

---

### Etapa 5: Atualização Instantânea em Tempo Real (A cada 10 segundos)
- **Status:** ✅ Concluído (Aguardando verificação do usuário)
- **Objetivo:** Atualizar o painel lateral automaticamente a cada 10 segundos, com checagem leve de timestamp e retorno antecipado quando inativo para evitar redesenho desnecessário (flicker).
- **Onde alterar:** Métodos `_startAutoRefresh` e `_stopAutoRefresh` na classe `la` (Linha 729 de `dist/extension.js`).
- **Como implementar:**
  1. Reduzir o intervalo de verificação leve de `60000` ms (60s) para `3000` ms (3s), porém fazendo uma checagem ultrarrápida em memória (`fs.statSync` apenas do `mtimeMs`/`size`) antes de processar qualquer JSON.
  2. Como `la._fetchCurrentSessionStats` já possui cache (`if (!e && i.lastModifiedTime === n.lastModifiedTime)`), se não houve nova mensagem, a checagem leva menos de `1 ms` e não redesenha a Webview (evitando qualquer *flicker* ou gasto de CPU).
  3. Quando o arquivo `transcript.jsonl` muda, o painel atualiza sozinho em até 3 segundos após a resposta.
- **Como testar:** Enviar uma mensagem no chat sem clicar no botão 🔄 do Altimeter e observar os tokens subirem automaticamente assim que a resposta terminar.

---

### Etapa 6: Identificação da Conversa e Seletor de Sessão / "Resumo de Hoje" na Barra Lateral
- **Status:** ✅ Concluído (Com Cache em Memória Multi-Sessão)
- **Objetivo:** Mostrar qual conversa está sendo exibida no painel lateral e permitir alternar entre a **Sessão Atual**, o **Total de Hoje (Todas as sessões)** ou **qualquer sessão anterior** através de um seletor (`<select>`).
- **Otimização Aplicada:** A classe `oa` (`SessionCache`) foi atualizada com um `Map` multi-sessão. Agora, qualquer sessão prévia carregada fica retida em memória enquanto o seu `lastModifiedTime` não mudar. Alternar entre sessões no dropdown é instantâneo (0 ms) e o modo "Hoje" não relê arquivos estáticos do disco a cada ciclo de 10s.
- **Onde alterar:**
  - `tt.getAllCascadeTrajectories` (Linha 15): extrair a primeira `<USER_REQUEST>` de cada `transcript.jsonl` (primeiros 45 caracteres + data `DD/MM/AAAA HH:mm`) para usar como título legível da conversa em `summary` (hoje aparece apenas o UUID).
  - Classe `la` (Linha 729): adicionar propriedade `this._selectedMode = "latest"` (podendo ser `"latest"`, `"today"` ou um `cascadeId` específico).
  - Template `hr` / `la._updateHtml`: adicionar um `<select id="session-selector">` estilizado no topo do painel lateral.
- **Como implementar:**
  1. Quando `this._selectedMode === "latest"`, comporta-se exatamente como hoje (acompanha automaticamente a última conversa ativa).
  2. Quando `this._selectedMode === "today"`, soma os tokens de todas as conversas do dia atual (no fuso `UTC-3`) e exibe no painel lateral.
  3. Quando `this._selectedMode === "<cascadeId>"`, exibe o histórico acumulado daquela sessão escolhida, mesmo sem enviar nova mensagem nela.
- **Como testar:** Alternar no dropdown do painel lateral entre "Sessão Atual", "Total de Hoje" e uma conversa anterior da lista.

---

### Etapa 7: Indicador na Barra de Status Inferior da IDE (`Status Bar`)
- **Status:** ✅ Concluído (Atualização em segundo plano desacoplada da visibilidade da aba)
- **Objetivo:** Exibir um resumo em tempo real na barra inferior do Antigravity IDE para acompanhar o contexto e os tokens mesmo quando a barra lateral estiver fechada ou no Explorer de arquivos.
- **Ajuste Realizado:** Desacoplado o timer de 10 segundos da visibilidade da Webview (`onDidChangeVisibility`). Agora o pooling leve em segundo plano roda continuamente assim que a extensão é ativada, mantendo a barra inferior sempre atualizada mesmo navegando no Explorer de arquivos, extensões ou controle de versão. Quando o usuário clica de volta na aba do Altimeter, a tela lateral re-renderiza instantaneamente com os dados em cache.
- **Onde alterar:** Função `Ru(t)` (Linha 857) e classe `la` (Linha 729).
- **Como implementar:**
  1. Em `Ru(t)`, criar um `vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100)`.
  2. Configurar `statusBarItem.command = "altimeter.openStatistics"`.
  3. Sempre que `la._fetchCurrentSessionStats` atualizar as estatísticas (ou em um timer leve independente caso a sidebar esteja oculta), atualizar o texto da barra inferior:
     - Texto: `$(graph) Ctx: 33,1K | Ent: 9,2M | Saída: 63,2K`
     - Tooltip (ao passar o mouse): detalhamento por modelo da sessão ativa e eficiência de cache.
- **Como testar:** Fechar a barra lateral do Altimeter, enviar uma mensagem no chat e verificar o contador atualizando no rodapé do Antigravity IDE.

---

### Etapa 8: Filtro de Período (8, 15, 30 dias ou Tudo) e Exportação CSV na Tela de Estatísticas
- **Status:** ✅ Concluído
- **Objetivo:** Permitir analisar períodos maiores que 8 dias na tela **Altimeter Statistics** e exportar os dados consolidados para planilha (`.csv` compatível com Excel em Português, separado por `;` e codificado em `UTF-8 com BOM`).
- **Onde alterar:** Template `xr` (Linhas 16–598) e classe `ca` (`StatisticsPanel`, Linha 598).
- **Como implementar:**
  1. Adicionar barra de controles no topo de `xr`:
     - Seletor de período: `8 dias` (padrão), `15 dias`, `30 dias`, `90 dias`, `Todo o histórico`.
     - Botão `📥 Exportar CSV`.
  2. Em `ca._onMessage`, receber o parâmetro `days` (`e.days || 8`) e passar para `Tt.filterByModifiedDate(n, days)` e `Tt.aggregateByDay(o, s, days)`.
  3. Ao clicar em `Exportar CSV`, enviar mensagem `exportCsv` para a extensão usar `vscode.window.showSaveDialog` e gravar o arquivo `.csv` com datas no formato `DD/MM/AAAA`.
- **Como testar:** Abrir o painel de estatísticas (`📊`), alternar o período para 15/30 dias e testar a exportação do arquivo CSV.

---

### Etapa 9: Janelas & Cotas (5h / 7d), Multi-Conta e Renovação Automática (v0.0.6)
- **Status:** ✅ Concluído
- **Objetivo:** Acompanhar na barra lateral o consumo acumulado nas janelas móveis de `5h` e `7d` por família de modelo (`Gemini Models`, `Claude & GPT`, `Gemini 4 Argon`), com suporte a múltiplas contas, referência de cota (`%`) e contagem regressiva de renovação (`resetAt`).
- **Onde alterar:** Template `hr` e classe `la` (`_getQuotaGroup`, `_resolveQuotaRef`, `_computeWindowStats`, `_updateHtml`).
- **Funcionalidades implementadas na `0.0.6`:**
  1. **Janelas móveis de 5h e 7d (`_computeWindowStats`):** Soma tokens de entrada, leitura de cache, saída e chamadas de API nos últimos 5h e 7d com cache incremental em memória (`_rawStepsCache`).
  2. **Multi-conta (`#account-select`):** Alternância rápida entre contas com persistência no `globalState`.
  3. **Tempo de renovação e Rollover automático (`_resolveQuotaRef`):** Permite informar o tempo restante para renovação (`3h 47m`, `4d 10h`), exibe badge colorido com contagem regressiva e renova automaticamente o ciclo quando o tempo expira.

---

### Etapa 10: Alinhamento Nativo de Cotas, Desgaste em Tempo Real e Correção de Reciclagem
- **Status:** ✅ Concluído
- **Objetivo:** Alinhar visualmente com a tela nativa do Antigravity (`Settings > Models & Usage`), apresentar `% Restante` (com `% Usado` discreto), calcular desgaste dinâmico em tempo real por chamada/tokens e impedir congelamento em 100% após a reciclagem.
- **Implementações:**
  1. **Ordem e formato nativos:** Apresentação em `% Restante` com cor verde (>50%), amarelo (20-50%) e laranja/vermelho (<20%).
  2. **Estimativa de Desgaste Dinâmico (`_estimateLiveQuota`):** Atualiza o consumo em tempo real entre sincronizações manuais, considerando chamadas de API, saída, tokens novos e cache lido com peso calibrado.
  3. **Calibração do Modelo Argon:** Redução do peso do cache lido (0.08) e calibração fina do divisor de tokens (260.000) e taxa por chamada (0.36%), igualando com precisão os 71% do painel nativo.
  4. **Reciclagem sem congelamento (`_checkAndPersistRollovers`):** Persiste novos ciclos no `globalState` e calcula o desgaste do novo ciclo sem travar a barra em 100%.

---

### Etapa 11: Suporte a Contas Free (Antigravity Starter Quota) e Gestão Dinâmica de Contas
- **Status:** ✅ Concluído
- **Objetivo:** Oferecer suporte pleno a contas gratuitas (*Antigravity Starter Quota*) e contas pagas (*Google AI Pro*), adaptando os cards e formulários para refletir com fidelidade as cotas reais de cada tipo de plano.
- **Implementações:**
  1. **Seletor Rápido de Plano na Barra Lateral:** Badge alternável com 1 clique (`★ Pro` <-> `⚡ Starter (Free)`) ao lado do seletor de contas.
  2. **Interface Adaptativa por Plano:**
     - **Modo Starter (Free):** Oculta automaticamente o card do **Gemini 4 Argon** e remove as linhas de **5 Horas**, exibindo exclusivamente os limites semanais reais: `Gemini Models Semanal (7d)` e `Claude and GPT models Semanal (7d)`.
     - **Modo Google AI Pro:** Exibe normalmente as 5 cotas completas (Gemini 7d/5h, Claude/GPT 7d/5h e Argon 7d).
  3. **Formulário de Cotas Inteligente:** O modal `⚙ Cotas` exibe apenas os campos correspondentes ao plano da conta ativa.
  4. **Calibração de Consumo por Plano:**
     - **Modo Pro:** Calibrado para cotas altas (Gemini 8M tok / 0.002% ch; Claude 1.8M tok / 0.03% ch; Argon 260k tok / 0.36% ch com peso de cache 0.08).
     - **Modo Starter (Free):** Ajustado com base em dados de uso real (Gemini 35k tok / 1.5% ch correspondendo aos 48% nativos; Claude 15k tok / 3.8% ch correspondendo com fidelidade à queda de 46% em 10 chamadas de Claude Opus para bater exatamente nos 6% reais).
  5. **Gestão Dinâmica de Contas:** Botão `+` para adicionar qualquer e-mail/conta via input nativo do IDE (com escolha imediata de plano Pro/Free) e botão `×` para excluir contas salvas.

---

### Etapa 12: Novo Modelo `Orchestrator (Argon)` e Coleta Automática de Cotas
- **Status:** ✅ Concluído
- **Objetivo:** Reconhecer e contabilizar o novo modelo `Orchestrator (Argon)` (`gemini-pro-orchestrator-g1-pro`) dentro do grupo **Gemini 4 Argon / Orchestrator** e sincronizar automaticamente conta ativa, plano (`Pro` vs `Starter Free`), `% Restante` e horários de renovação (`resetAt`) diretamente do servidor local da IDE.
- **Implementações:**
  1. **Registro do Modelo `Orchestrator (Argon)` (`sa`, `saSlugMap`, `at`, `_getQuotaGroup`):** Mapeia tanto o rótulo `Orchestrator (Argon)` quanto o ID `gemini-pro-orchestrator-g1-pro` com cor `#7C3AED` e agrupa seu consumo no card `Gemini 4 Argon / Orchestrator`.
  2. **Coleta Automática via API Local do Antigravity (`_autoSyncQuotasFromIDE`):**
     - Conecta-se à extensão nativa `google.google-antigravity` / `LanguageServerService` (`GetUserStatus` e `RetrieveUserQuotaSummary` com fallback via `getRpcClient()` e `cascadeModelConfigData.clientModelConfigs`).
     - Detecta automaticamente o e-mail da conta logada na IDE e seu plano (`Google AI Pro` vs `Antigravity Starter Quota`), respeitando alternâncias manuais até que o usuário clique em `🔄 Auto`.
     - Atualiza automaticamente `% Restante`, `resetAt` e reinicia `savedAt` no instante da sincronização para evitar desconto duplicado, mantendo throttling leve de 30 segundos em segundo plano.
  3. **Controles na Interface (`🔄 Auto` e indicador `✓`):** Botão `🔄 Auto` ao lado de `⚙ Cotas` para sincronização manual instantânea e selo `✓` no rodapé de cada barra sincronizada automaticamente.


---

## Ordem Original de Implantação (histórico — Etapas 1 a 8)

Ordem seguida na implantação das primeiras etapas (menor para maior impacto visual). As Etapas 9 a 12 foram acrescentadas depois:
1. **Etapa 1** — Cores dinâmicas automáticas para futuros modelos.
2. **Etapa 2** — Legenda e percentuais visíveis nos gráficos de pizza.
3. **Etapa 3** — Detalhamento `Entrada Nova` vs. `Cache Lido` e alerta de tamanho de contexto.
4. **Etapa 4** — Tradução para Português do Brasil e datas em `DD/MM/AAAA` com fuso `UTC-3`.
5. **Etapa 5** — Atualização instantânea em tempo real (a cada 3 segundos sem peso na CPU).
6. **Etapa 6** — Título legível da conversa e seletor `Sessão Atual / Hoje / Outras Sessões` na barra lateral.
7. **Etapa 7** — Contador na Barra de Status inferior da IDE (`Status Bar`).
8. **Etapa 8** — Filtro de período (8, 15, 30 dias, Tudo) e exportação CSV na janela de estatísticas.

> **Observação:** Após concluir qualquer etapa, execute `node gerar_pacote.js` na pasta raiz do projeto (`./`) para sincronizar a extensão instalada e atualizar o instalador `altimeter-0.1.0-agy2.6-fix.vsix`. Depois, versione em `develop` conforme `Git_Rotina_Versionamento.md`.

