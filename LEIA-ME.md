# Correção e Aprimoramentos da Extensão Altimeter para Antigravity IDE 2.6.0+

**Versão:** 0.1.0  
**Autor / Mantenedor:** Rogério Centeno (`rogeriocenteno`)  
**Data da atualização:** 18/11/2026  
**Compatibilidade:** Antigravity IDE 2.6.0 ou superior (com retrocompatibilidade para versões anteriores)  
**Fuso horário:** `America/Sao_Paulo` (`UTC-3`) | **Idioma:** Português do Brasil (`pt-BR`)

---

## O que esta versão inclui?

1. **Correção do Núcleo Antigravity 2.6.0+:** Elimina o erro `Language Server not found`, lendo diretamente os logs de transcript do Antigravity em `~/.gemini/antigravity/brain/`.
2. **Monitoramento de Janelas e Cotas (5h e 7d) com Coleta Automática:**
   - **Sincronização Automática com a IDE (`🔄 Auto`):** Coleta automaticamente do servidor local do Antigravity IDE o e-mail da conta ativa, o plano (`Google AI Pro` ou `Starter Free`), a porcentagem restante (`% Restante`) e os horários exatos de renovação (`resetAt`), mantendo ainda a opção `⚙ Cotas` para ajustes manuais.
   - Agrupamento nativo idêntico à tela do IDE: **Gemini Models**, **Claude and GPT models** e **Gemini 4 Argon / Orchestrator** (incluindo o novo modelo **Orchestrator (Argon)** / `gemini-pro-orchestrator-g1-pro`).
   - Formato nativo em **`% Restante`** com indicador discreto de **`% Usado`** e indicador `✓` quando sincronizado automaticamente.
   - Contagem regressiva em tempo real com badges visuais para renovação (`resetAt`).
   - Rollover automático de ciclo (renovação contínua de cota sem congelar em 100%).
3. **Suporte Completo a Planos Pro e Free (Starter Quota):**
   - **Google AI Pro:** Janelas semanais e de 5h para Gemini e Claude/GPT, mais limite semanal exclusivo de Gemini 4 Argon.
   - **Antigravity Starter Quota (Free):** Interface adaptativa que oculta o Argon e os limites de 5h, exibindo com fidelidade apenas os limites semanais reais do Gemini e Claude/GPT.
   - Alternância com 1 clique entre planos (`★ Pro` <-> `⚡ Starter (Free)`).
4. **Estimativa de Desgaste Dinâmico em Tempo Real:** Acompanha o consumo das cotas entre sincronizações com calibrações matemáticas separadas para plano Pro e Starter Free.
5. **Gestão Dinâmica Multi-Conta:** Permite adicionar novas contas com o botão `+` (via caixa de diálogo nativa), definir o plano e remover contas com o botão `×`.
6. **Interface Completa em Português do Brasil (`pt-BR`):** Todas as datas convertidas para o fuso `America/Sao_Paulo` (`UTC-3`) em formato brasileiro `DD/MM/AAAA`.
7. **Barra de Status Inferior (`Status Bar`):** Exibe o consumo de tokens no rodapé da IDE com atualização contínua e tooltip detalhado.
8. **Relatórios e Exportação CSV:** Janela de estatísticas com filtros rápidos (8, 15, 30 dias ou Tudo) e exportação em `.csv` (UTF-8 com BOM e separador `;` para Excel).

---

## Repositório e versionamento

- **GitHub:** `https://github.com/rogeriocenteno/MedidorTokens` — desenvolvimento em `develop`, versões estáveis em `main`.
- Guias: `Git_Implantacao_GitHub.md` (implantação inicial) e `Git_Rotina_Versionamento.md` (rotina diária).
- Documentação técnica e fluxo de trabalho: `Inicio.md` e `Plano_Melhorias.md`.

---

## Como instalar (3 opções)

### Forma A: Instalar o pacote `.vsix` direto pelo Antigravity IDE (Recomendado)
1. Abra o **Antigravity IDE**.
2. Abra a aba **Extensions** (`Ctrl + Shift + X`).
3. Clique no menu de três pontos **`...`** (no canto superior direito da barra de extensões) e selecione **`Install from VSIX...`**.
4. Selecione o arquivo **`altimeter-0.1.0-agy2.6-fix.vsix`**.
5. Recarregue a janela (`Ctrl + Shift + P` → `Developer: Reload Window`).

### Forma B: Instalador automático de 1 clique (Windows)
1. Dê dois cliques em **`instalar_correcao_altimeter.bat`**.
2. O script detecta automaticamente a instalação do Altimeter, salva backup do arquivo anterior e copia a versão atualizada.
3. No Antigravity IDE, pressione `Ctrl + Shift + P` e execute **`Developer: Reload Window`**.

### Forma C: Substituição manual do arquivo `extension.js`
1. Copie o arquivo `dist_altimeter_corrigido\extension.js`.
2. Cole substituindo o arquivo existente na sua pasta de extensões:
   `%USERPROFILE%\.antigravity-ide\extensions\rogeriocenteno.altimeter-0.1.0\dist\extension.js`  
   *(ou na pasta correspondente à versão instalada)*.
3. Recarregue a janela do Antigravity IDE (`Ctrl + Shift + P` → `Developer: Reload Window`).

