@echo off
chcp 65001 >nul
echo ============================================================================
echo   Instalador da Correcao do Altimeter para Antigravity IDE 2.6.0+
echo   Data: 18/11/2026 - Versao: 0.1.0 - Autor: Rogerio Centeno
echo ============================================================================
echo.

set "EXT_BASE=%USERPROFILE%\.antigravity-ide\extensions"
set "ORIGEM=%~dp0dist_altimeter_corrigido\extension.js"

if not exist "%ORIGEM%" (
    echo [ERRO] Arquivo de origem nao encontrado em:
    echo "%ORIGEM%"
    pause
    exit /b 1
)

set "ENCONTRADO=0"
for /d %%D in ("%EXT_BASE%\rogeriocenteno.altimeter-*" "%EXT_BASE%\pqub.altimeter-*") do (
    set "ENCONTRADO=1"
    echo Encontrada instalacao do Altimeter em:
    echo   %%D
    if not exist "%%D\dist\extension.js.original.bak" (
        copy /Y "%%D\dist\extension.js" "%%D\dist\extension.js.original.bak" >nul
        echo   [OK] Backup do arquivo original salvo em: extension.js.original.bak
    )
    copy /Y "%ORIGEM%" "%%D\dist\extension.js" >nul
    echo   [OK] Arquivo extension.js atualizado com sucesso!
    echo.
)

if "%ENCONTRADO%"=="0" (
    echo [AVISO] Nenhuma pasta rogeriocenteno.altimeter-* ou pqub.altimeter-* foi encontrada em:
    echo   "%EXT_BASE%"
    echo Voce pode instalar diretamente o pacote .vsix pelo Antigravity IDE:
    echo   Ctrl+Shift+X -^> menu "..." -^> Install from VSIX... -^> altimeter-0.1.0-agy2.6-fix.vsix
) else (
    echo ============================================================================
    echo   Concluido! Agora no Antigravity IDE pressione Ctrl+Shift+P e execute:
    echo   Developer: Reload Window
    echo ============================================================================
)
echo.
pause

