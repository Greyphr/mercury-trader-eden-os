@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo ============================================
echo  Mercury Trader - demo launcher
echo ============================================
echo.

rem -- 1. Python venv bootstrap --------------------------------------------
if not exist ".venv\Scripts\python.exe" (
    echo [setup] No virtualenv found - creating one at .venv ...
    python -m venv .venv
    if errorlevel 1 (
        echo [error] Failed to create virtualenv. Is Python installed and on PATH?
        pause
        exit /b 1
    )
    echo [setup] Installing dependencies from requirements\core.txt ...
    ".venv\Scripts\python.exe" -m pip install --upgrade pip
    ".venv\Scripts\python.exe" -m pip install -r requirements\core.txt
    if errorlevel 1 (
        echo [error] Dependency install failed - see output above.
        pause
        exit /b 1
    )
)
set PY=.venv\Scripts\python.exe
rem Always run the code that lives next to this script, regardless of any
rem stale editable install inside .venv.
set "PYTHONPATH=%CD%\src"

rem -- 2. .env sanity check -------------------------------------------------
if not exist ".env" (
    echo [setup] No .env found - copying .env.example to .env.
    copy ".env.example" ".env" >nul
    echo [action needed] Open .env and fill in MT5_LOGIN_DEMO / MT5_PASSWORD_DEMO
    echo                 at minimum, then re-run this script.
    pause
    exit /b 1
)

findstr /r /c:"^MT5_LOGIN_DEMO=." .env >nul
if errorlevel 1 (
    echo [action needed] MT5_LOGIN_DEMO is empty in .env - fill in your MetaQuotes
    echo                 demo account login before running the bot.
    pause
    exit /b 1
)
findstr /r /c:"^MT5_PASSWORD_DEMO=." .env >nul
if errorlevel 1 (
    echo [action needed] MT5_PASSWORD_DEMO is empty in .env - fill in your MetaQuotes
    echo                 demo account password before running the bot.
    pause
    exit /b 1
)

rem -- 3. Ollama reachability check (advisory) ------------------------------
echo [check] Looking for Ollama at localhost:11434 ...
curl -s -o nul -w "%%{http_code}" http://localhost:11434/api/tags > "%TEMP%\ollama_check.txt" 2>nul
set /p OLLAMA_CODE=<"%TEMP%\ollama_check.txt"
if not "!OLLAMA_CODE!"=="200" (
    echo [warning] Ollama doesn't seem to be running at http://localhost:11434.
    echo           Start the Ollama app/service, then re-run this script.
    echo           ^(Fast pre-trade checks will fall back to rule-based reasoning
    echo           if this isn't reachable when Mercury starts.^)
    echo.
    choice /c YN /m "Continue anyway"
    if errorlevel 2 exit /b 1
) else (
    echo [ok] Ollama is reachable.
)

rem -- 4. Detect an installed Ollama model if OLLAMA_MODEL isn't set --------
findstr /r /c:"^OLLAMA_MODEL=." .env >nul
if errorlevel 1 (
    echo [setup] OLLAMA_MODEL not set in .env - checking what's already pulled ...
    set DETECTED_MODEL=
    for /f "tokens=1" %%m in ('ollama list ^| findstr /v "NAME"') do (
        if not defined DETECTED_MODEL set DETECTED_MODEL=%%m
    )
    if defined DETECTED_MODEL (
        echo [setup] Using detected model: !DETECTED_MODEL!
        >> ".env" echo.
        >> ".env" echo OLLAMA_MODEL=!DETECTED_MODEL!
    ) else (
        echo [action needed] No Ollama models found and OLLAMA_MODEL isn't set in .env.
        echo                 Run e.g. "ollama pull llama3.1:8b", then re-run this script.
        pause
        exit /b 1
    )
)

rem -- 5. LM Studio reachability check (advisory) ---------------------------
echo [check] Looking for LM Studio's local server at localhost:1234 ...
curl -s -o nul -w "%%{http_code}" http://localhost:1234/v1/models > "%TEMP%\lmstudio_check.txt" 2>nul
set /p LMSTUDIO_CODE=<"%TEMP%\lmstudio_check.txt"
if not "!LMSTUDIO_CODE!"=="200" (
    echo [warning] LM Studio's local server doesn't seem to be running at
    echo           http://localhost:1234. In LM Studio: Developer tab -^> Start Server.
    echo           ^(Deep/post-trade reasoning will fall back to rule-based
    echo           reasoning if this isn't reachable when Mercury starts.^)
    echo.
    choice /c YN /m "Continue anyway"
    if errorlevel 2 exit /b 1
) else (
    echo [ok] LM Studio server is reachable.
)

findstr /r /c:"^LM_STUDIO_MODEL=." .env >nul
if errorlevel 1 (
    if "!LMSTUDIO_CODE!"=="200" (
        echo [setup] LM_STUDIO_MODEL not set - checking what LM Studio has available ...
        set PICKED_MODEL=
        "%PY%" scripts\list_lm_studio_models.py > "%TEMP%\lmstudio_models.txt" 2>nul
        set /a MODEL_COUNT=0
        for /f "delims=" %%L in ('type "%TEMP%\lmstudio_models.txt" ^| find /c /v ""') do set MODEL_COUNT=%%L

        if !MODEL_COUNT! EQU 0 (
            echo [action needed] LM Studio is reachable but reported no models.
            echo                 Download/load a model in LM Studio, then re-run this script.
            echo.
            choice /c YN /m "Continue anyway ^(rule-based fallback^)"
            if errorlevel 2 exit /b 1
        ) else if !MODEL_COUNT! EQU 1 (
            rem --select 1 avoids fragile inline parsing of "1. <id>" when the id
            rem itself contains dots (e.g. llama-3.1-8b-instruct).
            for /f "delims=" %%R in ('"%PY%" scripts\list_lm_studio_models.py --select 1') do set PICKED_MODEL=%%R
            echo [setup] Only one model available - using it: !PICKED_MODEL!
            >> ".env" echo.
            >> ".env" echo LM_STUDIO_MODEL=!PICKED_MODEL!
        ) else (
            echo.
            echo [choose] LM Studio has multiple models available:
            type "%TEMP%\lmstudio_models.txt"
            echo.
            set /p MODEL_CHOICE="Enter a number (default 1): "
            if "!MODEL_CHOICE!"=="" set MODEL_CHOICE=1
            for /f "delims=" %%R in ('"%PY%" scripts\list_lm_studio_models.py --select !MODEL_CHOICE!') do set PICKED_MODEL=%%R
            if defined PICKED_MODEL (
                echo [setup] Using: !PICKED_MODEL!
                >> ".env" echo.
                >> ".env" echo LM_STUDIO_MODEL=!PICKED_MODEL!
            ) else (
                echo [error] Invalid selection - LM_STUDIO_MODEL left unset.
                choice /c YN /m "Continue anyway ^(rule-based fallback^)"
                if errorlevel 2 exit /b 1
            )
        )
    ) else (
        echo [action needed] LM_STUDIO_MODEL is empty in .env, and LM Studio's
        echo                 server isn't reachable to list models from.
        echo                 ^(Deep reasoning will fall back to rule-based
        echo                 assessments - the bot will still run.^)
        echo.
        choice /c YN /m "Continue anyway"
        if errorlevel 2 exit /b 1
    )
)

rem -- 6. PostgreSQL reachability check (hard requirement) ------------------
echo [check] Looking for PostgreSQL at localhost:5432 ...
"%PY%" -c "import socket; s=socket.socket(); s.settimeout(3); exit(0 if s.connect_ex(('localhost',5432))==0 else 1)"
if errorlevel 1 (
    echo [error] Nothing is listening on localhost:5432 - is PostgreSQL running?
    echo         Mercury requires Postgres; there is no SQLite fallback for real runs.
    pause
    exit /b 1
) else (
    echo [ok] Something is listening on localhost:5432.
)

rem -- 7. Configure this run for hybrid Ollama + LM Studio ------------------
set MERCURY_ENV=metaquotes_demo
set DEPLOYMENT_MODE=paper
set HERMES_LLM_PROVIDER=hybrid
set HERMES_EXTERNAL_PROVIDER=lm_studio

echo.
echo [config] MERCURY_ENV=%MERCURY_ENV%
echo [config] DEPLOYMENT_MODE=%DEPLOYMENT_MODE%
echo [config] HERMES_LLM_PROVIDER=%HERMES_LLM_PROVIDER%  (fast=Ollama, deep=LM Studio)
echo.

rem -- 8. Health check first -------------------------------------------------
echo [step] Running config health check ...
"%PY%" -m mercury.main --env %MERCURY_ENV% health
if errorlevel 1 (
    echo [error] Health check failed - see output above. Not starting the bot.
    pause
    exit /b 1
)

echo.
choice /c YN /m "Health check looks OK above - start the bot now"
if errorlevel 2 exit /b 0

rem -- 9. Run, with a timestamped log file alongside the app's own logs ------
rem (tee isn't assumed to exist; output goes to the log file instead of the
rem console - open/tail the log in another terminal to follow progress.)
if not exist "logs\demo" mkdir "logs\demo"
for /f %%t in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set STAMP=%%t
set LOGFILE=logs\demo\run_%STAMP%.log

echo [step] Starting: mercury run --env %MERCURY_ENV%
echo        Console output goes to %LOGFILE% - tail it in another terminal:
echo        powershell -Command "Get-Content '%LOGFILE%' -Wait"
echo        Press Ctrl+C to stop.
echo.

"%PY%" -m mercury.main --env %MERCURY_ENV% run > "%LOGFILE%" 2>&1

echo.
echo [stopped] Mercury Trader has exited. Log saved to %LOGFILE%
pause
