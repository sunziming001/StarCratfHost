@echo off
setlocal

set "SCRIPT=%~dp0start_host.ps1"
if not defined SC_HOST_PYTHON set "SC_HOST_PYTHON=C:\Users\sunziming\AppData\Local\Programs\Python\Python310\python.exe"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT%" -Config "sc_host_3p.ini" %*
set "EXIT_CODE=%errorlevel%"
exit /b %EXIT_CODE%
