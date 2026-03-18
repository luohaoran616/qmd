@echo off
setlocal
set "QMD_ROOT=%~dp0"
set "XDG_CONFIG_HOME=%QMD_ROOT%else\openclaw-qmd-xdg\config"
set "XDG_CACHE_HOME=%QMD_ROOT%else\openclaw-qmd-xdg\cache"
if not exist "%XDG_CONFIG_HOME%\qmd" mkdir "%XDG_CONFIG_HOME%\qmd"
if not exist "%XDG_CACHE_HOME%" mkdir "%XDG_CACHE_HOME%"
bun "%QMD_ROOT%dist\cli\qmd.js" %*
