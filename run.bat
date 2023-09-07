@echo off
:Start
python scraper.py
echo Script has exited. Restarting in 1 minute...
timeout /t 60 /nobreak > nul
goto Start