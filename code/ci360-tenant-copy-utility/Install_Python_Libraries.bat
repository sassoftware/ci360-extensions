@echo off
:start
cls
cd \
python -m pip install --upgrade pip
python -m pip install configparser
python -m pip install jwt
python -m pip install requests
pause
exit