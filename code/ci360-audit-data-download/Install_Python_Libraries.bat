@echo off
:start
cls
cd \
python -m pip install --upgrade pip
python -m pip install configparser
python -m pip install pandas
python -m pip install fastparquet
python -m pip install requests
python -m pip install jsonpath
pause
exit