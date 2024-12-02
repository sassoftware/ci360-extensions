@echo off
:start
cls
cd \
python -m pip install --upgrade pip
python -m pip install configparser
python -m pip install flask
python -m pip install jsonify
python -m pip install requests
python -m pip install html
python -m pip install logging
pause
exit