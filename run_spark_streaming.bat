@echo off
set HADOOP_HOME=C:\hadoop
set PATH=%HADOOP_HOME%\bin;%PATH%

echo Demarrage du traitement Spark Streaming...
echo.

python spark_streaming_processor.py

echo.
pause
