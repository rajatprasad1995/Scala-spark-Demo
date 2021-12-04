@echo off
set arg1=%1
if "%arg1%"=="" (echo "You have not mentioned the task") else  (spark-submit --class com.scalaDemo.App   .\target\pesticide-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar %arg1%)