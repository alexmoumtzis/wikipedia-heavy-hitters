@echo off
cd /d c:\Users\alexm\wiki-heavy-hitters\spark-heavy-hitters
call sbt compile > compile_output.txt 2>&1
echo EXIT_CODE=%ERRORLEVEL%
