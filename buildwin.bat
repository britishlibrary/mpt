::::::::::::::::::::::::::::::::::
:: Script to build MPT exe      ::
::::::::::::::::::::::::::::::::::

@echo off
SETLOCAL

IF EXIST .\.venv_32\ ( CALL :Build 32 ) ELSE ( echo "Requires virtual environment (named .venv_32) with pyinstall" )
IF EXIST .\.venv_64\ ( CALL :Build 64 ) ELSE ( echo "Requires virtual environment (named .venv_64) with pyinstall" )
EXIT /B %ERRORLEVEL%
:::::::::::::::::::::::::::::::::::::::::

:Build
  SET bitness=%~1
  echo Building %bitness%-bit MPT
  :: switch to correct bitness venv
  CALL .\.venv_%bitness%\Scripts\activate.bat

  :: run pyinstaller to build
  pyinstaller --workpath=build\pyi.win%bitness% --distpath=dist\win%bitness% -y mpt-onefile.spec

  :: switch back out of venv  
  CALL .\.venv_%bitness%\Scripts\deactivate.bat
  echo Finished building %bitness%-bit MPT
  echo.
  EXIT /B 0

:End
