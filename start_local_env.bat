@echo off
REM Start the mock upstream, the local bridge, and optionally flutter run in separate cmd windows.
REM Usage: double-click this .bat or run from cmd. It will open new windows and leave them running.

SETLOCAL
SET ROOT=%~dp0
REM Ensure we're in the Testing the Flask app folder
cd /d "%ROOT%"

REM Start mock upstream (127.0.0.1:5001)
start "Mock Upstream" cmd /k "echo Starting mock_upstream on %CD% & python mock_upstream.py"

REM Give the mock a moment to start
ping -n 2 127.0.0.1 >nul

REM Start local bridge (testapp_local.py) - serves on 0.0.0.0:5000
start "Local Bridge" cmd /k "echo Starting testapp_local on %CD% & python testapp_local.py"

REM Optionally start Flutter (comment/uncomment as needed)
REM Change the path below to your flutter project root if different
REM start "Flutter" cmd /k "cd /d "%CD%\..\smart_farm_platform" & echo Running flutter (use CTRL+C to stop) & flutter run -d chrome --dart-define=API_BASE_URL=http://127.0.0.1:5000/api"

ENDLOCAL
exit /b 0
