@echo off
echo Demarrage de l'infrastructure Docker (Zookeeper, Kafka, PostgreSQL, Grafana)...
echo.

docker compose up -d

if %ERRORLEVEL% neq 0 (
    echo [ERREUR] Impossible de demarrer les conteneurs Docker.
    pause
    exit /b 1
)

echo.
echo Infrastructure demarree avec succes !
echo   - Zookeeper : localhost:2181
echo   - Kafka : localhost:9092
echo   - PostgreSQL : localhost:5433 (user: postgres, db: iot_db)
echo   - Grafana : http://localhost:3000 (admin / admin)
echo.
docker compose ps
echo.
pause
