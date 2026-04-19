# ===================================================================
# == Script de Backup Automático — Hub Dynamics (M4)               ==
# ===================================================================
# Faz uma cópia diária do banco de dados SQLite na pasta backups/.
# Recomendado: agendar via Agendador de Tarefas do Windows para rodar
# todo dia às 02:00.

param(
    [string]$DbPath    = "$PSScriptRoot\..\hub_dynamics.db",
    [string]$BackupDir = "$PSScriptRoot\..\backups",
    [int]$KeepDays     = 30
)

$ErrorActionPreference = "Stop"

try {
    # Garante que a pasta de backups existe
    if (-not (Test-Path $BackupDir)) {
        New-Item -ItemType Directory -Path $BackupDir | Out-Null
    }

    if (-not (Test-Path $DbPath)) {
        Write-Output "AVISO: Banco de dados não encontrado em '$DbPath'. Nada a fazer."
        exit 0
    }

    $timestamp  = Get-Date -Format "yyyy-MM-dd_HH-mm"
    $backupFile = Join-Path $BackupDir "hub_dynamics_$timestamp.db"

    Copy-Item -Path $DbPath -Destination $backupFile -Force
    Write-Output "SUCESSO: Backup salvo em '$backupFile'"

    # Remove backups mais antigos que $KeepDays dias
    $cutoff = (Get-Date).AddDays(-$KeepDays)
    Get-ChildItem -Path $BackupDir -Filter "hub_dynamics_*.db" |
        Where-Object { $_.LastWriteTime -lt $cutoff } |
        ForEach-Object {
            Remove-Item $_.FullName -Force
            Write-Output "INFO: Backup antigo removido: $($_.Name)"
        }
}
catch {
    Write-Output "ERRO: Falha no backup. Causa: $($_.Exception.Message)"
    exit 1
}
