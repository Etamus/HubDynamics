# ===================================================================
# == Script Genérico: runner.ps1 (VERSÃO SIMPLIFICADA)             ==
# ===================================================================

param(
    [string]$CaminhoArquivo,
    [string]$NomeMacro
)

$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$excel.DisplayAlerts = $false
$workbook = $excel.Workbooks.Open($CaminhoArquivo)
$excel.Run($NomeMacro)
$workbook.Save()
$workbook.Close()
$excel.Quit()
[System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null
Write-Output "SUCESSO: Macro executada."
