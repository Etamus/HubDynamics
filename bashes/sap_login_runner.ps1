# ===================================================================
# == Script SAP Login: sap_login_runner.ps1 (VERSÃO SIMPLIFICADA)  ==
# ===================================================================

param(
    [string]$CaminhoArquivo,
    [string]$NomeMacro,
    [string]$Usuario,
    [string]$Senha
)

$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$excel.DisplayAlerts = $false
$workbook = $excel.Workbooks.Open($CaminhoArquivo)
$worksheet = $workbook.Worksheets.Item("entrada")
$worksheet.Range("A60").Value = $Usuario
$worksheet.Range("A61").Value = $Senha
$workbook.Save()
$excel.Run($NomeMacro)
$workbook.Close($false)
$excel.Quit()
[System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null
Write-Output "SUCESSO: Macro de login executada."
