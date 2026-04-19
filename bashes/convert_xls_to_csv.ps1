# ===================================================================
# == Script para converter .xls para .csv                          ==
# ===================================================================
param(
    [string]$CaminhoArquivoXLS,
    [string]$CaminhoArquivoCSV
)

$ErrorActionPreference = "Stop"

try {
    $excel = New-Object -ComObject Excel.Application
    $excel.Visible = $false
    $excel.DisplayAlerts = $false

    $workbook = $excel.Workbooks.Open($CaminhoArquivoXLS)
    
    # O número '6' representa o formato xlCSV
    $workbook.SaveAs($CaminhoArquivoCSV, 6)

    $workbook.Close($false)
    $excel.Quit()

    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null

    Write-Output "SUCESSO: Arquivo convertido para CSV em $CaminhoArquivoCSV"
}
catch {
    Write-Output "ERRO: Falha ao converter o arquivo. Causa: $($_.Exception.Message)"
}
