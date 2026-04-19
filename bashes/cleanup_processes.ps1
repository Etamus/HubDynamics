# ===================================================================
# == Script Finalizador de Processos (ATUALIZADO)                  ==
# ===================================================================

# Lista dos nomes dos processos que queremos encerrar (sem o .exe)
$processosParaFinalizar = @(
    "EXCEL",
    "saplogon",
    "sapgui",
    "chrome",
    "msedge"
)

$mensagens = @()

foreach ($nome in $processosParaFinalizar) {
    $processo = Get-Process -Name $nome -ErrorAction SilentlyContinue

    if ($processo) {
        try {
            Stop-Process -Name $nome -Force -ErrorAction Stop
            $mensagens += "SUCESSO: Processo '$nome' foi finalizado."
        }
        catch {
            $mensagens += "ERRO: Falha ao tentar finalizar o processo '$nome'."
        }
    }
    else {
        $mensagens += "INFO: Processo '$nome' não estava em execução."
    }
}

Write-Output ($mensagens -join "`r`n")
