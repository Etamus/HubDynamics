# ===================================================================
# == Script de Automação BW HANA (com limpeza prévia do CSV)       ==
# ===================================================================

import sys
import os
import subprocess
from playwright.sync_api import sync_playwright, TimeoutError

def run_automation(username, password):
    DOWNLOAD_PATH = "C:\\Users\\Robo01\\Desktop"
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # --- NOVA ETAPA: Limpeza Prévia do Arquivo Antigo ---
    # Define o caminho para o arquivo CSV final
    final_csv_path = os.path.join(DOWNLOAD_PATH, "BW.csv")
    
    # Verifica se o arquivo antigo existe e o remove
    if os.path.exists(final_csv_path):
        try:
            print(f"Arquivo antigo encontrado em '{final_csv_path}'. Removendo...")
            os.remove(final_csv_path)
            print("Arquivo antigo removido com sucesso.")
        except Exception as e:
            # Se não conseguir remover, avisa o erro mas continua a execução
            print(f"AVISO: Não foi possível remover o arquivo antigo. Causa: {e}")
    # -----------------------------------------------------------

    with sync_playwright() as p:
        browser = None
        try:
            print("Iniciando o navegador Google Chrome...")
            browser = p.chromium.launch(channel="chrome", headless=False)
            page = browser.new_page()
            page.set_default_timeout(60000)

            print("Acessando o portal BW HANA...")
            page.goto("https://lar-bi-portal.whirlpool.com/irj/portal")

            print("Preenchendo credenciais...")
            page.fill("#logonuidfield", username)
            page.fill("#logonpassfield", password)
            
            print("Realizando login...")
            page.get_by_role("button", name="Efetuar logon").click()
            page.wait_for_load_state("networkidle")
            
            if page.locator("#logonpassfield").is_visible(timeout=5000):
                raise Exception("Login falhou. Verifique as credenciais.")
            
            print("Login realizado com sucesso.")

            print("Acessando favoritos...")
            page.click("text=Favoritos")

            print("Clicando no relatório 'IMPUT PÇS'...")
            page.click("text=IMPUT PÇS")
            page.wait_for_load_state("networkidle")

            print("Entrando no iframe principal: #contentAreaFrame")
            frame_principal = page.frame_locator("#contentAreaFrame")
            
            print("Entrando no iframe do relatório: #isolatedWorkArea")
            frame_relatorio = frame_principal.frame_locator("#isolatedWorkArea")
            
            print("Clicando em OK para gerar o relatório...")
            frame_relatorio.locator("#DLG_VARIABLE_dlgBase_BTNOK").click()
            
            export_button_selector = "#BUTTON_TOOLBAR_2_btn3_acButton"
            
            print(f"Aguardando o botão de exportação ('{export_button_selector}') aparecer...")
            frame_relatorio.locator(export_button_selector).wait_for(state="visible", timeout=120000)
            
            print("Clicando em 'Exportar para Excel'...")
            with page.expect_download(timeout=120000) as download_info:
                frame_relatorio.locator(export_button_selector).click()
            
            download = download_info.value
            
            temp_xls_path = os.path.join(DOWNLOAD_PATH, "BW.xls")
            # O caminho do arquivo final já foi definido no início
            
            print(f"Salvando arquivo temporário em {temp_xls_path}")
            download.save_as(temp_xls_path)
            
            print("Iniciando a conversão de XLS para CSV via PowerShell...")
            caminho_script_conversao = os.path.join(BASE_DIR, "bashes", "convert_xls_to_csv.ps1")
            
            comando_conversao = [
                "powershell.exe", "-ExecutionPolicy", "Bypass",
                "-File", caminho_script_conversao,
                "-CaminhoArquivoXLS", temp_xls_path,
                "-CaminhoArquivoCSV", final_csv_path
            ]
            
            resultado_conversao = subprocess.run(comando_conversao, capture_output=True, text=True, encoding="utf-8")
            
            if resultado_conversao.returncode != 0 or "ERRO:" in resultado_conversao.stdout.upper():
                raise Exception(f"Falha na conversão para CSV: {resultado_conversao.stdout} {resultado_conversao.stderr}")

            print(resultado_conversao.stdout.strip())

            print(f"Removendo arquivo temporário {temp_xls_path}")
            os.remove(temp_xls_path)
            
            print(f"SUCESSO: Arquivo final salvo em {final_csv_path}")

        except TimeoutError as te:
            print(f"ERRO: A automação falhou por timeout. Uma etapa demorou demais. Verifique se os seletores (login, OK, exportar) estão corretos. Detalhes: {te}")
        except Exception as e:
            print(f"ERRO: A automação falhou. Causa: {e}")
        
        finally:
            if browser:
                browser.close()

if __name__ == "__main__":
    # C5: Credenciais lidas de variáveis de ambiente em vez de argumentos de linha de comando.
    # Passadas pelo servidor via env= no subprocess.run — nunca visíveis em 'tasklist'.
    user_arg = os.environ.get('BW_USER', '').strip()
    pass_arg = os.environ.get('BW_PASS', '').strip()

    if not user_arg or not pass_arg:
        print("ERRO: BW_USER e BW_PASS devem ser definidos como variáveis de ambiente.")
        sys.exit(1)

    run_automation(user_arg, pass_arg)