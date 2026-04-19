import subprocess
import os
import sys
import hmac
import bcrypt
import concurrent.futures  # timeout na inferência LLM
import json
import string
from flask import (
    Flask, render_template, request, jsonify, send_from_directory,
    session, redirect, url_for
)
from werkzeug.utils import secure_filename
import time
import secrets
import datetime
from datetime import timezone, timedelta
from dotenv import load_dotenv

# Carrega variáveis de ambiente do .env (deve ser chamado antes de usar os.environ)
load_dotenv()

# Importa todas as funções de banco de dados (substitui os JSONs)
from database import (
    init_db, load_users, save_users,
    load_requests, save_requests,
    load_schedules, save_schedules,
    load_dashboards, save_dashboards,
    load_automations, save_automations,
    audit,
)

from llama_cpp import Llama
import glob

# --- INICIALIZAÇÃO DO MODELO LLAMA (llama.cpp) ---
MODELOS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'model')
os.makedirs(MODELOS_DIR, exist_ok=True)

def _load_llama_model():
    gguf_files = glob.glob(os.path.join(MODELOS_DIR, '*.gguf'))
    if not gguf_files:
        print('AVISO: Nenhum modelo .gguf encontrado em /model. O assistente estará indisponível.')
        return None
    model_path = gguf_files[0]
    n_cpu = os.cpu_count() or 4
    # Reserva 1 core para o SO/Flask não congelar durante a inferência
    n_threads = max(1, n_cpu - 1)
    print(f'Carregando modelo: {os.path.basename(model_path)} | CPUs: {n_cpu} | threads: {n_threads}')
    try:
        llm = Llama(
            model_path=model_path,
            n_ctx=4096,
            n_threads=n_threads,
            n_threads_batch=n_threads,
            n_batch=512,
            n_ubatch=512,
            # flash_attn=False (padrão) — no CPU causa travamentos; é feature de GPU
            # use_mlock=False (padrão) — mlock no Windows pode falhar silenciosamente
            verbose=True  # Mantenha True para diagnóstico; oculta detalhes mas mostra erros críticos
        )
        print('Modelo carregado com sucesso. Executando warm-up...')
        try:
            llm.create_chat_completion(
                messages=[{"role": "user", "content": "oi"}],
                max_tokens=1,
            )
            print('Warm-up concluído. Assistente pronto.')
        except Exception as e:
            print(f'AVISO: warm-up falhou ({e}), prosseguindo mesmo assim.')
        return llm
    except Exception as e:
        print(f'ERRO ao carregar modelo llama: {e}')
        return None

llama_model = _load_llama_model()

# Cache do prompt.json em memória — recarrega automaticamente se o arquivo mudar
_prompt_cache: dict = {}
_prompt_cache_mtime: float = 0.0

def _get_context_data() -> dict:
    """Retorna o conteúdo do prompt.json usando cache em memória."""
    global _prompt_cache, _prompt_cache_mtime
    try:
        mtime = os.path.getmtime(LLAMA_CONTEXT_FILE)
        if mtime != _prompt_cache_mtime:
            with open(LLAMA_CONTEXT_FILE, 'r', encoding='utf-8') as f:
                _prompt_cache = json.load(f)
            _prompt_cache_mtime = mtime
    except Exception as e:
        print(f'AVISO: não recarregou prompt.json: {e}')
    return _prompt_cache

app = Flask(__name__)

# C2: Chave secreta carregada do .env — nunca hardcoded
_secret = os.environ.get('FLASK_SECRET_KEY')
if not _secret or _secret == 'TROQUE_POR_UMA_CHAVE_ALEATORIA_FORTE':
    raise RuntimeError(
        'FLASK_SECRET_KEY não configurada. '
        'Edite o arquivo .env e defina uma chave aleatória segura.'
    )
app.secret_key = _secret

# A2: Flags de segurança dos cookies de sessão
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Strict',
    SESSION_COOKIE_SECURE=bool(os.environ.get('SSL_CERT')),
)

BRASILIA_TZ = timezone(timedelta(hours=-3))
LOGIN_ATTEMPT_LIMIT = 5
LOCKOUT_DURATION = 300

# --- Variáveis de Estado Globais ---
is_sap_logged_in = False
is_bw_hana_logged_in = False
is_salesforce_logged_in = False
last_bw_creds = {}
last_salesforce_creds = {}
# C4: Timestamps de expiração das credenciais (15 min)
BW_CREDS_TTL = 15 * 60  # segundos
last_bw_creds_expiry: float = 0.0
last_salesforce_creds_expiry: float = 0.0

# --- Caminhos e Configurações ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DRIVE_ROOT = os.path.join(BASE_DIR, "drive")
LLAMA_CONTEXT_FILE = os.path.join(BASE_DIR, "prompt.json")

CACHE_DIR = os.path.join(BASE_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# Scripts .ps1 agora ficam na subpasta bashes/
BASHES_DIR = os.path.join(BASE_DIR, "bashes")
SCRIPT_RUNNER_SIMPLES    = os.path.join(BASHES_DIR, "runner.ps1")
SCRIPT_RUNNER_SAP_LOGIN  = os.path.join(BASHES_DIR, "sap_login_runner.ps1")
SCRIPT_CLEANUP           = os.path.join(BASHES_DIR, "cleanup_processes.ps1")
SCRIPT_BW_HANA           = os.path.join(BASE_DIR,   "bw_hana_extractor.py")
DOWNLOAD_DIR             = os.path.join(BASE_DIR,   "automations")

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg'} # REMOVIDO 'gif'

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# --- HELPERS DE APLICAÇÃO ---
# load_users / save_users / load_schedules / save_schedules /
# load_requests / save_requests / load_dashboards / save_dashboards /
# load_automations / save_automations / audit
# → importados de database.py no topo do arquivo.

def generate_access_code(existing_tokens=None, length=6):
    """Gera um código curto alfanumérico para acompanhar solicitações."""
    alphabet = string.ascii_uppercase + string.digits
    existing_tokens = set(existing_tokens or [])
    while True:
        code = ''.join(secrets.choice(alphabet) for _ in range(length))
        if code not in existing_tokens:
            return code

def generate_initial_password(length=8):
    """Gera uma senha inicial com letras, números e caracteres especiais."""
    alphabet = string.ascii_letters + string.digits + "@#$%&*?!"
    return ''.join(secrets.choice(alphabet) for _ in range(length))
        



# --- NOVA FUNCÃO: Áreas de hub disponíveis ---
HUB_AREAS = ['Spare Parts', 'Finished Goods']

# ---------------------------------------------------------------------------
# A4: CSRF — token na sessão, verificado em todos os POST/PUT/DELETE
# ---------------------------------------------------------------------------

@app.context_processor
def inject_csrf_token():
    """Injeta csrf_token em todos os templates para uso no meta tag."""
    if '_csrf_token' not in session:
        session['_csrf_token'] = secrets.token_urlsafe(32)
    return {'csrf_token': session['_csrf_token']}


@app.before_request
def verify_csrf():
    """Bloqueia requisições POST/PUT/DELETE sem token CSRF válido."""
    if request.method not in ('POST', 'PUT', 'DELETE', 'PATCH'):
        return
    # Gera o token se ainda não existir (primeira requisição sem GET prévio)
    session_token = session.get('_csrf_token')
    if not session_token:
        session['_csrf_token'] = secrets.token_urlsafe(32)
        session_token = session['_csrf_token']
    token = (
        request.headers.get('X-CSRF-Token')
        or request.form.get('_csrf_token')
        or (request.get_json(silent=True) or {}).get('_csrf_token')
    )
    if not token or not hmac.compare_digest(token, session_token):
        return jsonify({'status': 'erro', 'mensagem': 'Token CSRF inválido.'}), 403


# A5: Headers de segurança HTTP em todas as respostas
@app.after_request
def set_security_headers(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['Referrer-Policy'] = 'no-referrer'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Content-Security-Policy'] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' cdnjs.cloudflare.com; "
        "style-src 'self' 'unsafe-inline' cdnjs.cloudflare.com; "
        "font-src 'self' cdnjs.cloudflare.com; "
        "img-src 'self' data:; "
        "frame-src *;"
    )
    return response



def filter_automations_by_area(automations, user_area):
    """Filtra automações pelo hub_area do usuário. Spare Parts vê tudo suas, Finished Goods vê as suas."""
    return {k: v for k, v in automations.items()
            if v.get('hub_area', 'Spare Parts') == user_area}


def filter_dashboards_by_area(dashboards_data, user_area):
    """Retorna uma cópia dos dashboards com apenas os itens do hub_area do usuário.
    Remove áreas vazias após a filtragem e também plataformas que ficam sem nenhuma área."""
    import copy
    filtered = copy.deepcopy(dashboards_data)
    for system_key in list(filtered.keys()):
        system_data = filtered[system_key]
        areas = system_data.get('areas', {})
        for area_key in list(areas.keys()):
            area_data = areas[area_key]
            if isinstance(area_data, dict) and 'items' in area_data:
                area_data['items'] = [
                    item for item in area_data['items']
                    if item.get('hub_area', 'Spare Parts') == user_area
                ]
                if not area_data['items']:
                    del areas[area_key]
        # Remove a plataforma inteira se não sobrou nenhuma área com conteúdo
        if not areas and 'areas' in system_data:
            del filtered[system_key]
    return filtered

def filter_dashboards_general_only(dashboards_data):
    """Retorna apenas itens marcados como general=True (para usuários não logados)."""
    import copy
    filtered = copy.deepcopy(dashboards_data)
    for system_key in list(filtered.keys()):
        system_data = filtered[system_key]
        areas = system_data.get('areas', {})
        for area_key in list(areas.keys()):
            area_data = areas[area_key]
            if isinstance(area_data, dict) and 'items' in area_data:
                area_data['items'] = [
                    item for item in area_data['items']
                    if item.get('general', False)
                ]
                if not area_data['items']:
                    del areas[area_key]
        if not areas and 'areas' in system_data:
            del filtered[system_key]
    return filtered

def get_user_profile_data():
    """Retorna dados de perfil necessários para renderização do Hub."""
    username = session.get('username')
    profile_data = {
        'username': None, 
        'profile_image': "/static/icones/default_profile.png", 
        'role': None
    }
    
    if username:
        users = load_users()
        user_data = users.get(username, {})
        
        # Obtém o nome do arquivo de imagem do usuário (se existir)
        image_filename = user_data.get('profile_image')
        
        if image_filename:
            # Adiciona um timestamp para evitar problemas de cache no navegador
            profile_data['profile_image'] = f'/cache/{image_filename}?t={int(time.time())}'

        # --- LÓGICA DE ROLE (Req 1) ---
        role = user_data.get('role', 'Analista') # Padrão 'Analista'
        # Trata 'admin' como 'Executor'
        if username.lower() == 'admin':
            role = 'Executor'
            
        profile_data['role'] = role
        # -----------------------------    
            
    return profile_data

# --- FIM DAS NOVAS FUNÇÕES ---

def find_file_by_prefix(directory, prefix):
    """Procura por um arquivo baseado no prefixo."""
    try:
        for filename in os.listdir(directory):
            if filename.startswith(prefix): 
                return filename
    except FileNotFoundError: 
        return None
    return None

# --- ROTAS DE PÁGINAS (MODIFICADAS) ---

@app.route('/')
def hub():
    profile_data = get_user_profile_data()
    return render_template('hub.html', **profile_data)

@app.route('/automacao')
def automacao():
    global is_sap_logged_in, is_bw_hana_logged_in, is_salesforce_logged_in
    global last_bw_creds, last_salesforce_creds, last_bw_creds_expiry, last_salesforce_creds_expiry
    is_sap_logged_in = False
    is_bw_hana_logged_in = False
    is_salesforce_logged_in = False
    last_bw_creds = {}
    last_salesforce_creds = {}
    last_bw_creds_expiry = 0.0
    last_salesforce_creds_expiry = 0.0
    
    initial_zv62n_file = find_file_by_prefix(DOWNLOAD_DIR, "ZV62N")
    automations = load_automations()
    
    # --- FILTRAGEM POR ÁREA DE HUB ---
    profile_data = get_user_profile_data()
    username = session.get('username')
    users = load_users()
    user_area = session.get('active_area') or (users.get(username, {}).get('area', 'Spare Parts') if username else 'Spare Parts')
    if username and username.lower() == 'admin' and not session.get('active_area'):
        user_area = 'Spare Parts'
    automations = filter_automations_by_area(automations, user_area)
    # --- FIM DA FILTRAGEM ---

    # Detecta quais plataformas têm automações para a área atual
    has_sap         = any(v.get('type', '').lower() == 'sap'         for v in automations.values())
    has_bw          = any(v.get('type', '').lower() == 'bw'          for v in automations.values())
    has_salesforce  = any(v.get('type', '').lower() == 'salesforce'  for v in automations.values())

    return render_template(
        'automacao.html',
        macros=automations,
        initial_zv62n_file=initial_zv62n_file,
        is_hub_logged_in=session.get('username'),
        active_area=user_area,
        role=profile_data['role'],
        profile_image=profile_data['profile_image'],
        profile_username=profile_data['username'],
        has_sap=has_sap,
        has_bw=has_bw,
        has_salesforce=has_salesforce,
    )

@app.route('/dashboards')
def dashboards():
    all_dashboards_data = load_dashboards()
    
    # --- FILTRAGEM POR ÁREA DE HUB ---
    profile_data = get_user_profile_data()
    username = session.get('username')
    users = load_users()
    user_area = session.get('active_area') or (users.get(username, {}).get('area', 'Spare Parts') if username else 'Spare Parts')
    if username and username.lower() == 'admin' and not session.get('active_area'):
        user_area = 'Spare Parts'

    # Detecção automática de área: se ?open=ID foi passado, usa a área do item
    open_id = request.args.get('open')
    if open_id and username:
        found_area = None
        search_done = False
        for system_data in all_dashboards_data.values():
            if search_done:
                break
            if isinstance(system_data, dict):
                for area_data in system_data.get('areas', {}).values():
                    for item in area_data.get('items', []):
                        if item.get('id') == open_id:
                            found_area = item.get('hub_area', 'Spare Parts')
                            search_done = True
                            break
                    if search_done:
                        break
        if found_area:
            u = users.get(username, {})
            allowed = u.get('allowed_areas', [u.get('area', 'Spare Parts')])
            if username.lower() == 'admin':
                allowed = list(HUB_AREAS)
            if found_area in allowed:
                user_area = found_area
                session['active_area'] = found_area

    if username:
        dashboards_data = filter_dashboards_by_area(all_dashboards_data, user_area)
    else:
        dashboards_data = filter_dashboards_general_only(all_dashboards_data)
    # --- FIM DA FILTRAGEM ---

    return render_template(
        'dashboards.html',
        is_hub_logged_in=session.get('username'),
        dashboards_data=dashboards_data,
        active_area=user_area,
        role=profile_data['role'], # <-- ADICIONADO
        profile_image=profile_data['profile_image'],
        profile_username=profile_data['username']
    )

@app.route('/drive')
def drive():
    # --- INÍCIO DA MODIFICAÇÃO ---
    profile_data = get_user_profile_data()
    username = session.get('username')
    users = load_users()
    user_area = session.get('active_area') or (users.get(username, {}).get('area', 'Spare Parts') if username else 'Spare Parts')
    # --- FIM DA MODIFICAÇÃO ---
    
    return render_template(
        'drive.html', 
        is_hub_logged_in=session.get('username'),
        active_area=user_area,
        role=profile_data['role'],
        profile_image=profile_data['profile_image'],
        profile_username=profile_data['username']
    ) # <-- ADICIONADO


# --- ROTAS DE API (DRIVE) ---
@app.route('/api/browse')
def api_browse():
    relative_path = request.args.get('path', '')
    safe_relative_path = os.path.normpath(relative_path).lstrip('.\\/')
    current_path = os.path.join(DRIVE_ROOT, safe_relative_path)
    
    if not os.path.abspath(current_path).startswith(os.path.abspath(DRIVE_ROOT)):
        return jsonify({"error": "Acesso negado."}), 403
        
    try:
        items = os.listdir(current_path)
        content = []
        for item in items:
            item_path = os.path.join(current_path, item)
            is_dir = os.path.isdir(item_path)
            
            # --- INÍCIO DA MODIFICAÇÃO (Req 1) ---
            try:
                # Pega as estatísticas do arquivo/pasta
                stats = os.stat(item_path)
                mod_time_stamp = stats.st_mtime
                # Converte o timestamp para formato ISO (JS consegue ler)
                mod_date = datetime.datetime.fromtimestamp(mod_time_stamp).isoformat()
                
                # Pega o tamanho (se não for diretório)
                size = stats.st_size if not is_dir else 0
                
            except (FileNotFoundError, PermissionError):
                # Caso o arquivo seja bloqueado ou excluído durante a leitura
                mod_date = None
                size = 0
            # --- FIM DA MODIFICAÇÃO ---

            content.append({
                "name": item, 
                "is_dir": is_dir,
                "mod_date": mod_date, # Adicionado
                "size": size        # Adicionado
            })
    
        content.sort(key=lambda x: (not x['is_dir'], x['name'].lower()))
        
        return jsonify({
            "path": safe_relative_path,
            "content": content
        })
    except FileNotFoundError:
        return jsonify({"error": "Pasta não encontrada."}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/download')
def api_download():
    relative_path = request.args.get('path', '')
    safe_relative_path = os.path.normpath(relative_path).lstrip('.\\/')
    full_path = os.path.join(DRIVE_ROOT, safe_relative_path)
    
    if not os.path.abspath(full_path).startswith(os.path.abspath(DRIVE_ROOT)) or os.path.isdir(full_path):
        return "Acesso negado.", 403
        
    try:
        directory = os.path.dirname(full_path)
        filename = os.path.basename(full_path)
        return send_from_directory(directory, filename, as_attachment=True)
    except FileNotFoundError:
        return "Arquivo não encontrado.", 404

# --- NOVAS ROTAS DE API (PERFIL E CACHE) ---

@app.route('/api/profile/upload', methods=['POST'])
def profile_upload():
    username = session.get('username')
    if not username:
        return jsonify({"status": "erro", "mensagem": "Usuário não logado."}), 401
    
    if 'file' not in request.files:
        return jsonify({"status": "erro", "mensagem": "Nenhuma imagem selecionada."}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "erro", "mensagem": "Nome de arquivo inválido."}), 400
    
    if file and allowed_file(file.filename):
        # Cria um nome seguro e único baseado no username
        file_extension = file.filename.rsplit('.', 1)[1].lower()
        new_filename = f'{username}.{file_extension}'
        filepath = os.path.join(CACHE_DIR, new_filename)

        # 1. Salva o arquivo no disco
        file.save(filepath)

        # 2. Atualiza o JSON de usuários
        users = load_users()
        if username in users:
            # 3. Opcional: Remove a imagem antiga se a extensão for diferente
            old_filename = users[username].get('profile_image')
            if old_filename and old_filename != new_filename and os.path.exists(os.path.join(CACHE_DIR, old_filename)):
                 os.remove(os.path.join(CACHE_DIR, old_filename))
                 
            users[username]['profile_image'] = new_filename
            save_users(users)
            
            return jsonify({"status": "sucesso", "mensagem": "Imagem de perfil atualizada.", "url": f'/cache/{new_filename}?t={int(time.time())}'})
        
        return jsonify({"status": "erro", "mensagem": "Erro ao salvar perfil do usuário."}), 500
    
    return jsonify({"status": "erro", "mensagem": "Tipo de arquivo não permitido."}), 400

@app.route('/cache/<filename>')
def serve_cache(filename):
    """Serve arquivos da pasta 'cache'."""
    return send_from_directory(CACHE_DIR, filename)

# Em: servidor_unico.py (Adicione junto às rotas de API do Hub)

@app.route('/api/profile/remove-image', methods=['POST'])
def profile_remove_image():
    username = session.get('username')
    if not username:
        return jsonify({"status": "erro", "mensagem": "Usuário não logado."}), 401

    users = load_users()
    if username in users:
        old_filename = users[username].get('profile_image')
        
        # 1. Remove o arquivo do cache se ele existir
        if old_filename:
            filepath = os.path.join(CACHE_DIR, old_filename)
            if os.path.exists(filepath):
                os.remove(filepath)
        
        # 2. Remove a referência do JSON e salva
        users[username]['profile_image'] = None
        save_users(users)
        
        return jsonify({"status": "sucesso", "mensagem": "Imagem removida.", "default_url": "/static/icones/default_profile.png"})
    
    return jsonify({"status": "erro", "mensagem": "Usuário não encontrado."}), 404

# (Req 1) NOVO: Rota para salvar o Nome de Usuário
@app.route('/api/profile/update-details', methods=['POST'])
def profile_update_details():
    username = session.get('username')
    if not username:
        return jsonify({"status": "erro", "mensagem": "Usuário não logado."}), 401
    
    display_name = request.json.get('display_name', None)
    
    users = load_users()
    if username in users:
        users[username]['display_name'] = display_name
        save_users(users)
        return jsonify({"status": "sucesso", "mensagem": "Nome de usuário atualizado.", "display_name": display_name})
    
    return jsonify({"status": "erro", "mensagem": "Usuário não encontrado."}), 404

# (Req 2) NOVO: Rota para alterar a senha
@app.route('/api/profile/change-password', methods=['POST'])
def profile_change_password():
    username = session.get('username')
    if not username:
        return jsonify({"status": "erro", "mensagem": "Usuário não logado."}), 401
    
    current_pass = request.json.get('current_pass')
    new_pass = request.json.get('new_pass')

    if not current_pass or not new_pass:
        return jsonify({"status": "erro", "mensagem": "Todos os campos são obrigatórios."}), 400

    users = load_users()
    user_data = users.get(username)

    if not user_data:
        return jsonify({"status": "erro", "mensagem": "Usuário não encontrado."}), 404

    # C1/C6: Valida a senha atual com bcrypt (resistente a timing attacks)
    try:
        senha_correta = bcrypt.checkpw(current_pass.encode(), user_data['password'].encode())
    except Exception:
        senha_correta = False

    if not senha_correta:
        return jsonify({"status": "erro", "mensagem": "A senha atual está incorreta."}), 403

    # C1: Hash da nova senha antes de salvar
    user_data['password'] = bcrypt.hashpw(new_pass.encode(), bcrypt.gensalt()).decode()
    # Reseta tentativas de login por segurança
    user_data['login_attempts'] = 0
    user_data['lockout_until'] = None
    save_users(users)
    audit(username, 'change_password')
    
    return jsonify({"status": "sucesso", "mensagem": "Senha alterada com sucesso!"})

# (Req 3) NOVO: Rota para buscar atividades do usuário
@app.route('/api/profile/get-activity')
def profile_get_activity():
    username = session.get('username')
    if not username:
        return jsonify({"status": "erro", "mensagem": "Usuário não logado."}), 401

    all_schedules = load_schedules()
    user_activity = []
    
    # Filtra a fila global
    queue = all_schedules.get("global_schedule", {}).get("queue", [])
    user_queue = [j for j in queue if j.get('creator') == username]
    user_activity.extend(user_queue)

    # Filtra o histórico global
    history = all_schedules.get("global_schedule", {}).get("history", [])
    user_history = [j for j in history if j.get('creator') == username]
    user_activity.extend(user_history)

    # Ordena por data (mais recentes primeiro) e limita
    try:
        user_activity.sort(key=lambda x: x.get('startTime', 0), reverse=True)
    except Exception:
        pass # Ignora falhas de ordenação

    # Retorna apenas as 10 atividades mais recentes
    return jsonify({"status": "sucesso", "activity": user_activity[:10]})

# --- ROTAS DE API (HUB LOGIN & CONEXÕES) ---

@app.route('/api/hub/login', methods=['POST'])
def hub_login():
    """Login do usuário principal do Hub."""
    users = load_users()
    username = request.form['username']
    password = request.form['password']
    
    user_data = users.get(username)
    
    # --- LÓGICA DE BLOQUEIO ---
    if user_data:
        # 1. Verifica se o usuário está bloqueado
        lockout_until = user_data.get('lockout_until')
        if lockout_until and datetime.datetime.now(BRASILIA_TZ) < datetime.datetime.fromisoformat(lockout_until):
            remaining_seconds = int((datetime.datetime.fromisoformat(lockout_until) - datetime.datetime.now(BRASILIA_TZ)).total_seconds())
            return jsonify({"status": "erro", "mensagem": f"Usuário bloqueado. Tente novamente em {remaining_seconds} segundos."}), 429
        
        # 2. Se a senha estiver errada
        # C1/C6: bcrypt.checkpw é resistente a timing attacks
        try:
            senha_correta = bcrypt.checkpw(password.encode(), user_data['password'].encode())
        except Exception:
            senha_correta = False

        if not senha_correta:
            attempts = user_data.get('login_attempts', 0) + 1
            user_data['login_attempts'] = attempts
            
            if attempts >= LOGIN_ATTEMPT_LIMIT:
                # Bloqueia o usuário
                user_data['lockout_until'] = (datetime.datetime.now(BRASILIA_TZ) + timedelta(seconds=LOCKOUT_DURATION)).isoformat()
                user_data['login_attempts'] = 0 # Reseta a contagem
                save_users(users)
                return jsonify({"status": "erro", "mensagem": f"Muitas tentativas falhas. Usuário bloqueado por 5 minutos."}), 429
            
            # --- INÍCIO DA MODIFICAÇÃO (Req 1) ---
            remaining_attempts = LOGIN_ATTEMPT_LIMIT - attempts
            
            # Cria a segunda linha da mensagem
            attempts_text = f"{remaining_attempts} {'tentativa restante' if remaining_attempts == 1 else 'tentativas restantes'}."
            # Junta as duas linhas com um caractere de nova linha (\n)
            mensagem_erro = f"Usuário ou senha inválidos.\n{attempts_text}"
            
            save_users(users)
            # Retorna a nova mensagem
            return jsonify({"status": "erro", "mensagem": mensagem_erro}), 401
            # --- FIM DA MODIFICAÇÃO ---
            
        # 3. Se a senha estiver correta
        # Reseta o bloqueio e tentativas no login bem-sucedido
        user_data['login_attempts'] = 0
        user_data['lockout_until'] = None
        save_users(users)
        # M2: Registra login bem-sucedido no audit log
        audit(username, 'login', f'IP: {request.remote_addr}')
        
    elif not user_data:
         return jsonify({"status": "erro", "mensagem": "Usuário ou senha inválidos."}), 401
    # --- FIM DA LÓGICA DE BLOQUEIO ---

    # (Código existente para login bem-sucedido)
    session['username'] = username
    
    image_filename = user_data.get('profile_image')
    image_url = f'/cache/{image_filename}?t={int(time.time())}' if image_filename else "/static/icones/default_profile.png"
    area = user_data.get('area', 'N/A')
    role = user_data.get('role', 'Analista')
    if username.lower() == 'admin':
        role = 'Executor'
    
    allowed_areas = user_data.get('allowed_areas', [area])
    if username.lower() == 'admin':
        allowed_areas = HUB_AREAS
        
    display_name = user_data.get('display_name', None) 
    return jsonify({"status": "sucesso", "username": username, "display_name": display_name, "profile_image": image_url, "area": area, "allowed_areas": allowed_areas, "role": role})

@app.route('/api/hub/logout', methods=['POST'])
def hub_logout():
    """Logout do usuário principal do Hub."""
    username = session.get('username')
    session.pop('username', None)
    if username:
        audit(username, 'logout')
    return jsonify({"status": "sucesso"})

@app.route('/api/hub/check-session')
def check_session():
    """Verifica se há um usuário logado na sessão do Hub."""
    username = session.get('username')
    if username:
        # Puxa a imagem do perfil para retornar na sessão
        users = load_users()
        user_data = users.get(username, {})
        image_filename = user_data.get('profile_image')
        
        # --- CORREÇÃO APLICADA AQUI ---
        image_url = f'/cache/{image_filename}?t={int(time.time())}' if image_filename else "/static/icones/default_profile.png"

        # --- ADICIONADO: Enviar a Área do usuário ---
        area = user_data.get('area', 'N/A') # 'N/A' como fallback

        role = user_data.get('role', 'Analista')
        
        allowed_areas = user_data.get('allowed_areas', [area])
        if username.lower() == 'admin':
            allowed_areas = HUB_AREAS
        
        display_name = user_data.get('display_name', None) # <-- ADICIONE ESTA LINHA
        return jsonify({"status": "logado", "username": username, "display_name": display_name, "profile_image": image_url, "area": area, "allowed_areas": allowed_areas, "role": role})
    
    # --- CORREÇÃO APLICADA AQUI ---
    return jsonify({"status": "deslogado", "profile_image": "/static/icones/default_profile.png"})

@app.route('/api/hub/set-active-area', methods=['POST'])
def set_active_area():
    """Define a área ativa da sessão do usuário."""
    username = session.get('username')
    if not username:
        return jsonify({"status": "erro", "mensagem": "Não logado."}), 401
    new_area = request.json.get('area')
    if new_area not in HUB_AREAS:
        return jsonify({"status": "erro", "mensagem": "Área inválida."}), 400
    users = load_users()
    user_data = users.get(username, {})
    allowed = user_data.get('allowed_areas', [user_data.get('area', 'Spare Parts')])
    if username.lower() == 'admin':
        allowed = HUB_AREAS
    if new_area not in allowed:
        return jsonify({"status": "erro", "mensagem": "Acesso negado a esta área."}), 403
    session['active_area'] = new_area
    return jsonify({"status": "sucesso", "area": new_area})

@app.route('/api/scheduler/load')
def scheduler_load():
    """Carrega a fila e o histórico do usuário logado."""
    schedule_key = "global_schedule"
        
    all_schedules = load_schedules()
    user_schedule = all_schedules.get(schedule_key, {"queue": [], "history": []})
    return jsonify(user_schedule)

@app.route('/api/scheduler/save', methods=['POST'])
def scheduler_save():
    """Salva a fila e o histórico do usuário logado."""
    schedule_key = "global_schedule"
    
    data = request.json
    job_queue = data.get('queue', [])
    job_history = data.get('history', [])
    
    all_schedules = load_schedules()
    all_schedules[schedule_key] = {
        "queue": job_queue,
        "history": job_history
    }
    save_schedules(all_schedules)
    
    return jsonify({"status": "sucesso", "mensagem": "Agendamento salvo."})

@app.route('/api/hub/get-connections')
def get_connections():
    """Busca as conexões SAP/BW salvas para o usuário logado."""
    username = session.get('username')
    if not username:
        return jsonify({"status": "erro", "mensagem": "Usuário não logado."}), 401
        
    users = load_users()
    connections = users.get(username, {}).get('connections', {})
    return jsonify({"status": "sucesso", "connections": connections})

@app.route('/api/hub/remove-connection/<system>', methods=['POST'])
def remove_connection(system):
    """Remove uma conexão SAP, BW ou Tableau salva.""" # (Texto atualizado)
    username = session.get('username')
    if not username:
        return jsonify({"status": "erro", "mensagem": "Usuário não logado."}), 401
    
    # --- INÍCIO DA MODIFICAÇÃO ---
    if system not in ['sap', 'bw', 'tableau']:
    # --- FIM DA MODIFICAÇÃO ---
        return jsonify({"status": "erro", "mensagem": "Sistema inválido."}), 400

    users = load_users()
    if username in users and system in users[username]['connections']:
        users[username]['connections'][system] = None
        save_users(users)
        return jsonify({"status": "sucesso", "mensagem": f"Conexão {system.upper()} removida."})
    
    return jsonify({"status": "erro", "mensagem": "Conexão não encontrada."}), 404

# --- NOVAS ROTAS DE API (REGISTRO DE USUÁRIO) ---

@app.route('/api/hub/register', methods=['POST'])
def hub_register():
    data = request.json
    username = data.get('username')
    area = data.get('area')
    role = data.get('role')

    if not username or not area or not role:
        return jsonify({"status": "erro", "mensagem": "Todos os campos são obrigatórios."}), 400

    users = load_users()
    if username in users:
        return jsonify({"status": "erro", "mensagem": "Este login de funcionário já está cadastrado."}), 400

    requests = load_requests()
    # Verifica se já existe um pedido pendente para este usuário
    if any(r['username'] == username and r['status'] == 'Aguardando Aprovação' for r in requests.values()):
        return jsonify({"status": "erro", "mensagem": "Você já possui uma solicitação pendente."}), 400

    token = generate_access_code(requests.keys())
    requests[token] = {
        "username": username,
        "area": area,
        "role": role,
        "status": "Aguardando Aprovação",
        "request_date": datetime.datetime.now(BRASILIA_TZ).isoformat(), # <-- CORREÇÃO DE FUSO
        "justification": None,
        "expiration_date": None,
        "generated_password": None
    }
    save_requests(requests)
    
    return jsonify({"status": "sucesso", "mensagem": "Solicitação enviada. Guarde seu token!", "token": token})

@app.route('/api/hub/consult', methods=['POST'])
def hub_consult():
    data = request.json
    token = data.get('token')
    if not token:
        return jsonify({"status": "erro", "mensagem": "Token é obrigatório."}), 400

    requests = load_requests()
    request_data = requests.get(token)

    if not request_data:
        return jsonify({"status": "erro", "mensagem": "Token inválido ou não encontrado."}), 404
        
    # Verifica se o token aprovado expirou
    if request_data['status'] == 'Aprovado':
        users = load_users()
        if not request_data.get('generated_password'):
            generated_password = generate_initial_password()
            request_data['generated_password'] = generated_password
            requests[token] = request_data
            save_requests(requests)
        if request_data.get('username') and request_data.get('generated_password'):
            username = request_data['username']
            if username not in users:
                raw_pw = request_data['generated_password'] or ''
                users[username] = {
                    "password": bcrypt.hashpw(raw_pw.encode(), bcrypt.gensalt()).decode(),
                    "role": request_data.get('role', 'Analista'),
                    "area": request_data.get('area', 'N/A'),
                    "display_name": None,
                    "profile_image": None,
                    "connections": {
                        "sap": None,
                        "bw": None,
                        "tableau": None
                    },
                    "login_attempts": 0,
                    "lockout_until": None
                }
                save_users(users)
        if request_data.get('expiration_date'):
            try:
                expiration_date = datetime.datetime.fromisoformat(request_data['expiration_date'])
                if datetime.datetime.now(datetime.timezone.utc) > expiration_date:
                    request_data['status'] = 'Expirado'
                    request_data['justification'] = 'O prazo de 7 dias para cadastro de senha expirou. Faça uma nova solicitação.'
                    requests[token] = request_data
                    save_requests(requests)
            except (ValueError, TypeError):
                # Ignora datas de expiração mal formatadas
                pass 
        else:
            # Caso de segurança: Aprovado mas sem data (não deveria acontecer)
            request_data['status'] = 'Erro: Aprovado sem data de expiração'
            
    return jsonify({"status": "sucesso", "request_data": request_data})

@app.route('/api/hub/complete-registration', methods=['POST'])
def hub_complete_registration():
    data = request.json
    token = data.get('token')
    password = data.get('password')

    if not token or not password:
        return jsonify({"status": "erro", "mensagem": "Token e senha são obrigatórios."}), 400

    requests = load_requests()
    request_data = requests.get(token)

    if not request_data or request_data['status'] != 'Aprovado':
        return jsonify({"status": "erro", "mensagem": "Token inválido, expirado ou não aprovado."}), 403
    
    # Verifica a expiração novamente
    expiration_date = datetime.datetime.fromisoformat(request_data['expiration_date'])
    if datetime.datetime.now(datetime.timezone.utc) > expiration_date:
        return jsonify({"status": "erro", "mensagem": "Este token expirou. Faça uma nova solicitação."}), 403

    # Adiciona o usuário ao DB principal
    users = load_users()
    username = request_data['username']
    
    if username in users:
         return jsonify({"status": "erro", "mensagem": "Este usuário já foi registrado. Tente fazer login."}), 400
         
    users[username] = {
        "password": bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode(),
        "role": request_data['role'],
        "area": request_data['area'],
        "display_name": None,
        "profile_image": None,
        "connections": {
            "sap": None,
            "bw": None,
            "tableau": None
        }
    }
    save_users(users)
    
    # Remove o token do DB de solicitações
    del requests[token]
    save_requests(requests)
    
    return jsonify({"status": "sucesso", "mensagem": "Cadastro concluído! Você já pode fazer login."})


# --- ROTAS DE ADMINISTRAÇÃO ---

def is_admin():
    """Verifica se o usuário logado é 'admin'."""
    return session.get('username', '').lower() == 'admin'

@app.route('/api/admin/get-requests')
def admin_get_requests():
    if not is_admin():
        return jsonify({"status": "erro", "mensagem": "Acesso negado."}), 403
        
    requests = load_requests()
    # Filtra apenas solicitações pendentes
    pending_requests = {token: data for token, data in requests.items() if data['status'] == 'Aguardando Aprovação'}
    return jsonify({"status": "sucesso", "requests": pending_requests})

@app.route('/api/admin/approve', methods=['POST'])
def admin_approve():
    if not is_admin():
        return jsonify({"status": "erro", "mensagem": "Acesso negado."}), 403
    
    token = request.json.get('token')
    requests = load_requests()
    
    if token in requests and requests[token]['status'] == 'Aguardando Aprovação':
        request_entry = requests[token]
        users = load_users()
        username = request_entry['username']

        if username in users:
            return jsonify({"status": "erro", "mensagem": "Este usuário já possui um acesso ativo."}), 400

        generated_password = generate_initial_password()
        request_entry['status'] = 'Aprovado'
        request_entry['expiration_date'] = (datetime.datetime.now(BRASILIA_TZ) + timedelta(days=7)).isoformat()
        request_entry['generated_password'] = generated_password
        request_entry['approved_at'] = datetime.datetime.now(BRASILIA_TZ).isoformat()
        requests[token] = request_entry
        save_requests(requests)

        users[username] = {
            "password": bcrypt.hashpw(generated_password.encode(), bcrypt.gensalt()).decode(),
            "role": request_entry.get('role', 'Analista'),
            "area": request_entry.get('area', 'N/A'),
            "display_name": None,
            "profile_image": None,
            "connections": {
                "sap": None,
                "bw": None,
                "tableau": None
            },
            "login_attempts": 0,
            "lockout_until": None
        }
        save_users(users)
        # M3: Audit de aprovação de acesso
        audit(session.get('username'), 'admin_approve', f'Token: {token}, User: {username}')

        return jsonify({"status": "sucesso"})
    
    return jsonify({"status": "erro", "mensagem": "Solicitação não encontrada ou já processada."}), 404

@app.route('/api/admin/reject', methods=['POST'])
def admin_reject():
    if not is_admin():
        return jsonify({"status": "erro", "mensagem": "Acesso negado."}), 403
    
    token = request.json.get('token')
    justification = request.json.get('justification')
    
    if not justification:
        return jsonify({"status": "erro", "mensagem": "A justificativa é obrigatória."}), 400
        
    requests = load_requests()
    
    if token in requests and requests[token]['status'] == 'Aguardando Aprovação':
        requests[token]['status'] = 'Reprovado'
        requests[token]['justification'] = justification
        save_requests(requests)
        audit(session.get('username'), 'admin_reject', f'Token: {token}')
        return jsonify({"status": "sucesso"})
    
    return jsonify({"status": "erro", "mensagem": "Solicitação não encontrada ou já processada."}), 404

@app.route('/api/admin/get-users')
def admin_get_users():
    """Retorna todos os usuários, exceto o próprio admin."""
    if not is_admin():
        return jsonify({"status": "erro", "mensagem": "Acesso negado."}), 403
    
    users = load_users()
    user_list = []
    
    for username, data in users.items():
        # O admin não pode editar a si mesmo nesta interface
        if username.lower() == 'admin':
            continue
            
        user_list.append({
            "username": username,
            "display_name": data.get('display_name', None),
            "area": data.get('area', 'N/A'),
            "allowed_areas": data.get('allowed_areas', [data.get('area', 'N/A')]),
            "role": data.get('role', 'Analista'),
            "password": "",  # Hash bcrypt nunca retornado ao cliente
            "lockout_until": data.get('lockout_until', None)
        })
        
    return jsonify({"status": "sucesso", "users": user_list})

@app.route('/api/admin/update-user', methods=['POST'])
def admin_update_user():
    """Atualiza a senha (opcional), area ou role de um usuário."""
    if not is_admin():
        return jsonify({"status": "erro", "mensagem": "Acesso negado."}), 403
        
    data = request.json
    username = data.get('username')
    new_password = data.get('password') # Pode ser vazio
    new_area = data.get('area')
    new_role = data.get('role')
    new_display_name = data.get('display_name', None)
    new_allowed_areas = data.get('allowed_areas', None)

    if not username or not new_area or not new_role:
        return jsonify({"status": "erro", "mensagem": "Campos obrigatórios (usuário, área, função) ausentes."}), 400
    
    if username.lower() == 'admin':
        return jsonify({"status": "erro", "mensagem": "Não é permitido editar o usuário 'admin' por esta interface."}), 403

    users = load_users()
    if username not in users:
        return jsonify({"status": "erro", "mensagem": "Usuário não encontrado."}), 404

    # Atualiza os dados
    users[username]['area'] = new_area
    users[username]['role'] = new_role
    users[username]['display_name'] = new_display_name
    if new_allowed_areas is not None:
        users[username]['allowed_areas'] = [a for a in new_allowed_areas if a in HUB_AREAS]
    else:
        users[username]['allowed_areas'] = [new_area]
    if new_password:  # C1: Hash bcrypt se nova senha enviada
        users[username]['password'] = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
    
    save_users(users)
    audit(session.get('username'), 'admin_update_user', f'Target: {username}')
    return jsonify({"status": "sucesso", "mensagem": f"Usuário {username} atualizado."})

@app.route('/api/admin/delete-user', methods=['POST'])
def admin_delete_user():
    """Exclui um usuário do sistema."""
    if not is_admin():
        return jsonify({"status": "erro", "mensagem": "Acesso negado."}), 403

    username = request.json.get('username')
    
    if not username or username.lower() == 'admin':
        return jsonify({"status": "erro", "mensagem": "Usuário inválido ou não permitido."}), 400
    
    users = load_users()
    if username in users:
        # Remove o usuário do 'usuarios.json'
        del users[username]
        save_users(users)
        
        # Opcional: Remove o agendamento do usuário (se existir)
        schedules = load_schedules()
        if username in schedules:
            del schedules[username]
            save_schedules(schedules)
            
        return jsonify({"status": "sucesso", "mensagem": f"Usuário {username} excluído."})
    
    return jsonify({"status": "erro", "mensagem": "Usuário não encontrado."}), 404

@app.route('/api/admin/unlock-user', methods=['POST'])
def admin_unlock_user():
    """Define 'lockout_until' como None para um usuário."""
    if not is_admin():
        return jsonify({"status": "erro", "mensagem": "Acesso negado."}), 403
        
    username = request.json.get('username')
    if not username:
        return jsonify({"status": "erro", "mensagem": "Nome de usuário ausente."}), 400

    users = load_users()
    if username not in users:
        return jsonify({"status": "erro", "mensagem": "Usuário não encontrado."}), 404
    
    # Define lockout_until como None e reseta as tentativas
    users[username]['lockout_until'] = None
    users[username]['login_attempts'] = 0
    
    save_users(users)
    return jsonify({"status": "sucesso", "mensagem": f"Usuário {username} desbloqueado."})

# --- ROTAS DE API (AUTOMAÇÃO) ---
@app.route('/executar-macro', methods=['POST'])
def executar_macro():
    if not is_sap_logged_in: 
        return jsonify({"status": "erro", "mensagem": "Acesso negado. Por favor, faça o login no SAP primeiro."}), 403
        
    # --- ALTERADO: Carrega automações do JSON ---
    automations = load_automations()
    nome_macro_selecionada = request.form['macro']
    config = automations.get(nome_macro_selecionada)
    # --------------------------------------------
    
    if not config or config.get("type") != "sap": # Garante que é uma macro SAP
        return jsonify({"status": "erro", "mensagem": "Macro não encontrada ou inválida!"}), 400
        
    comando = [ "powershell.exe", "-ExecutionPolicy", "Bypass", "-File", SCRIPT_RUNNER_SIMPLES, "-CaminhoArquivo", config['arquivo'], "-NomeMacro", config['macro'] ]
    contexto = f"tarefa '{nome_macro_selecionada}'"
    resultado = executar_comando_externo(comando, contexto_tarefa=contexto)
    
    if resultado['status'] == 'sucesso' and nome_macro_selecionada == "Base Mãe":
        nome_arquivo_download = find_file_by_prefix(DOWNLOAD_DIR, "ZV62N")
        if nome_arquivo_download: 
            resultado['download_file'] = nome_arquivo_download
        else: 
            resultado['mensagem'] += " (Aviso: arquivo de relatório não encontrado para download.)"
            
    return jsonify(resultado)

@app.route('/executar-bw-hana', methods=['POST'])
def executar_bw_hana():
    global is_bw_hana_logged_in, last_bw_creds, last_bw_creds_expiry

    if not is_bw_hana_logged_in:
        return jsonify({"status": "erro", "mensagem": "Acesso negado. Por favor, faça o login no BW HANA primeiro."}), 403

    # C4: Verifica expiração das credenciais (15 min)
    if not last_bw_creds or time.time() > last_bw_creds_expiry:
        is_bw_hana_logged_in = False
        last_bw_creds = {}
        last_bw_creds_expiry = 0.0
        return jsonify({"status": "erro", "mensagem": "Sessão BW expirada (15 min). Faça login novamente."}), 401

    usuario = last_bw_creds.get('user')
    senha   = last_bw_creds.get('pass')

    # C5: Credenciais passadas via variável de ambiente — não expostoas em args de processo
    env = os.environ.copy()
    env['BW_USER'] = usuario or ''
    env['BW_PASS'] = senha   or ''
    comando = [sys.executable, SCRIPT_BW_HANA]
    resultado = executar_comando_externo(comando, contexto_tarefa="Extração BW HANA", timeout_seconds=600, env=env)

    if resultado['status'] == 'sucesso' and "ERRO:" in resultado['mensagem'].upper():
        resultado['status'] = 'erro'

    return jsonify(resultado)

@app.route('/download/<filename>')
def download_file(filename):
    try: 
        return send_from_directory(DOWNLOAD_DIR, filename, as_attachment=True)
    except FileNotFoundError: 
        return "Arquivo não encontrado.", 404

# --- ROTAS DE LOGIN / LOGOUT (ESTADO) ---

def save_connection_if_requested(system, user, password):
    """Salva a conexão no BD se o usuário estiver logado no Hub."""
    save_conn = request.form.get('save_connection') == 'true'
    username = session.get('username')

    if save_conn and username:
        users = load_users()
        if username in users:
            if 'connections' not in users[username]:
                users[username]['connections'] = {}
            users[username]['connections'][system] = {'user': user, 'pass': password}
            save_users(users)
            # M6: Nunca logar senha — apenas sistema e usuário
            print(f"[INFO] Conexão {system.upper()} salva para o usuário {username}")

@app.route('/login-sap', methods=['POST'])
def login_sap():
    global is_sap_logged_in, is_bw_hana_logged_in, is_salesforce_logged_in, last_bw_creds, last_salesforce_creds
    usuario = request.form['usuario']
    senha = request.form['senha']
    
    executar_comando_externo([ "powershell.exe", "-ExecutionPolicy", "Bypass", "-File", SCRIPT_CLEANUP ], "Limpeza pré-login")

    login_xlsm_path = os.path.join(DOWNLOAD_DIR, "LoginSAP.xlsm")
    
    comando = [ "powershell.exe", "-ExecutionPolicy", "Bypass", "-File", SCRIPT_RUNNER_SAP_LOGIN, "-CaminhoArquivo", login_xlsm_path, "-NomeMacro", "funcSAPOpen", "-Usuario", usuario, "-Senha", senha ]
    resultado = executar_comando_externo(comando, contexto_tarefa="Login SAP", timeout_seconds=30)
    
    if resultado['status'] == 'erro' and 'excedeu o tempo limite' in resultado['mensagem']:
        resultado['mensagem'] = "Login ou senha incorreto, ou o SAP demorou para responder."
        
    if resultado['status'] == 'sucesso':
        is_sap_logged_in = True
        is_bw_hana_logged_in = False
        is_salesforce_logged_in = False
        last_bw_creds = {}
        last_salesforce_creds = {}
        resultado['mensagem'] = "Login realizado com sucesso."
        
        save_connection_if_requested('sap', usuario, senha)
    else:
        is_sap_logged_in = False
        executar_comando_externo([ "powershell.exe", "-ExecutionPolicy", "Bypass", "-File", SCRIPT_CLEANUP ], "Cleanup Pós-Falha de Login")
        
    return jsonify(resultado)

# (Req 1) NOVO: Rota para salvar a conexão do Tableau
@app.route('/api/tableau/login', methods=['POST'])
def login_tableau():
    global is_sap_logged_in, is_bw_hana_logged_in, is_salesforce_logged_in, last_bw_creds, last_salesforce_creds
    
    usuario = request.form['usuario']
    senha = request.form['senha']
    
    # Esta rota apenas salva a conexão, não gerencia um "estado de login"
    # no backend, pois o Tableau é um iframe.
    save_connection_if_requested('tableau', usuario, senha)
    
    # Reinicia os outros estados de login para evitar conflito
    is_sap_logged_in = False
    is_bw_hana_logged_in = False
    is_salesforce_logged_in = False
    last_bw_creds = {}
    last_salesforce_creds = {}
    
    return jsonify({"status": "sucesso", "mensagem": "Credenciais salvas. Abrindo dashboard."})

@app.route('/logout-sap', methods=['POST'])
def logout_sap():
    global is_sap_logged_in
    is_sap_logged_in = False
    
    comando_cleanup = [ "powershell.exe", "-ExecutionPolicy", "Bypass", "-File", SCRIPT_CLEANUP ]
    resultado_cleanup = executar_comando_externo(comando_cleanup, contexto_tarefa="Logout SAP e Limpeza")
    
    return jsonify(resultado_cleanup)

@app.route('/login-bw-hana', methods=['POST'])
def login_bw_hana():
    global is_sap_logged_in, is_bw_hana_logged_in, is_salesforce_logged_in
    global last_bw_creds, last_salesforce_creds, last_bw_creds_expiry, last_salesforce_creds_expiry

    executar_comando_externo([ "powershell.exe", "-ExecutionPolicy", "Bypass", "-File", SCRIPT_CLEANUP ], "Limpeza pré-login")

    usuario = request.form['usuario']
    senha   = request.form['senha']

    last_bw_creds = {'user': usuario, 'pass': senha}
    # C4: Define expiração de 15 minutos
    last_bw_creds_expiry = time.time() + BW_CREDS_TTL
    is_bw_hana_logged_in = True
    is_sap_logged_in = False
    is_salesforce_logged_in = False
    last_salesforce_creds = {}
    last_salesforce_creds_expiry = 0.0

    save_connection_if_requested('bw', usuario, senha)

    return jsonify({"status": "sucesso", "mensagem": "Login realizado com sucesso."})

@app.route('/logout-bw-hana', methods=['POST'])
def logout_bw_hana():
    global is_bw_hana_logged_in, last_bw_creds, last_bw_creds_expiry

    is_bw_hana_logged_in = False
    last_bw_creds = {}
    last_bw_creds_expiry = 0.0

    executar_comando_externo([ "powershell.exe", "-ExecutionPolicy", "Bypass", "-File", SCRIPT_CLEANUP ], "Cleanup Pós-BW")

    return jsonify({"status": "sucesso", "mensagem": "Estado de login BW HANA reiniciado."})

@app.route('/login-salesforce', methods=['POST'])
def login_salesforce():
    global is_sap_logged_in, is_bw_hana_logged_in, is_salesforce_logged_in
    global last_bw_creds, last_salesforce_creds, last_bw_creds_expiry, last_salesforce_creds_expiry

    executar_comando_externo([ "powershell.exe", "-ExecutionPolicy", "Bypass", "-File", SCRIPT_CLEANUP ], "Limpeza pré-login")

    usuario = request.form['usuario']
    senha   = request.form['senha']

    last_salesforce_creds = {'user': usuario, 'pass': senha}
    # C4: Define expiração de 15 minutos
    last_salesforce_creds_expiry = time.time() + BW_CREDS_TTL
    is_salesforce_logged_in = True
    is_sap_logged_in = False
    is_bw_hana_logged_in = False
    last_bw_creds = {}
    last_bw_creds_expiry = 0.0

    save_connection_if_requested('salesforce', usuario, senha)

    return jsonify({"status": "sucesso", "mensagem": "Login realizado com sucesso."})

@app.route('/executar-salesforce', methods=['POST'])
def executar_salesforce():
    global is_salesforce_logged_in, last_salesforce_creds, last_salesforce_creds_expiry

    if not is_salesforce_logged_in:
        return jsonify({"status": "erro", "mensagem": "Acesso negado. Por favor, faça o login no Salesforce primeiro."}), 403

    # C4: Verifica expiração das credenciais (15 min)
    if not last_salesforce_creds or time.time() > last_salesforce_creds_expiry:
        is_salesforce_logged_in = False
        last_salesforce_creds = {}
        last_salesforce_creds_expiry = 0.0
        return jsonify({"status": "erro", "mensagem": "Sessão Salesforce expirada (15 min). Faça login novamente."}), 401

    automations = load_automations()
    nome_macro_selecionada = request.form['macro']
    config = automations.get(nome_macro_selecionada)

    if not config or config.get("type") != "salesforce":
        return jsonify({"status": "erro", "mensagem": "Automação Salesforce não encontrada ou inválida!"}), 400

    comando = [ "powershell.exe", "-ExecutionPolicy", "Bypass", "-File", SCRIPT_RUNNER_SIMPLES, "-CaminhoArquivo", config['arquivo'], "-NomeMacro", config['macro'] ]
    contexto = f"tarefa Salesforce '{nome_macro_selecionada}'"
    resultado = executar_comando_externo(comando, contexto_tarefa=contexto)

    return jsonify(resultado)

@app.route('/logout-salesforce', methods=['POST'])
def logout_salesforce():
    global is_salesforce_logged_in, last_salesforce_creds, last_salesforce_creds_expiry

    is_salesforce_logged_in = False
    last_salesforce_creds = {}
    last_salesforce_creds_expiry = 0.0

    executar_comando_externo([ "powershell.exe", "-ExecutionPolicy", "Bypass", "-File", SCRIPT_CLEANUP ], "Cleanup Pós-Salesforce")

    return jsonify({"status": "sucesso", "mensagem": "Estado de login Salesforce reiniciado."})

# --- Função de Execução de Comando ---
def executar_comando_externo(comando, contexto_tarefa="Tarefa genérica", timeout_seconds=600, env=None):
    try:
        resultado = subprocess.run(
            comando, capture_output=True, check=True, text=False,
            timeout=timeout_seconds, env=env
        )
        output = resultado.stdout.decode('cp1252', errors='ignore').strip()
        
        if "ERRO:" in output.upper():
             return {"status": "erro", "mensagem": output.replace("ERRO:", "").strip()}
             
        return {"status": "sucesso", "mensagem": output}
        
    except subprocess.TimeoutExpired:
        cleanup_command = ["powershell.exe", "-ExecutionPolicy", "Bypass", "-File", SCRIPT_CLEANUP]
        subprocess.run(cleanup_command)
        return {"status": "erro", "mensagem": f"A {contexto_tarefa} excedeu o tempo limite e foi finalizada."}
    except subprocess.CalledProcessError as e:
        erro_msg = e.stdout.decode('cp1252', errors='ignore').strip() + "\n" + e.stderr.decode('cp1252', errors='ignore').strip()
        return {"status": "erro", "mensagem": f"Erro crítico durante a {contexto_tarefa}: {erro_msg.strip()}"}
    except Exception as e:
        return {"status": "erro", "mensagem": f"Erro inesperado no Python: {str(e)}"}

# ---------------------------------------------------------------------------
# Helpers para injeção dinâmica de contexto do chatbot
# ---------------------------------------------------------------------------

def _build_automation_index():
    """Retorna lista de strings com nome, tipo e caminho explícito de cada automação."""
    try:
        data = load_automations()
        lines = []
        for name, info in data.items():
            text = (info.get('text') or '').strip()
            tipo = (info.get('type') or '').upper()
            area = info.get('hub_area', '')
            # Monta caminho de navegação explícito
            if tipo == 'SAP':
                path = f"Automações > SAP > {name}"
            elif tipo == 'BW':
                path = f"Automações > BW > {name}"
            elif tipo == 'SALESFORCE':
                path = f"Automações > Salesforce > {name}"
            else:
                path = f"Automações > {name}"
            entry = f"- **{name}**"
            if area:
                entry += f" ({area})"
            if text:
                entry += f": {text}"
            entry += f" | Caminho: {path}"
            lines.append(entry)
        return lines
    except Exception as e:
        print(f"AVISO automations_db: {e}")
        return []

def _build_dashboard_index():
    """Retorna lista com nome, descrição e caminho explícito de cada dashboard."""
    try:
        data = load_dashboards()
        lines = []
        for platform, pdata in data.items():
            # Usa system_name se disponível (ex: 'Looker Studio', 'Tableau', 'Biblioteca')
            plabel = pdata.get('system_name', platform.capitalize())
            if 'areas' in pdata:
                for area_obj in pdata['areas'].values():
                    alabel = area_obj.get('name', '')
                    for item in area_obj.get('items', []):
                        iname = item.get('name', '')
                        itext = (item.get('text') or '').strip()
                        if not iname:
                            continue
                        path = f"Dashboards > {plabel} > {alabel} > {iname}"
                        entry = f"- **{iname}** [{plabel} / {alabel}]"
                        if itext:
                            entry += f": {itext}"
                        entry += f" | Caminho: {path}"
                        lines.append(entry)
            elif 'items' in pdata:
                for item in pdata['items']:
                    iname = item.get('name', '')
                    itext = (item.get('text') or '').strip()
                    if not iname:
                        continue
                    path = f"Dashboards > {plabel} > {iname}"
                    entry = f"- **{iname}** [{plabel}]"
                    if itext:
                        entry += f": {itext}"
                    entry += f" | Caminho: {path}"
                    lines.append(entry)
        return lines
    except Exception as e:
        print(f"AVISO dashboards_db: {e}")
        return []

# Pré-carrega índices na inicialização para não ler arquivo a cada request
_AUTO_INDEX  = _build_automation_index()
_DASH_INDEX  = _build_dashboard_index()

# Palavras-chave para decidir qual contexto injetar
_KW_AUTOMATION = {
    'automação', 'automacao', 'robô', 'robo', 'macro', 'sap', 'bw', 'base mãe',
    'base mae', 'outlook', 'aging', 'relatório peças', 'relatorio pecas',
    'cancelamento', 'salesforce', 'monitor de pedidos', 'extração', 'extracao'
}
_KW_DASHBOARD = {
    'dashboard', 'looker', 'tableau', 'biblioteca', 'library', 'daily block',
    'receipt', 'cdp', 'embalagem', 'packing', 'stock', 'linhas', 'returns',
    'faturamento', 'transporte', 'calendar', 'gerencial', 'farol', 'sla',
    'projects', 'heinrich', 'nave', 'on time', 'ontime', 'sgi', 'glossário',
    'sheets', 'jira', 'bigquery', 'supply', 'acompanhamento'
}
_KW_AODOCS = {
    'processo', 'logístic', 'logistic', 'armazenagem', 'armazém', 'armazem',
    'recebimento', 'expedição', 'expedicao', 'estoque', 'inventário', 'inventario',
    'custo', 'ehs', 'reversa', 'devolução', 'devolucao', 'wms', 'gardem',
    'erp', 'picking', 'packing list', 'romaneio', 'raci', 'pivo', 'usp',
    'joinville', 'rio claro', 'nota fiscal', 'nf', 'cte', 'transportador',
    'm&a', 'sg&a', 'acuracidade', 'saldo contábil', 'recontagem', 'inspeção',
    'inspecao', 'sucata', 'ordem inversa', 'whirlpool', 'sap ecc'
}
_KW_HUB = {
    'hub', 'ferramenta', 'acesso rápido', 'drive', 'perfil', 'login', 'senha',
    'agendador', 'conexão', 'conexao', 'analista', 'executor', 'registrar',
    'token', 'cargo', 'função', 'funcao', 'bug', 'erro', 'feedback', 'sugestão'
}

def _detect_topics(text: str):
    """Retorna set de tópicos relevantes para a mensagem do usuário."""
    low = text.lower()
    topics = set()
    if any(kw in low for kw in _KW_AUTOMATION):
        topics.add('auto')
    if any(kw in low for kw in _KW_DASHBOARD):
        topics.add('dash')
    if any(kw in low for kw in _KW_AODOCS):
        topics.add('aodocs')
    if any(kw in low for kw in _KW_HUB):
        topics.add('hub')
    # fallback: sem match → inclui tudo (pergunta genérica)
    if not topics:
        topics = {'hub', 'auto', 'dash'}
    return topics

# Palavras irrelevantes para filtragem do índice (stopwords PT-BR + EN comuns)
_STOPWORDS = {
    'de', 'do', 'da', 'dos', 'das', 'o', 'a', 'os', 'as', 'um', 'uma',
    'em', 'no', 'na', 'nos', 'nas', 'por', 'para', 'com', 'e', 'ou',
    'que', 'se', 'me', 'te', 'ele', 'ela', 'nós', 'eles', 'eu', 'tu',
    'como', 'onde', 'qual', 'quando', 'quem', 'quais', 'esta', 'esse',
    'isto', 'isso', 'aquele', 'aquilo', 'meu', 'fica', 'tem', 'ser',
    'the', 'is', 'of', 'in', 'on', 'at', 'to', 'for', 'and', 'or',
    'quero', 'saber', 'preciso', 'pode', 'favor', 'obrigado', 'ajuda',
    'quer', 'dizer', 'faz', 'vai', 'vou', 'nao', 'sim',
    # Termos genéricos de query que batem em tudo (não são nomes de dashboards/automações)
    'dashboard', 'dashboards', 'automacao', 'automacao', 'automações', 'automacoes',
    'ferramenta', 'ferramentas', 'acesso', 'encontrar', 'encontro', 'encontre',
    'quer', 'temos', 'tem', 'tenho', 'achar', 'acho', 'ver',
}

def _filter_index(index: list, query: str, fallback_limit: int = 15) -> list:
    """Filtra o índice retornando apenas entradas relevantes para a query.

    Busca SOMENTE no nome+descrição (antes de '| Caminho:') para evitar
    falsos positivos com a palavra 'Dashboards' presente em todos os caminhos.
    Se nenhuma entrada casar, retorna até `fallback_limit` entradas.
    """
    if not index:
        return []
    low = query.lower()
    tokens = [t.strip(string.punctuation) for t in low.split()]
    keywords = [t for t in tokens if len(t) >= 3 and t not in _STOPWORDS]

    if not keywords:
        return index[:fallback_limit]

    matched = []
    for entry in index:
        # Filtra apenas contra nome+descrição — ignora o campo "| Caminho: ..."
        searchable = entry.split('| Caminho:')[0].lower()
        if any(kw in searchable for kw in keywords):
            matched.append(entry)

    return matched if matched else index[:fallback_limit]


# Todos os domínios conhecidos do Hub para pré-filtro de off-topic
_ALL_HUB_KW = _KW_HUB | _KW_AUTOMATION | _KW_DASHBOARD | _KW_AODOCS

def _is_offtopic(text: str, history: list) -> bool:
    """Retorna True APENAS se a mensagem for claramente não relacionada ao Hub.

    Estratégia conservadora: só rejeita se NÃO houver NENHUMA evidência de
    contexto Hub — nem na mensagem atual, nem nas últimas trocas do histórico.
    Nunca bloqueia perguntas curtas (possíveis follow-ups como 'onde fica?').
    """
    low = text.lower().strip()
    # Pergunta curta = provavelmente follow-up de conversa em andamento
    if len(low.split()) <= 4:
        return False
    # Contém keyword do Hub → relevante
    if any(kw in low for kw in _ALL_HUB_KW):
        return False
    # Histórico recente tem conteúdo Hub → provavelmente follow-up
    for msg in history[-4:]:
        if any(kw in msg.get('text', '').lower() for kw in _ALL_HUB_KW):
            return False
    # Nenhuma evidência de contexto Hub → off-topic
    return True

# --- ROTA DE API: CHATBOT (llama.cpp) ---
@app.route('/api/chatbot/query', methods=['POST'])
def chatbot_query():
    if not llama_model:
        return jsonify({"status": "erro", "text": "Nenhum modelo .gguf encontrado na pasta /model. Adicione um modelo e reinicie o servidor."}), 500

    history = request.json.get('history', [])
    if not history:
        return jsonify({"status": "erro", "text": "Mensagem vazia."}), 400

    # Limita o histórico — menos tokens = prefill mais rápido
    MAX_HISTORY_MESSAGES = 4  # 2 trocas (user + assistant)
    if len(history) > MAX_HISTORY_MESSAGES:
        history = history[-MAX_HISTORY_MESSAGES:]

    # Detecta tópico pela última mensagem do usuário
    last_user_text = ""
    for msg in reversed(history):
        if msg.get("role") == "user":
            last_user_text = msg.get("text", "")
            break

    # Pré-filtro: rejeita off-topic sem chamar o modelo (resposta instantânea)
    if _is_offtopic(last_user_text, history):
        return jsonify({
            "status": "sucesso",
            "text": "Só posso ajudar com dúvidas relacionadas ao **Hub Spare Parts**.",
            "form_trigger": None
        })

    topics = _detect_topics(last_user_text)

    # Monta o system prompt — COMPACTO para manter prefill < 400 tokens no CPU
    try:
        context_data = _get_context_data()
        if not context_data:
            raise ValueError('prompt.json vazio ou não carregado')

        # Cabeçalho fixo e curto
        system_prompt = (
            "Você é o Hub Assistant do Hub Spare Parts (Whirlpool). "
            "Responda SOMENTE sobre o Hub. Seja breve e direto. "
            "Use **negrito** para nomes. Não invente informações. "
            "Se perguntarem nova automação/bug: termine com [FORM:DEMANDA]. "
            "Se pedirem feedback/sugestão: termine com [FORM:SUGESTAO].\n"
        )

        # Ferramentas gerais — apenas se pergunta sobre Hub/ferramentas/login
        if 'hub' in topics:
            tools = context_data.get("tools", [])
            if tools:
                # Trunca cada item para economizar tokens: máx 120 chars por item
                short_tools = [t[:120] for t in tools]
                system_prompt += "\nFerramentas: " + " | ".join(short_tools) + "\n"

        # Automações — apenas se tópico específico detectado (NÃO no fallback genérico)
        if 'auto' in topics and topics != {'hub', 'auto', 'dash'}:
            filtered_auto = _filter_index(_AUTO_INDEX, last_user_text, fallback_limit=5)
            if filtered_auto:
                system_prompt += "\nAutomações:\n" + "\n".join(filtered_auto[:5]) + "\n"

        # Dashboards — apenas se tópico específico detectado
        if 'dash' in topics and topics != {'hub', 'auto', 'dash'}:
            filtered_dash = _filter_index(_DASH_INDEX, last_user_text, fallback_limit=5)
            if filtered_dash:
                system_prompt += "\nDashboards:\n" + "\n".join(filtered_dash[:5]) + "\n"

        # AODOCS — apenas se detectado explicitamente
        if 'aodocs' in topics:
            aodocs = context_data.get("aodocs_knowledge", "").strip()
            if aodocs:
                system_prompt += "\nProcessos: " + aodocs[:500] + "\n"

        # Login/registro — regra curta embutida (não mais do prompt.json longo)
        system_prompt += (
            "\nLogin: clique no ícone de perfil > Logar. "
            "Sem acesso: Solicitar Acesso > preencher dados > guardar token. "
            "Dashboard bloqueado: solicitar ao gestor da área. "
            "Criadores: Mateus Lopes (github.com/Etamus) e Jonathan Paixão.\n"
        )

        tok_est = len(system_prompt) // 4
        print(f"[LLM] system_prompt: {len(system_prompt)} chars / ~{tok_est} tokens | tópicos: {topics}")

    except Exception as e:
        print(f"ERRO ao ler prompt.json: {e}")
        return jsonify({"status": "erro", "text": "Desculpe, estou com problemas para acessar meu contexto interno."}), 500

    try:
        # Constrói prompt no formato ChatML (compatível com a maioria dos modelos GGUF)
        messages = [{"role": "system", "content": system_prompt}]
        for msg in history:
            role = msg.get("role", "user")
            text = msg.get("text", "").replace("<strong>", "").replace("</strong>", "")
            # Gemini usava 'model', llama.cpp usa 'assistant'
            if role == "model":
                role = "assistant"
            messages.append({"role": role, "content": text})

        # Executa inferência com timeout de 180s para evitar hang infinito no Flask
        _INFERENCE_TIMEOUT = 180
        _inference_kwargs = dict(
            messages=messages,
            max_tokens=200,      # respostas curtas = mais rápido na CPU
            temperature=0.25,    # baixo = determinístico/factual (ideal para FAQ)
            top_k=40,
            top_p=0.90,
            repeat_penalty=1.15,
            stop=["\nUsuário:", "\nUser:", "\nAssistant:", "<|im_end|>"]
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _pool:
            _future = _pool.submit(llama_model.create_chat_completion, **_inference_kwargs)
            try:
                response = _future.result(timeout=_INFERENCE_TIMEOUT)
            except concurrent.futures.TimeoutError:
                print(f'ERRO: inferência excedeu {_INFERENCE_TIMEOUT}s. Possível travamento.')
                return jsonify({"status": "erro", "text": "A resposta demorou muito. Tente uma pergunta mais curta."}), 504

        bot_response_text = response["choices"][0]["message"]["content"].strip()

        form_trigger = None
        if "[FORM:DEMANDA]" in bot_response_text:
            bot_response_text = bot_response_text.replace("[FORM:DEMANDA]", "").strip()
            form_trigger = "demanda"
        elif "[FORM:SUGESTAO]" in bot_response_text:
            bot_response_text = bot_response_text.replace("[FORM:SUGESTAO]", "").strip()
            form_trigger = "sugestao"

        return jsonify({"status": "sucesso", "text": bot_response_text, "form_trigger": form_trigger})

    except Exception as e:
        print(f"Erro no llama.cpp: {e}")
        return jsonify({"status": "erro", "text": "Desculpe, não consegui processar sua solicitação no momento."}), 500
    
 # --- NOVAS ROTAS DE API (CMS ADMIN) ---

@app.route('/api/hub/get-cms-data')
def hub_get_cms_data():
    """Retorna dados de CMS filtrados pela área do usuário logado (para a busca)."""
    automations = load_automations()
    dashboards_data = load_dashboards()

    username = session.get('username')
    users = load_users()
    user_area = session.get('active_area') or (users.get(username, {}).get('area', 'Spare Parts') if username else 'Spare Parts')
    if username and username.lower() == 'admin' and not session.get('active_area'):
        user_area = 'Spare Parts'
    automations = filter_automations_by_area(automations, user_area)
    if username:
        dashboards_data = filter_dashboards_by_area(dashboards_data, user_area)
    else:
        dashboards_data = filter_dashboards_general_only(dashboards_data)

    return jsonify({
        "status": "sucesso",
        "automations": automations,
        "dashboards": dashboards_data
    })

@app.route('/api/admin/get-cms-data')
def admin_get_cms_data():
    """Retorna os dados completos dos JSONs de automação e dashboards."""
    if not is_admin():
        return jsonify({"status": "erro", "mensagem": "Acesso negado."}), 403
    
    automations = load_automations()
    dashboards = load_dashboards()
    
    return jsonify({
        "status": "sucesso", 
        "automations": automations, 
        "dashboards": dashboards
    })

@app.route('/api/admin/save-automations', methods=['POST'])
def admin_save_automations():
    """Salva automações no banco de dados."""
    if not is_admin():
        return jsonify({"status": "erro", "mensagem": "Acesso negado."}), 403

    try:
        data = request.json
        save_automations(data)
        # M3: Audit de alteração de automações
        audit(session.get('username'), 'save_automations', f'Itens: {len(data)}')
        return jsonify({"status": "sucesso", "mensagem": "Automações atualizadas."})
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": f"Falha ao salvar automações: {str(e)}"}), 500

@app.route('/api/admin/save-dashboards', methods=['POST'])
def admin_save_dashboards():
    """Salva dashboards no banco de dados."""
    if not is_admin():
        return jsonify({"status": "erro", "mensagem": "Acesso negado."}), 403

    try:
        data = request.json
        save_dashboards(data)
        # M3: Audit de alteração de dashboards
        audit(session.get('username'), 'save_dashboards')
        return jsonify({"status": "sucesso", "mensagem": "Dashboards atualizados."})
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": f"Falha ao salvar dashboards: {str(e)}"}), 500   

@app.route('/api/admin/add-user', methods=['POST'])
def admin_add_user():
    """Cria um novo usuário (via Admin)."""
    if not is_admin():
        return jsonify({"status": "erro", "mensagem": "Acesso negado."}), 403
        
    data = request.json
    username = data.get('username')
    password = data.get('password')
    area = data.get('area')
    role = data.get('role')
    allowed_areas = data.get('allowed_areas', [area])

    if not username or not password or not area or not role:
        return jsonify({"status": "erro", "mensagem": "Todos os campos são obrigatórios (usuário, senha, área, função)."}), 400

    users = load_users()
    if username in users:
        return jsonify({"status": "erro", "mensagem": "Este usuário já existe."}), 400
    
    # C1: Hash bcrypt na criação de usuário pelo admin
    users[username] = {
        "password": bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode(),
        "role": role,
        "area": area,
        "allowed_areas": [a for a in allowed_areas if a in HUB_AREAS] or [area],
        "display_name": None,
        "profile_image": None,
        "connections": {
            "sap": None,
            "bw": None,
            "tableau": None
        },
        "login_attempts": 0,
        "lockout_until": None
    }

    save_users(users)
    audit(session.get('username'), 'admin_add_user', f'Created: {username}')
    return jsonify({"status": "sucesso", "mensagem": f"Usuário {username} criado."})

# --- Início do Servidor ---

if __name__ == '__main__':
    import flask.cli as _flask_cli
    _flask_cli.show_server_banner = lambda *a, **kw: None

    # Inicializa banco de dados (cria tabelas e migra JSONs legados se necessário)
    init_db()

    port = int(os.environ.get('PORT', 5000))

    # A1: Suporte a SSL/HTTPS via certificado interno da empresa
    ssl_cert = os.environ.get('SSL_CERT', '').strip()
    ssl_key  = os.environ.get('SSL_KEY', '').strip()
    ssl_ctx  = (ssl_cert, ssl_key) if ssl_cert and ssl_key else None

    if ssl_ctx:
        print(f"Servidor Iniciado com HTTPS na porta {port}")
        print(f"Acesse o Hub em https://[SEU_IP]:{port}")
    else:
        print(f"Servidor Iniciado com HTTP na porta {port} (rede interna)")
        print(f"Acesse o Hub em http://[SEU_IP]:{port}")

    app.run(
        host='0.0.0.0',
        port=port,
        use_reloader=False,
        debug=False,
        threaded=True,
        ssl_context=ssl_ctx
    )