from flask import Flask, jsonify, request, session, redirect
import threading
import traceback

import requests
import pandas as pd
import time
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import urllib3

# =============================
# CONFIG API
# =============================
app = Flask(__name__)

API_TOKEN = "123456"

# 🔐 NOVO - SECRET KEY (necessário para login)
app.secret_key = "super_secret_key_123"

# 🔐 NOVO - CREDENCIAIS DE LOGIN
USUARIO = "admin"
SENHA = "1234"

urllib3.disable_warnings()

# =============================
# CONFIG ORIGINAL (MANTIDO)
# =============================
BASE_URL = "https://b1.ativy.com:51032/b1s/v1"
LOGIN_URL = f"{BASE_URL}/Login"
QUERY_URL = f"{BASE_URL}/SQLQueries('AReceberGreg')/List"

COMPANY_DB = "SBO_PRD_INOVASUPRI"
USERNAME = "3i_0016"
PASSWORD = "Ib@admin016"

SPREADSHEET_ID = "1mX5Igw-_PMyWXmTTLfAthU5w6cJpiUvnVY76ECe1fPk"
WORKSHEET_NAME = "AReceber"

DATA_INICIAL = "22/04/2026"
data_filtro = datetime.strptime(DATA_INICIAL, "%d/%m/%Y")

CREDENTIALS_FILE = "credentials.json"

# =============================
# 🔎 ROTA BASE
# =============================
@app.route("/")
def home():
    return redirect ("/login")

# =============================
# 🔐 LOGIN (NOVO)
# =============================
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = request.form.get("usuario")
        senha = request.form.get("senha")

        if usuario == USUARIO and senha == SENHA:
            session["logado"] = True
            return redirect("/painel")
        else:
            return "❌ Login inválido"

    return """
    <h2>Login</h2>
    <form method="POST">
        Usuário: <input type="text" name="usuario"><br><br>
        Senha: <input type="password" name="senha"><br><br>
        <button type="submit">Entrar</button>
    </form>
    """
@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")

# =============================
# AUTENTICAÇÃO GOOGLE (MANTIDO)
# =============================
def autenticar_google():
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"\n❌ Arquivo não encontrado: {CREDENTIALS_FILE}")
        return None
    
    try:
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        
        with open(CREDENTIALS_FILE, 'r') as f:
            data = json.load(f)
            print(f"   📧 Autenticado como: {data['client_email']}")
        
        return client
        
    except Exception as e:
        print(f"   ❌ Erro na autenticação: {e}")
        return None

# =============================
# BUSCAR DADOS DO SAP (MANTIDO)
# =============================
def buscar_dados_sap():
    print("\n1️⃣ Conectando ao SAP...")
    session_req = requests.Session()
    
    login_payload = {
        "CompanyDB": COMPANY_DB,
        "UserName": USERNAME,
        "Password": PASSWORD
    }
    
    try:
        response = session_req.post(LOGIN_URL, json=login_payload, verify=False)
        if response.status_code != 200:
            raise Exception(f"Erro HTTP: {response.status_code}")
        print("   ✅ Login SAP OK")
    except Exception as e:
        print(f"   ❌ Erro: {e}")
        return None
    
    print("\n2️⃣ Buscando dados do SAP...")
    all_data = []
    skip = 0
    
    while True:
        url = f"{QUERY_URL}?$skip={skip}"
        
        try:
            response = session_req.get(url, verify=False)
            
            if response.status_code != 200:
                break
            
            data = response.json().get("value", [])
            
            if not data:
                break
            
            all_data.extend(data)
            print(f"   📦 Coletados: {len(all_data)} registros", end="\r")
            
            if len(data) < 20:
                break
            
            skip += 20
            time.sleep(0.3)
            
        except Exception as e:
            print(f"\n   ❌ Erro: {e}")
            break
    
    print(f"\n   ✅ Total coletado: {len(all_data)} registros")
    return all_data

# =============================
# FUNÇÃO PRINCIPAL (MANTIDO)
# =============================
def buscar_e_salvar():
    print("="*60)
    print("🚀 SAP → GOOGLE SHEETS")
    print("="*60)
    
    dados = buscar_dados_sap()
    
    if not dados:
        return "Nenhum dado encontrado"
    
    print(f"\n3️⃣ Filtrando a partir de {DATA_INICIAL}...")
    filtrados = []
    
    for item in dados:
        data_venc = item.get("DataVencimento")
        if data_venc:
            try:
                dt = datetime.strptime(str(data_venc), "%Y%m%d")
                if dt >= data_filtro:
                    filtrados.append(item)
            except:
                continue
    
    if not filtrados:
        return "Sem dados após filtro"
    
    df = pd.DataFrame(filtrados)
    
    if 'DataVencimento' in df.columns:
        df['DataVencimento'] = pd.to_datetime(df['DataVencimento'], format='%Y%m%d')
    
    client = autenticar_google()
    if not client:
        return "Erro autenticação Google"
    
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    
    try:
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        worksheet.clear()
    except:
        worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=100000, cols=50)
    
    df_convertido = df.copy()
    
    for col in df_convertido.columns:
        if pd.api.types.is_datetime64_any_dtype(df_convertido[col]):
            df_convertido[col] = df_convertido[col].dt.strftime('%d/%m/%Y')
    
    dados_sheet = [df_convertido.columns.tolist()] + df_convertido.fillna('').values.tolist()
    
    for i in range(0, len(dados_sheet), 500):
        worksheet.update(range_name=f'A{i+1}', values=dados_sheet[i:i+500])
        time.sleep(0.3)
    
    return f"{len(df)} registros salvos"

# =============================
# THREAD (MANTIDO)
# =============================
def executar_background():
    try:
        print("🚀 Iniciando processo...")
        resultado = buscar_e_salvar()
        print(f"✅ Finalizado: {resultado}")
    except Exception as e:
        print("❌ ERRO NA EXECUÇÃO:")
        traceback.print_exc()

# =============================
# ENDPOINT (MANTIDO)
# =============================
@app.route("/executar", methods=["GET"])
def executar():
    print("📡 Endpoint /executar chamado")

    token = request.headers.get("Authorization")

    if token != API_TOKEN:
        print("❌ Token inválido")
        return jsonify({"status": "erro", "mensagem": "Não autorizado"}), 401

    threading.Thread(target=executar_background).start()

    return jsonify({
        "status": "ok",
        "mensagem": "Execução iniciada"
    })

# =============================
# 🔐 PAINEL PROTEGIDO
# =============================
@app.route("/painel")
def painel():
    if not session.get("logado"):
        return redirect("/login")

    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Painel SAP</title>
    </head>
    <body>

        <h1>🚀 Integração SAP → Google Sheets</h1>

        <button onclick="executar()">Iniciar Processo</button>
        <br><br>
        <a href="/logout">Sair</a>

        <div id="status"></div>

        <script>
            async function executar() {
                const status = document.getElementById("status");
                status.innerHTML = "⏳ Executando...";

                try {
                    const response = await fetch('/executar', {
                        method: 'GET',
                        headers: {
                            'Authorization': '123456'
                        }
                    });

                    const data = await response.json();

                    if (response.status === 200) {
                        status.innerHTML = "✅ " + data.mensagem;
                    } else {
                        status.innerHTML = "❌ " + data.mensagem;
                    }

                } catch (error) {
                    status.innerHTML = "❌ Erro na requisição";
                }
            }
        </script>

    </body>
    </html>
    """

# =============================
# START
# =============================
if __name__ == "__main__":
    print("🔥 Servidor iniciando...")
    port = int(os.environ.get("PORT", 8080))

    # 🔥 NOVO - MOSTRAR LINK
    print(f"🌐 Acesse: http://localhost:{port}/login")

    app.run(host="0.0.0.0", port=port)
