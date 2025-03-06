import streamlit as st
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
import time
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
import sqlite3
from sqlite3 import Error

# Configuração da página
st.set_page_config(
    page_title="Gerenciador de Anúncios Facebook",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Função para criar banco de dados SQLite
def create_connection():
    conn = None
    try:
        if not os.path.exists('data'):
            os.makedirs('data')
        conn = sqlite3.connect('data/facebook_ads_manager.db')
        return conn
    except Error as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
    return conn

# Inicializar banco de dados e tabelas
def init_db():
    conn = create_connection()
    if conn is not None:
        try:
            c = conn.cursor()
            
            # Tabela de configurações da API (múltiplas contas)
            c.execute('''
                CREATE TABLE IF NOT EXISTS api_config (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    app_id TEXT NOT NULL,
                    app_secret TEXT NOT NULL,
                    access_token TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    business_id TEXT,
                    page_id TEXT,
                    is_active INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Verificar se a tabela rules existe
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='rules'")
            table_exists = c.fetchone()
            
            if table_exists:
                # Verificar se a coluna is_composite existe
                try:
                    c.execute("SELECT is_composite FROM rules LIMIT 1")
                except Error:
                    # Coluna não existe, vamos criar uma tabela temporária e migrar os dados
                    st.info("Migrando banco de dados para suportar regras compostas...")
                    
                    # Criar nova tabela com estrutura atualizada
                    c.execute('''
                        CREATE TABLE rules_new (
                            id INTEGER PRIMARY KEY,
                            name TEXT NOT NULL,
                            description TEXT,
                            condition_type TEXT NOT NULL,
                            is_composite INTEGER DEFAULT 0,
                            primary_metric TEXT NOT NULL,
                            primary_operator TEXT NOT NULL,
                            primary_value REAL NOT NULL,
                            secondary_metric TEXT,
                            secondary_operator TEXT,
                            secondary_value REAL,
                            join_operator TEXT DEFAULT 'AND',
                            action_type TEXT NOT NULL,
                            action_value REAL,
                            is_active INTEGER DEFAULT 1,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                    
                    # Migrar dados da tabela antiga para a nova
                    c.execute('''
                        INSERT INTO rules_new (
                            id, name, description, condition_type, is_composite,
                            primary_metric, primary_operator, primary_value, 
                            action_type, action_value, is_active, created_at, updated_at
                        )
                        SELECT 
                            id, name, description, condition_type, 0,
                            condition_metric, condition_operator, condition_value, 
                            action_type, action_value, is_active, created_at, updated_at
                        FROM rules
                    ''')
                    
                    # Renomear tabelas
                    c.execute("DROP TABLE rules")
                    c.execute("ALTER TABLE rules_new RENAME TO rules")
                    
                    conn.commit()
                    st.success("Migração concluída com sucesso!")
            else:
                # Tabela não existe, criar com a nova estrutura
                c.execute('''
                    CREATE TABLE IF NOT EXISTS rules (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        description TEXT,
                        condition_type TEXT NOT NULL,
                        is_composite INTEGER DEFAULT 0,
                        primary_metric TEXT NOT NULL,
                        primary_operator TEXT NOT NULL,
                        primary_value REAL NOT NULL,
                        secondary_metric TEXT,
                        secondary_operator TEXT,
                        secondary_value REAL,
                        join_operator TEXT DEFAULT 'AND',
                        action_type TEXT NOT NULL,
                        action_value REAL,
                        is_active INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
            
            # Tabela de execuções de regras
            c.execute('''
                CREATE TABLE IF NOT EXISTS rule_executions (
                    id INTEGER PRIMARY KEY,
                    rule_id INTEGER NOT NULL,
                    ad_object_id TEXT NOT NULL,
                    ad_object_type TEXT NOT NULL,
                    ad_object_name TEXT NOT NULL,
                    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    was_successful INTEGER DEFAULT 0,
                    message TEXT,
                    FOREIGN KEY (rule_id) REFERENCES rules (id)
                )
            ''')
            
            conn.commit()
        except Error as e:
            st.error(f"Erro ao criar tabelas: {e}")
        finally:
            conn.close()

# Inicializar o banco de dados
init_db()

# Função para salvar configurações da API
def save_api_config(name, app_id, app_secret, access_token, account_id, business_id="", page_id=""):
    conn = create_connection()
    if conn is not None:
        try:
            c = conn.cursor()
            # Verifica se é a primeira conexão
            c.execute("SELECT COUNT(*) FROM api_config")
            count = c.fetchone()[0]
            is_active = 1 if count == 0 else 0
            
            c.execute(
                """INSERT INTO api_config 
                   (name, app_id, app_secret, access_token, account_id, business_id, page_id, is_active) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (name, app_id, app_secret, access_token, account_id, business_id, page_id, is_active)
            )
            conn.commit()
            return True
        except Error as e:
            st.error(f"Erro ao salvar configurações: {e}")
            return False
        finally:
            conn.close()
    return False

# Função para obter configurações ativas da API
def get_active_api_config():
    conn = create_connection()
    if conn is not None:
        try:
            c = conn.cursor()
            c.execute("""SELECT id, name, app_id, app_secret, access_token, account_id, 
                         business_id, page_id FROM api_config WHERE is_active = 1 LIMIT 1""")
            row = c.fetchone()
            if row:
                return {
                    "id": row[0],
                    "name": row[1],
                    "app_id": row[2],
                    "app_secret": row[3],
                    "access_token": row[4],
                    "account_id": row[5],
                    "business_id": row[6],
                    "page_id": row[7]
                }
        except Error as e:
            st.error(f"Erro ao obter configurações ativas: {e}")
        finally:
            conn.close()
    return None

# Função para obter todas as configurações da API
def get_all_api_configs():
    conn = create_connection()
    if conn is not None:
        try:
            c = conn.cursor()
            c.execute("""SELECT id, name, app_id, app_secret, access_token, account_id, 
                         business_id, page_id, is_active FROM api_config ORDER BY name""")
            rows = c.fetchall()
            configs = []
            for row in rows:
                configs.append({
                    "id": row[0],
                    "name": row[1],
                    "app_id": row[2],
                    "app_secret": row[3],
                    "access_token": row[4],
                    "account_id": row[5],
                    "business_id": row[6],
                    "page_id": row[7],
                    "is_active": row[8]
                })
            return configs
        except Error as e:
            st.error(f"Erro ao obter todas as configurações: {e}")
        finally:
            conn.close()
    return []

# Função para definir configuração ativa
def set_active_api_config(config_id):
    conn = create_connection()
    if conn is not None:
        try:
            c = conn.cursor()
            # Desativa todas as configurações
            c.execute("UPDATE api_config SET is_active = 0")
            # Ativa a configuração selecionada
            c.execute("UPDATE api_config SET is_active = 1 WHERE id = ?", (config_id,))
            conn.commit()
            return True
        except Error as e:
            st.error(f"Erro ao definir configuração ativa: {e}")
            return False
        finally:
            conn.close()
    return False

# Função para excluir uma configuração
def delete_api_config(config_id):
    conn = create_connection()
    if conn is not None:
        try:
            c = conn.cursor()
            # Verifica se é a configuração ativa
            c.execute("SELECT is_active FROM api_config WHERE id = ?", (config_id,))
            row = c.fetchone()
            if row and row[0] == 1:
                # Se for a configuração ativa, ativa outra antes de excluir
                c.execute("SELECT id FROM api_config WHERE id != ? LIMIT 1", (config_id,))
                other_config = c.fetchone()
                if other_config:
                    c.execute("UPDATE api_config SET is_active = 1 WHERE id = ?", (other_config[0],))
            
            # Exclui a configuração
            c.execute("DELETE FROM api_config WHERE id = ?", (config_id,))
            conn.commit()
            return True
        except Error as e:
            st.error(f"Erro ao excluir configuração: {e}")
            return False
        finally:
            conn.close()
    return False

# Função para inicializar a API do Facebook
def init_facebook_api():
    config = get_active_api_config()
    if config:
        try:
            FacebookAdsApi.init(
                app_id=config["app_id"],
                app_secret=config["app_secret"],
                access_token=config["access_token"]
            )
            return config["account_id"]
        except Exception as e:
            st.error(f"Erro ao inicializar API do Facebook: {e}")
    return None

# Função para adicionar regra (com suporte a regras compostas)
def add_rule(name, description, condition_type, primary_metric, primary_operator, 
             primary_value, action_type, action_value, is_composite=0, secondary_metric=None, 
             secondary_operator=None, secondary_value=None, join_operator="AND"):
    conn = create_connection()
    if conn is not None:
        try:
            c = conn.cursor()
            c.execute(
                '''INSERT INTO rules 
                   (name, description, condition_type, is_composite, primary_metric, 
                    primary_operator, primary_value, secondary_metric, secondary_operator, 
                    secondary_value, join_operator, action_type, action_value) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (name, description, condition_type, is_composite, primary_metric, 
                 primary_operator, primary_value, secondary_metric, secondary_operator, 
                 secondary_value, join_operator, action_type, action_value)
            )
            conn.commit()
            return True
        except Error as e:
            st.error(f"Erro ao adicionar regra: {e}")
            return False
        finally:
            conn.close()
    return False

# Função para obter todas as regras
def get_all_rules():
    conn = create_connection()
    if conn is not None:
        try:
            c = conn.cursor()
            # Verificar quais colunas existem na tabela rules
            c.execute("PRAGMA table_info(rules)")
            columns_info = c.fetchall()
            column_names = [column[1] for column in columns_info]
            
            # Construir consulta SQL com base nas colunas existentes
            if 'is_composite' in column_names:
                # Novo formato (após migração)
                c.execute("""
                    SELECT id, name, description, condition_type, is_composite,
                           primary_metric, primary_operator, primary_value,
                           secondary_metric, secondary_operator, secondary_value,
                           join_operator, action_type, action_value, is_active, 
                           created_at, updated_at
                    FROM rules 
                    ORDER BY created_at DESC
                """)
            else:
                # Formato antigo (antes da migração)
                c.execute("""
                    SELECT id, name, description, condition_type,
                           condition_metric, condition_operator, condition_value,
                           action_type, action_value, is_active, 
                           created_at, updated_at
                    FROM rules 
                    ORDER BY created_at DESC
                """)
            
            rules = c.fetchall()
            columns = [description[0] for description in c.description]
            result = []
            for rule in rules:
                rule_dict = {}
                for i, column in enumerate(columns):
                    rule_dict[column] = rule[i]
                result.append(rule_dict)
            return result
        except Error as e:
            st.error(f"Erro ao obter regras: {e}")
        finally:
            conn.close()
    return []

# Função para excluir regra
def delete_rule(rule_id):
    conn = create_connection()
    if conn is not None:
        try:
            c = conn.cursor()
            c.execute("DELETE FROM rules WHERE id = ?", (rule_id,))
            conn.commit()
            return True
        except Error as e:
            st.error(f"Erro ao excluir regra: {e}")
            return False
        finally:
            conn.close()
    return False

# Função para atualizar estado da regra (ativar/desativar)
def toggle_rule_status(rule_id, is_active):
    conn = create_connection()
    if conn is not None:
        try:
            c = conn.cursor()
            c.execute("UPDATE rules SET is_active = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", 
                     (1 if is_active else 0, rule_id))
            conn.commit()
            return True
        except Error as e:
            st.error(f"Erro ao atualizar status da regra: {e}")
            return False
        finally:
            conn.close()
    return False

# Função para registrar execução de regra
def log_rule_execution(rule_id, ad_object_id, ad_object_type, ad_object_name, was_successful, message=""):
    conn = create_connection()
    if conn is not None:
        try:
            c = conn.cursor()
            c.execute(
                '''INSERT INTO rule_executions 
                   (rule_id, ad_object_id, ad_object_type, ad_object_name, was_successful, message) 
                   VALUES (?, ?, ?, ?, ?, ?)''',
                (rule_id, ad_object_id, ad_object_type, ad_object_name, 1 if was_successful else 0, message)
            )
            conn.commit()
            return True
        except Error as e:
            st.error(f"Erro ao registrar execução da regra: {e}")
            return False
        finally:
            conn.close()
    return False

# Função para obter histórico de execuções
def get_rule_executions(limit=100):
    conn = create_connection()
    if conn is not None:
        try:
            c = conn.cursor()
            c.execute('''
                SELECT re.id, r.name as rule_name, re.ad_object_id, re.ad_object_type, 
                       re.ad_object_name, re.executed_at, re.was_successful, re.message
                FROM rule_executions re
                JOIN rules r ON re.rule_id = r.id
                ORDER BY re.executed_at DESC
                LIMIT ?
            ''', (limit,))
            executions = c.fetchall()
            columns = [description[0] for description in c.description]
            result = []
            for execution in executions:
                execution_dict = {}
                for i, column in enumerate(columns):
                    execution_dict[column] = execution[i]
                result.append(execution_dict)
            return result
        except Error as e:
            st.error(f"Erro ao obter histórico de execução: {e}")
        finally:
            conn.close()
    return []

# Função para obter campanhas do Facebook
def get_facebook_campaigns(account_id):
    try:
        account = AdAccount(f'act_{account_id}')
        campaigns = account.get_campaigns(
            fields=[
                'id', 'name', 'status', 'objective', 'created_time', 
                'start_time', 'stop_time', 'daily_budget', 'lifetime_budget'
            ]
        )
        return campaigns
    except Exception as e:
        st.error(f"Erro ao obter campanhas: {e}")
        return []

# Função para obter conjuntos de anúncios
def get_facebook_adsets(account_id, campaign_id=None):
    try:
        account = AdAccount(f'act_{account_id}')
        params = {}
        if campaign_id:
            params['campaign_id'] = campaign_id
            
        adsets = account.get_ad_sets(
            params=params,
            fields=[
                'id', 'name', 'status', 'campaign_id', 'daily_budget', 
                'lifetime_budget', 'targeting', 'bid_amount'
            ]
        )
        return adsets
    except Exception as e:
        st.error(f"Erro ao obter conjuntos de anúncios: {e}")
        return []

# Função para obter anúncios
def get_facebook_ads(account_id, adset_id=None):
    try:
        account = AdAccount(f'act_{account_id}')
        params = {}
        if adset_id:
            params['adset_id'] = adset_id
            
        ads = account.get_ads(
            params=params,
            fields=[
                'id', 'name', 'status', 'adset_id', 'creative', 
                'created_time', 'updated_time'
            ]
        )
        return ads
    except Exception as e:
        st.error(f"Erro ao obter anúncios: {e}")
        return []

# Função para obter insights de campanhas
def get_campaign_insights(account_id, campaign_ids, time_range='last_7d'):
    try:
        params = {
            'level': 'campaign',
            'filtering': [{'field': 'campaign.id', 'operator': 'IN', 'value': campaign_ids}]
        }
        
        if time_range == 'yesterday':
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            params['time_range'] = {'since': yesterday, 'until': yesterday}
        elif time_range == 'last_7d':
            params['date_preset'] = 'last_7d'
        elif time_range == 'last_30d':
            params['date_preset'] = 'last_30d'
        
        account = AdAccount(f'act_{account_id}')
        insights = account.get_insights(
            params=params,
            fields=[
                'campaign_id', 'campaign_name', 'spend', 'impressions', 'clicks', 
                'ctr', 'cpc', 'actions', 'cost_per_action_type'
            ]
        )
        
        processed_insights = []
        for insight in insights:
            insight_dict = insight.export_all_data()
            
            # Processar ações e conversões
            purchases = 0
            if 'actions' in insight_dict:
                for action in insight_dict['actions']:
                    if action['action_type'] == 'purchase':
                        purchases = int(action['value'])
            
            # Calcular CPA
            cpa = 0
            if 'cost_per_action_type' in insight_dict:
                for cost_action in insight_dict['cost_per_action_type']:
                    if cost_action['action_type'] == 'purchase':
                        cpa = float(cost_action['value'])
            
            # Adicionar dados processados
            insight_dict['purchases'] = purchases
            insight_dict['cpa'] = cpa
            
            processed_insights.append(insight_dict)
            
        return processed_insights
    except Exception as e:
        st.error(f"Erro ao obter insights de campanhas: {e}")
        return []

# NOVA FUNÇÃO: Testar pausa de campanha diretamente
def test_pause_campaign(campaign_id):
    st.subheader("Teste de Pausa de Campanha")
    
    try:
        # Inicializar a API
        account_id = init_facebook_api()
        if not account_id:
            st.error("Não foi possível inicializar a API do Facebook")
            return
        
        st.info(f"Testando pausa da campanha ID: {campaign_id}")
        
        # Tentar obter a campanha
        campaign = Campaign(campaign_id)
        
        # Verificar estado atual
        try:
            campaign_data = campaign.api_get(fields=['name', 'status'])
            st.success(f"Campanha encontrada: {campaign_data.get('name')}")
            st.info(f"Status atual: {campaign_data.get('status')}")
        except Exception as e:
            st.error(f"Erro ao acessar a campanha: {str(e)}")
            return
        
        # Tentar pausar a campanha
        try:
            campaign.api_update(params={'status': 'PAUSED'})
            st.success("✅ Campanha pausada com sucesso!")
            
            # Verificar o novo estado
            updated_data = campaign.api_get(fields=['status'])
            st.info(f"Novo status: {updated_data.get('status')}")
        except Exception as e:
            st.error(f"❌ Erro ao pausar campanha: {str(e)}")
            
            # Mostrar detalhes do erro se for um problema de API
            if hasattr(e, 'api_error_code'):
                st.error(f"Código de erro da API: {e.api_error_code}")
                st.error(f"Mensagem de erro da API: {e.api_error_message}")
            
            # Mostrar informações detalhadas sobre o token
            st.warning("Verificando informações do token:")
            try:
                from facebook_business.adobjects.adaccount import AdAccount
                ad_account = AdAccount(f'act_{account_id}')
                ad_account_info = ad_account.api_get()
                st.json(ad_account_info)
            except Exception as account_error:
                st.error(f"Erro ao verificar conta: {str(account_error)}")
    
    except Exception as e:
        st.error(f"Erro geral: {str(e)}")

# NOVA FUNÇÃO: Verificação de regras com debug detalhado
def check_and_apply_rules(insights):
    st.subheader("Log de Verificação de Regras")
    debug_container = st.empty()
    debug_log = []
    
    def add_log(message):
        debug_log.append(message)
        debug_container.code("\n".join(debug_log))
    
    add_log("Iniciando verificação de regras...")
    
    rules = get_all_rules()
    account_id = init_facebook_api()
    
    add_log(f"- Total de regras encontradas: {len(rules)}")
    add_log(f"- Total de insights de campanhas: {len(insights)}")
    
    if not account_id:
        add_log("❌ ERRO: Não foi possível inicializar a API do Facebook (account_id não encontrado)")
        return
    
    if not rules:
        add_log("❌ ERRO: Nenhuma regra encontrada no banco de dados")
        return
    
    if not insights:
        add_log("❌ ERRO: Nenhum insight de campanha disponível para análise")
        return
    
    for insight in insights:
        campaign_id = insight.get('campaign_id')
        campaign_name = insight.get('campaign_name')
        cpa = insight.get('cpa', 0)
        purchases = insight.get('purchases', 0)
        
        add_log(f"\n🔍 Verificando campanha: {campaign_name}")
        add_log(f"- ID: {campaign_id}")
        add_log(f"- CPA: R${cpa:.2f}")
        add_log(f"- Compras: {purchases}")
        
        for rule in rules:
            add_log(f"\n  📋 Verificando regra: {rule['name']}")
            
            if not rule.get('is_active', 1):
                add_log(f"  - Regra inativa, pulando")
                continue
            
            # Iniciar verificação de condições
            add_log(f"  - Status da regra: {'Ativa' if rule.get('is_active', 1) else 'Inativa'}")
            
            # Verificar se estamos usando o formato antigo ou novo de regras
            if 'is_composite' in rule:
                # Novo formato (com regras compostas)
                primary_condition_met = False
                primary_metric_value = None
                
                # Obter valor da métrica primária
                if rule['primary_metric'] == 'cpa':
                    primary_metric_value = cpa
                    add_log(f"  - Verificando condição primária: CPA {rule['primary_operator']} {rule['primary_value']}")
                    add_log(f"  - Valor atual: R${cpa:.2f}")
                elif rule['primary_metric'] == 'purchases':
                    primary_metric_value = purchases
                    add_log(f"  - Verificando condição primária: Compras {rule['primary_operator']} {rule['primary_value']}")
                    add_log(f"  - Valor atual: {purchases}")
                
                # Verificar condição primária
                if primary_metric_value is not None:
                    if rule['primary_operator'] == '<' and primary_metric_value < rule['primary_value']:
                        primary_condition_met = True
                    elif rule['primary_operator'] == '<=' and primary_metric_value <= rule['primary_value']:
                        primary_condition_met = True
                    elif rule['primary_operator'] == '>' and primary_metric_value > rule['primary_value']:
                        primary_condition_met = True
                    elif rule['primary_operator'] == '>=' and primary_metric_value >= rule['primary_value']:
                        primary_condition_met = True
                    elif rule['primary_operator'] == '==' and primary_metric_value == rule['primary_value']:
                        primary_condition_met = True
                
                add_log(f"  - Condição primária atendida: {primary_condition_met}")
                
                # Se regra não é composta, usa apenas a condição primária
                if not rule.get('is_composite', 0):
                    condition_met = primary_condition_met
                else:
                    # Para regra composta, verifica também a condição secundária
                    secondary_condition_met = False
                    secondary_metric_value = None
                    
                    # Obter valor da métrica secundária
                    if rule.get('secondary_metric') == 'cpa':
                        secondary_metric_value = cpa
                        add_log(f"  - Verificando condição secundária: CPA {rule.get('secondary_operator')} {rule.get('secondary_value')}")
                        add_log(f"  - Valor atual: R${cpa:.2f}")
                    elif rule.get('secondary_metric') == 'purchases':
                        secondary_metric_value = purchases
                        add_log(f"  - Verificando condição secundária: Compras {rule.get('secondary_operator')} {rule.get('secondary_value')}")
                        add_log(f"  - Valor atual: {purchases}")
                    
                    # Verificar condição secundária
                    if secondary_metric_value is not None:
                        if rule.get('secondary_operator') == '<' and secondary_metric_value < rule.get('secondary_value', 0):
                            secondary_condition_met = True
                        elif rule.get('secondary_operator') == '<=' and secondary_metric_value <= rule.get('secondary_value', 0):
                            secondary_condition_met = True
                        elif rule.get('secondary_operator') == '>' and secondary_metric_value > rule.get('secondary_value', 0):
                            secondary_condition_met = True
                        elif rule.get('secondary_operator') == '>=' and secondary_metric_value >= rule.get('secondary_value', 0):
                            secondary_condition_met = True
                        elif rule.get('secondary_operator') == '==' and secondary_metric_value == rule.get('secondary_value', 0):
                            secondary_condition_met = True
                    
                    add_log(f"  - Condição secundária atendida: {secondary_condition_met}")
                    
                    # Combina as condições de acordo com o operador de junção
                    if rule.get('join_operator') == 'AND':
                        condition_met = primary_condition_met and secondary_condition_met
                    elif rule.get('join_operator') == 'OR':
                        condition_met = primary_condition_met or secondary_condition_met
                    else:
                        condition_met = primary_condition_met
                    
                    add_log(f"  - Operador de junção: {rule.get('join_operator', 'AND')}")
            else:
                # Formato antigo (regras simples)
                condition_met = False
                metric_value = None
                
                if rule.get('condition_metric') == 'cpa':
                    metric_value = cpa
                    add_log(f"  - Verificando condição: CPA {rule.get('condition_operator')} {rule.get('condition_value')}")
                    add_log(f"  - Valor atual: R${cpa:.2f}")
                elif rule.get('condition_metric') == 'purchases':
                    metric_value = purchases
                    add_log(f"  - Verificando condição: Compras {rule.get('condition_operator')} {rule.get('condition_value')}")
                    add_log(f"  - Valor atual: {purchases}")
                
                if metric_value is not None:
                    if rule.get('condition_operator') == '<' and metric_value < rule.get('condition_value', 0):
                        condition_met = True
                    elif rule.get('condition_operator') == '<=' and metric_value <= rule.get('condition_value', 0):
                        condition_met = True
                    elif rule.get('condition_operator') == '>' and metric_value > rule.get('condition_value', 0):
                        condition_met = True
                    elif rule.get('condition_operator') == '>=' and metric_value >= rule.get('condition_value', 0):
                        condition_met = True
                    elif rule.get('condition_operator') == '==' and metric_value == rule.get('condition_value', 0):
                        condition_met = True
            
            add_log(f"  - Condição final atendida: {condition_met}")
            
            if not condition_met:
                add_log(f"  - Condição não atendida, pulando para próxima regra")
                continue
            
            # Condição atendida, aplicar ação da regra
            add_log(f"  ✅ CONDIÇÃO ATENDIDA! Executando ação: {rule.get('action_type')}")
            
            # Aplicar ação da regra
            try:
                campaign = Campaign(campaign_id)
                
                # Testar se conseguimos acessar a campanha
                try:
                    campaign_data = campaign.api_get(fields=['name', 'status', 'daily_budget', 'lifetime_budget'])
                    add_log(f"  - Campanha acessada com sucesso: {campaign_data.get('name')}")
                    add_log(f"  - Status atual: {campaign_data.get('status')}")
                    add_log(f"  - Orçamento diário: {campaign_data.get('daily_budget')}")
                    add_log(f"  - Orçamento total: {campaign_data.get('lifetime_budget')}")
                except Exception as e:
                    add_log(f"  ❌ ERRO ao acessar dados da campanha: {str(e)}")
                    continue
                
                success = False
                message = ""
                
                if rule.get('action_type') == 'duplicate_budget':
                    add_log(f"  - Tentando duplicar orçamento")
                    
                    if 'daily_budget' in campaign_data and campaign_data['daily_budget']:
                        new_budget = int(campaign_data['daily_budget']) * 2
                        add_log(f"  - Duplicando orçamento diário de {campaign_data['daily_budget']} para {new_budget}")
                        
                        try:
                            campaign.api_update(params={'daily_budget': new_budget})
                            message = f"Orçamento diário duplicado de {campaign_data['daily_budget']} para {new_budget}"
                            success = True
                            add_log(f"  ✅ Sucesso: {message}")
                        except Exception as e:
                            add_log(f"  ❌ ERRO ao atualizar orçamento: {str(e)}")
                    elif 'lifetime_budget' in campaign_data and campaign_data['lifetime_budget']:
                        new_budget = int(campaign_data['lifetime_budget']) * 2
                        add_log(f"  - Duplicando orçamento total de {campaign_data['lifetime_budget']} para {new_budget}")
                        
                        try:
                            campaign.api_update(params={'lifetime_budget': new_budget})
                            message = f"Orçamento total duplicado de {campaign_data['lifetime_budget']} para {new_budget}"
                            success = True
                            add_log(f"  ✅ Sucesso: {message}")
                        except Exception as e:
                            add_log(f"  ❌ ERRO ao atualizar orçamento: {str(e)}")
                    else:
                        add_log(f"  ❌ ERRO: Nenhum orçamento encontrado para duplicar")
                
                elif rule.get('action_type') == 'triple_budget':
                    add_log(f"  - Tentando triplicar orçamento")
                    
                    if 'daily_budget' in campaign_data and campaign_data['daily_budget']:
                        new_budget = int(campaign_data['daily_budget']) * 3
                        add_log(f"  - Triplicando orçamento diário de {campaign_data['daily_budget']} para {new_budget}")
                        
                        try:
                            campaign.api_update(params={'daily_budget': new_budget})
                            message = f"Orçamento diário triplicado de {campaign_data['daily_budget']} para {new_budget}"
                            success = True
                            add_log(f"  ✅ Sucesso: {message}")
                        except Exception as e:
                            add_log(f"  ❌ ERRO ao atualizar orçamento: {str(e)}")
                    elif 'lifetime_budget' in campaign_data and campaign_data['lifetime_budget']:
                        new_budget = int(campaign_data['lifetime_budget']) * 3
                        add_log(f"  - Triplicando orçamento total de {campaign_data['lifetime_budget']} para {new_budget}")
                        
                        try:
                            campaign.api_update(params={'lifetime_budget': new_budget})
                            message = f"Orçamento total triplicado de {campaign_data['lifetime_budget']} para {new_budget}"
                            success = True
                            add_log(f"  ✅ Sucesso: {message}")
                        except Exception as e:
                            add_log(f"  ❌ ERRO ao atualizar orçamento: {str(e)}")
                    else:
                        add_log(f"  ❌ ERRO: Nenhum orçamento encontrado para triplicar")
                
                elif rule.get('action_type') == 'pause_campaign':
                    add_log(f"  - Tentando pausar campanha")
                    
                    try:
                        add_log(f"  - Estado atual: {campaign_data.get('status')}")
                        campaign.api_update(params={'status': 'PAUSED'})
                        message = "Campanha pausada"
                        success = True
                        add_log(f"  ✅ Sucesso: Campanha pausada")
                    except Exception as e:
                        add_log(f"  ❌ ERRO ao pausar campanha: {str(e)}")
                
                elif rule.get('action_type') == 'halve_budget':
                    add_log(f"  - Tentando reduzir orçamento pela metade")
                    
                    if 'daily_budget' in campaign_data and campaign_data['daily_budget']:
                        new_budget = int(campaign_data['daily_budget']) // 2
                        add_log(f"  - Reduzindo orçamento diário de {campaign_data['daily_budget']} para {new_budget}")
                        
                        try:
                            campaign.api_update(params={'daily_budget': new_budget})
                            message = f"Orçamento diário reduzido pela metade de {campaign_data['daily_budget']} para {new_budget}"
                            success = True
                            add_log(f"  ✅ Sucesso: {message}")
                        except Exception as e:
                            add_log(f"  ❌ ERRO ao atualizar orçamento: {str(e)}")
                    elif 'lifetime_budget' in campaign_data and campaign_data['lifetime_budget']:
                        new_budget = int(campaign_data['lifetime_budget']) // 2
                        add_log(f"  - Reduzindo orçamento total de {campaign_data['lifetime_budget']} para {new_budget}")
                        
                        try:
                            campaign.api_update(params={'lifetime_budget': new_budget})
                            message = f"Orçamento total reduzido pela metade de {campaign_data['lifetime_budget']} para {new_budget}"
                            success = True
                            add_log(f"  ✅ Sucesso: {message}")
                        except Exception as e:
                            add_log(f"  ❌ ERRO ao atualizar orçamento: {str(e)}")
                    else:
                        add_log(f"  ❌ ERRO: Nenhum orçamento encontrado para reduzir")
                
                elif rule.get('action_type') == 'custom_budget_multiplier' and rule.get('action_value'):
                    add_log(f"  - Tentando aplicar multiplicador personalizado de {rule.get('action_value')}")
                    
                    if 'daily_budget' in campaign_data and campaign_data['daily_budget']:
                        new_budget = int(int(campaign_data['daily_budget']) * rule.get('action_value', 1))
                        add_log(f"  - Multiplicando orçamento diário de {campaign_data['daily_budget']} por {rule.get('action_value')} = {new_budget}")
                        
                        try:
                            campaign.api_update(params={'daily_budget': new_budget})
                            message = f"Orçamento diário multiplicado por {rule.get('action_value')} de {campaign_data['daily_budget']} para {new_budget}"
                            success = True
                            add_log(f"  ✅ Sucesso: {message}")
                        except Exception as e:
                            add_log(f"  ❌ ERRO ao atualizar orçamento: {str(e)}")
                    elif 'lifetime_budget' in campaign_data and campaign_data['lifetime_budget']:
                        new_budget = int(int(campaign_data['lifetime_budget']) * rule.get('action_value', 1))
                        add_log(f"  - Multiplicando orçamento total de {campaign_data['lifetime_budget']} por {rule.get('action_value')} = {new_budget}")
                        
                        try:
                            campaign.api_update(params={'lifetime_budget': new_budget})
                            message = f"Orçamento total multiplicado por {rule.get('action_value')} de {campaign_data['lifetime_budget']} para {new_budget}"
                            success = True
                            add_log(f"  ✅ Sucesso: {message}")
                        except Exception as e:
                            add_log(f"  ❌ ERRO ao atualizar orçamento: {str(e)}")
                    else:
                        add_log(f"  ❌ ERRO: Nenhum orçamento encontrado para multiplicar")
                
                # Registrar execução da regra
                add_log(f"  - Registrando execução no histórico: {success}, {message}")
                log_result = log_rule_execution(
                    rule_id=rule.get('id'),
                    ad_object_id=campaign_id,
                    ad_object_type='campaign',
                    ad_object_name=campaign_name,
                    was_successful=success,
                    message=message
                )
                add_log(f"  - Resultado do log: {log_result}")
                
            except Exception as e:
                error_message = f"Erro ao aplicar regra: {str(e)}"
                add_log(f"  ❌ ERRO GERAL: {error_message}")
                log_rule_execution(
                    rule_id=rule.get('id'),
                    ad_object_id=campaign_id,
                    ad_object_type='campaign',
                    ad_object_name=campaign_name,
                    was_successful=False,
                    message=error_message
                )
    
    add_log("\nVerificação de regras concluída!")

# Interface do Streamlit
def main():
    st.title("Gerenciador de Anúncios do Facebook")
    
    # Barra lateral para navegação
    st.sidebar.title("Navegação")
    
    # Verificar se a API já está configurada
    active_config = get_active_api_config()
    all_configs = get_all_api_configs()
    account_id = None
    
    # Seletor de contas na barra lateral
    if all_configs:
        st.sidebar.subheader("Selecionar Conta")
        
        # Criar opções para o selectbox
        config_options = [f"{config['name']} ({config['account_id']})" for config in all_configs]
        config_index = 0
        
        # Encontrar índice da configuração ativa
        for i, config in enumerate(all_configs):
            if config["is_active"] == 1:
                config_index = i
                break
        
        # Selectbox para escolher conta
        selected_config_index = st.sidebar.selectbox(
            "Conta de Anúncios:", 
            range(len(config_options)),
            format_func=lambda i: config_options[i],
            index=config_index
        )
        
        # Aplicar seleção
        selected_config_id = all_configs[selected_config_index]["id"]
        if all_configs[selected_config_index]["is_active"] != 1:
            set_active_api_config(selected_config_id)
            st.rerun()
        
        # Mostrar detalhes da conexão ativa
        active_config = all_configs[selected_config_index]
        account_id = active_config["account_id"]
        
        try:
            # Inicializar API do Facebook
            FacebookAdsApi.init(
                app_id=active_config["app_id"],
                app_secret=active_config["app_secret"],
                access_token=active_config["access_token"]
            )
            st.sidebar.success(f"Conectado: {active_config['name']}")
        except Exception as e:
            st.sidebar.error(f"Erro na conexão: {e}")
    
    # Menu de navegação
    page = st.sidebar.radio(
        "Selecione uma página:",
        ["Configuração de Contas", "Campanhas", "Conjuntos de Anúncios", "Anúncios", "Regras", "Execuções", "Dashboard"]
    )
    
    # Verificar se existe pelo menos uma configuração
    if not all_configs:
        if page != "Configuração de Contas":
            st.warning("Por favor, configure pelo menos uma conta do Facebook primeiro.")
            page = "Configuração de Contas"
    
    # Página: Configuração de Contas
    if page == "Configuração de Contas":
        st.header("Gerenciamento de Contas de Anúncios")
        
        # Exibir contas existentes
        if all_configs:
            st.subheader("Contas Configuradas")
            
            for config in all_configs:
                with st.expander(f"{config['name']} ({config['account_id']})"):
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.markdown(f"**Nome:** {config['name']}")
                        st.markdown(f"**Account ID:** {config['account_id']}")
                        if config['business_id']:
                            st.markdown(f"**Business Manager ID:** {config['business_id']}")
                        if config['page_id']:
                            st.markdown(f"**Página ID:** {config['page_id']}")
                        st.markdown(f"**Status:** {'✅ Ativa' if config['is_active'] == 1 else 'Inativa'}")
                    
                    with col2:
                        if st.button("Ativar", key=f"activate_{config['id']}", disabled=config['is_active'] == 1):
                            set_active_api_config(config['id'])
                            st.success(f"Conta {config['name']} ativada com sucesso!")
                            st.rerun()
                        
                        if st.button("Excluir", key=f"delete_{config['id']}"):
                            if delete_api_config(config['id']):
                                st.success(f"Conta {config['name']} excluída com sucesso!")
                                st.rerun()
        
        # Formulário para adicionar nova conta
        st.subheader("Adicionar Nova Conta")
        with st.form("api_config_form"):
            name = st.text_input("Nome da Conexão (ex: Cliente A - Página Principal)")
            app_id = st.text_input("App ID")
            app_secret = st.text_input("App Secret", type="password")
            access_token = st.text_input("Access Token")
            account_id = st.text_input("Account ID (sem 'act_')")
            
            # Campos opcionais
            with st.expander("Configurações Avançadas (Opcional)"):
                business_id = st.text_input("Business Manager ID")
                page_id = st.text_input("Página ID")
            
            submitted = st.form_submit_button("Adicionar Conta")
            
            if submitted:
                if name and app_id and app_secret and access_token and account_id:
                    if save_api_config(name, app_id, app_secret, access_token, account_id, business_id, page_id):
                        st.success(f"Conta '{name}' adicionada com sucesso!")
                        st.rerun()
                else:
                    st.error("Os campos Nome, App ID, App Secret, Access Token e Account ID são obrigatórios.")
        
        st.subheader("Como obter credenciais do Facebook")
        st.markdown("""
        1. Acesse [Facebook Developers](https://developers.facebook.com/)
        2. Crie ou use um aplicativo existente
        3. Adicione o produto "Marketing API" ao seu aplicativo
        4. Gere um token de acesso na seção de ferramentas do Marketing API
        5. O Account ID pode ser encontrado no Gerenciador de Anúncios do Facebook
        
        **Dica:** Para gerenciar múltiplas contas, recomenda-se criar um aplicativo no Business Manager e solicitar tokens de acesso de longa duração.
        """)
    
    # Página: Campanhas (ATUALIZADA)
    elif page == "Campanhas" and account_id:
        st.header("Campanhas")
        
        # Opções de filtro por período
        time_range = st.selectbox(
            "Selecione o período:",
            ["last_7d", "last_30d", "yesterday"],
            format_func=lambda x: {
                "last_7d": "Últimos 7 dias", 
                "last_30d": "Últimos 30 dias", 
                "yesterday": "Ontem"
            }.get(x)
        )
        
        # Adicionar campo para ID de campanha para teste direto
        test_campaign_id = st.text_input("ID da Campanha para Teste Direto (opcional)")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Botão para atualizar dados
            if st.button("Atualizar Campanhas"):
                with st.spinner("Carregando campanhas..."):
                    campaigns = get_facebook_campaigns(account_id)
                    
                    if campaigns:
                        # Preparar dados para tabela
                        campaign_data = []
                        campaign_ids = []
                        
                        for campaign in campaigns:
                            campaign_dict = campaign.export_all_data()
                            campaign_data.append({
                                "ID": campaign_dict.get("id"),
                                "Nome": campaign_dict.get("name"),
                                "Status": campaign_dict.get("status"),
                                "Objetivo": campaign_dict.get("objective"),
                                "Orçamento Diário": campaign_dict.get("daily_budget"),
                                "Orçamento Total": campaign_dict.get("lifetime_budget"),
                                "Data de Criação": campaign_dict.get("created_time")
                            })
                            campaign_ids.append(campaign_dict.get("id"))
                        
                        # Exibir tabela de campanhas
                        st.subheader("Lista de Campanhas")
                        campaign_df = pd.DataFrame(campaign_data)
                        st.dataframe(campaign_df)
                        
                        # Obter e exibir insights
                        if campaign_ids:
                            with st.spinner("Carregando insights..."):
                                insights = get_campaign_insights(account_id, campaign_ids, time_range)
                                
                                if insights:
                                    insight_data = []
                                    for insight in insights:
                                        insight_data.append({
                                            "ID da Campanha": insight.get("campaign_id"),
                                            "Nome da Campanha": insight.get("campaign_name"),
                                            "Gasto (R$)": float(insight.get("spend", 0)),
                                            "Impressões": int(insight.get("impressions", 0)),
                                            "Cliques": int(insight.get("clicks", 0)),
                                            "CTR": float(insight.get("ctr", 0)) * 100,
                                            "CPC (R$)": float(insight.get("cpc", 0)),
                                            "Compras": insight.get("purchases", 0),
                                            "CPA (R$)": float(insight.get("cpa", 0))
                                        })
                                    
                                    st.subheader(f"Insights das Campanhas ({time_range})")
                                    insight_df = pd.DataFrame(insight_data)
                                    st.dataframe(insight_df)
                                    
                                    # Verificar e aplicar regras com versão de debug
                                    st.subheader("Verificação de Regras")
                                    with st.spinner("Verificando e aplicando regras..."):
                                        # Usar nossa função de debug
                                        check_and_apply_rules(insights)
                                else:
                                    st.info("Nenhum insight encontrado.")
                        else:
                            st.info("Nenhuma campanha encontrada para obter insights.")
                    else:
                        st.info("Nenhuma campanha encontrada.")
                        
        with col2:
            if test_campaign_id and st.button("Testar Pausa Direta"):
                test_pause_campaign(test_campaign_id)
    
    # Página: Conjuntos de Anúncios
    elif page == "Conjuntos de Anúncios" and account_id:
        st.header("Conjuntos de Anúncios")
        
        # Obter campanhas para filtro
        campaigns = get_facebook_campaigns(account_id)
        campaign_options = [{"label": campaign["name"], "value": campaign["id"]} for campaign in campaigns]
        campaign_options.insert(0, {"label": "Todas as campanhas", "value": ""})
        
        # Filtro de campanhas
        selected_campaign = st.selectbox(
            "Filtrar por campanha:",
            options=[opt["value"] for opt in campaign_options],
            format_func=lambda x: next((opt["label"] for opt in campaign_options if opt["value"] == x), x)
        )
        
        # Botão para atualizar dados
        if st.button("Atualizar Conjuntos de Anúncios"):
            with st.spinner("Carregando conjuntos de anúncios..."):
                adsets = get_facebook_adsets(account_id, selected_campaign)
                
                if adsets:
                    # Preparar dados para tabela
                    adset_data = []
                    
                    for adset in adsets:
                        adset_dict = adset.export_all_data()
                        adset_data.append({
                            "ID": adset_dict.get("id"),
                            "Nome": adset_dict.get("name"),
                            "Status": adset_dict.get("status"),
                            "ID da Campanha": adset_dict.get("campaign_id"),
                            "Orçamento Diário": adset_dict.get("daily_budget"),
                            "Orçamento Total": adset_dict.get("lifetime_budget"),
                            "Valor da Oferta": adset_dict.get("bid_amount")
                        })
                    
                    # Exibir tabela de conjuntos de anúncios
                    st.subheader("Lista de Conjuntos de Anúncios")
                    adset_df = pd.DataFrame(adset_data)
                    st.dataframe(adset_df)
                else:
                    st.info("Nenhum conjunto de anúncios encontrado.")
    
    # Página: Anúncios
    elif page == "Anúncios" and account_id:
        st.header("Anúncios")
        
        # Obter conjuntos de anúncios para filtro
        adsets = get_facebook_adsets(account_id)
        adset_options = [{"label": adset["name"], "value": adset["id"]} for adset in adsets]
        adset_options.insert(0, {"label": "Todos os conjuntos de anúncios", "value": ""})
        
        # Filtro de conjuntos de anúncios
        selected_adset = st.selectbox(
            "Filtrar por conjunto de anúncios:",
            options=[opt["value"] for opt in adset_options],
            format_func=lambda x: next((opt["label"] for opt in adset_options if opt["value"] == x), x)
        )
        
        # Botão para atualizar dados
        if st.button("Atualizar Anúncios"):
            with st.spinner("Carregando anúncios..."):
                ads = get_facebook_ads(account_id, selected_adset)
                
                if ads:
                    # Preparar dados para tabela
                    ad_data = []
                    
                    for ad in ads:
                        ad_dict = ad.export_all_data()
                        ad_data.append({
                            "ID": ad_dict.get("id"),
                            "Nome": ad_dict.get("name"),
                            "Status": ad_dict.get("status"),
                            "ID do Conjunto": ad_dict.get("adset_id"),
                            "Data de Criação": ad_dict.get("created_time"),
                            "Última Atualização": ad_dict.get("updated_time")
                        })
                    
                    # Exibir tabela de anúncios
                    st.subheader("Lista de Anúncios")
                    ad_df = pd.DataFrame(ad_data)
                    st.dataframe(ad_df)
                else:
                    st.info("Nenhum anúncio encontrado.")
    
    # Página: Regras
    elif page == "Regras":
        st.header("Gerenciamento de Regras")
        
        # Inicializar variáveis de estado de sessão para interface dinâmica
        if 'is_composite' not in st.session_state:
            st.session_state.is_composite = True
        
        if 'primary_metric' not in st.session_state:
            st.session_state.primary_metric = 'cpa'
        
        if 'secondary_metric' not in st.session_state:
            st.session_state.secondary_metric = 'purchases'
        
        # Exibir regras existentes
        rules = get_all_rules()
        if rules:
            st.subheader("Regras Existentes")
            
            for rule in rules:
                with st.expander(f"{rule['name']} - {'Ativa' if rule['is_active'] else 'Inativa'}"):
                    col1, col2, col3 = st.columns([2, 2, 1])
                    
                    with col1:
                        st.markdown(f"**Descrição:** {rule['description']}")
                        
                        # Exibir condições no novo formato
                        if 'is_composite' in rule and rule['is_composite']:
                            primary_metric_name = "CPA" if rule['primary_metric'] == 'cpa' else "Compras"
                            primary_value_fmt = f"R${rule['primary_value']:.2f}" if rule['primary_metric'] == 'cpa' else f"{int(rule['primary_value'])}"
                            
                            secondary_metric_name = "CPA" if rule['secondary_metric'] == 'cpa' else "Compras"
                            secondary_value_fmt = f"R${rule['secondary_value']:.2f}" if rule['secondary_metric'] == 'cpa' else f"{int(rule['secondary_value'])}"
                            
                            join_op = "E" if rule['join_operator'] == "AND" else "OU"
                            
                            st.markdown(f"**Condição 1:** {primary_metric_name} {rule['primary_operator']} {primary_value_fmt}")
                            st.markdown(f"**Condição 2:** {secondary_metric_name} {rule['secondary_operator']} {secondary_value_fmt}")
                            st.markdown(f"**Operador de Junção:** {join_op}")
                        else:
                            # Compatibilidade com regras no formato antigo
                            if 'condition_metric' in rule:
                                metric_name = "CPA" if rule['condition_metric'] == 'cpa' else "Compras"
                                value_fmt = f"{rule['condition_value']}"
                                st.markdown(f"**Condição:** {metric_name} {rule['condition_operator']} {value_fmt}")
                            else:
                                metric_name = "CPA" if rule['primary_metric'] == 'cpa' else "Compras"
                                value_fmt = f"R${rule['primary_value']:.2f}" if rule['primary_metric'] == 'cpa' else f"{int(rule['primary_value'])}"
                                st.markdown(f"**Condição:** {metric_name} {rule['primary_operator']} {value_fmt}")
                    
                    with col2:
                        action_text = rule['action_type']
                        if rule['action_type'] == 'custom_budget_multiplier':
                            action_text = f"Multiplicar orçamento por {rule['action_value']}"
                        elif rule['action_type'] == 'duplicate_budget':
                            action_text = "Duplicar orçamento"
                        elif rule['action_type'] == 'triple_budget':
                            action_text = "Triplicar orçamento"
                        elif rule['action_type'] == 'pause_campaign':
                            action_text = "Pausar campanha"
                        elif rule['action_type'] == 'halve_budget':
                            action_text = "Reduzir orçamento pela metade"
                            
                        st.markdown(f"**Ação:** {action_text}")
                        st.markdown(f"**Criada em:** {rule['created_at']}")
                    
                    with col3:
                        if st.button("Excluir", key=f"delete_{rule['id']}"):
                            if delete_rule(rule['id']):
                                st.success("Regra excluída com sucesso!")
                                st.rerun()
                        
                        status = st.checkbox(
                            "Ativa", 
                            value=rule['is_active'], 
                            key=f"status_{rule['id']}",
                            on_change=lambda: toggle_rule_status(rule['id'], not rule['is_active'])
                        )
        
        st.subheader("Criar Nova Regra")
        
        # Interface dinâmica fora do formulário
        # Checkbox para regra composta
        if st.checkbox("Usar duas condições (regra composta)", value=st.session_state.is_composite):
            st.session_state.is_composite = True
        else:
            st.session_state.is_composite = False
        
        # Operador de junção para regras compostas
        if st.session_state.is_composite:
            st.subheader("Configuração de Condições")
            join_operator = st.radio(
                "Operador de Junção:",
                ["AND", "OR"],
                format_func=lambda x: {"AND": "E (ambas condições devem ser verdadeiras)", 
                                      "OR": "OU (pelo menos uma condição deve ser verdadeira)"}.get(x)
            )
        else:
            join_operator = "AND"  # Default para regras simples
        
        # Configurar primeira condição - fora do formulário
        st.markdown("**Primeira Condição**")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.selectbox(
                "Métrica da Primeira Condição",
                options=["cpa", "purchases"],
                format_func=lambda x: {"cpa": "CPA", "purchases": "Compras"}.get(x),
                index=0 if st.session_state.primary_metric == 'cpa' else 1,
                key="primary_metric_select"
            ) == "cpa":
                st.session_state.primary_metric = "cpa"
            else:
                st.session_state.primary_metric = "purchases"
        
        # Configurar segunda condição - fora do formulário
        if st.session_state.is_composite:
            st.markdown("**Segunda Condição**")
            col1, col2 = st.columns(2)
            
            with col1:
                if st.selectbox(
                    "Métrica da Segunda Condição",
                    options=["cpa", "purchases"],
                    format_func=lambda x: {"cpa": "CPA", "purchases": "Compras"}.get(x),
                    index=0 if st.session_state.secondary_metric == 'cpa' else 1,
                    key="secondary_metric_select"
                ) == "cpa":
                    st.session_state.secondary_metric = "cpa"
                else:
                    st.session_state.secondary_metric = "purchases"
        
        # Formulário para input e submissão
        with st.form("new_rule_form"):
            name = st.text_input("Nome da Regra")
            description = st.text_area("Descrição")
            
            # Primeira condição dentro do formulário
            st.markdown("**Configuração da Primeira Condição**")
            col1, col2 = st.columns(2)
            
            with col1:
                primary_operator = st.selectbox(
                    "Operador",
                    options=["<", "<=", ">", ">=", "=="],
                    format_func=lambda x: {
                        "<": "Menor que", 
                        "<=": "Menor ou igual a", 
                        ">": "Maior que", 
                        ">=": "Maior ou igual a", 
                        "==": "Igual a"
                    }.get(x),
                    key="primary_operator"
                )
            
            with col2:
                # Tipo do valor dependendo da métrica salva no session_state
                if st.session_state.primary_metric == "cpa":
                    primary_value = st.number_input(
                        "Valor (R$)", 
                        min_value=0.0, 
                        step=0.1,
                        format="%.2f",
                        key="primary_value_cpa",
                        value=10.0
                    )
                else:  # purchases
                    primary_value = st.number_input(
                        "Quantidade", 
                        min_value=0, 
                        step=1,
                        value=2,
                        key="primary_value_purchases"
                    )
            
            # Segunda condição (se for regra composta)
            secondary_metric = None
            secondary_operator = None
            secondary_value = None
            
            if st.session_state.is_composite:
                st.markdown("**Configuração da Segunda Condição**")
                col1, col2 = st.columns(2)
                
                with col1:
                    secondary_operator = st.selectbox(
                        "Operador",
                        options=["<", "<=", ">", ">=", "=="],
                        format_func=lambda x: {
                            "<": "Menor que", 
                            "<=": "Menor ou igual a", 
                            ">": "Maior que", 
                            ">=": "Maior ou igual a", 
                            "==": "Igual a"
                        }.get(x),
                        key="secondary_operator"
                    )
                
                with col2:
                    # Tipo do valor dependendo da métrica secundária
                    if st.session_state.secondary_metric == "cpa":
                        secondary_value = st.number_input(
                            "Valor (R$)", 
                            min_value=0.0, 
                            step=0.1,
                            format="%.2f",
                            key="secondary_value_cpa",
                            value=15.0
                        )
                    else:  # purchases
                        secondary_value = st.number_input(
                            "Quantidade", 
                            min_value=0, 
                            step=1,
                            value=4,
                            key="secondary_value_purchases"
                        )
            
            # Ação a ser executada
            st.subheader("Ação a Executar")
            col1, col2 = st.columns(2)
            
            with col1:
                action_type = st.selectbox(
                    "Tipo de Ação",
                    options=[
                        "duplicate_budget", 
                        "triple_budget", 
                        "pause_campaign", 
                        "halve_budget",
                        "custom_budget_multiplier"
                    ],
                    format_func=lambda x: {
                        "duplicate_budget": "Duplicar orçamento", 
                        "triple_budget": "Triplicar orçamento", 
                        "pause_campaign": "Pausar campanha", 
                        "halve_budget": "Reduzir orçamento pela metade",
                        "custom_budget_multiplier": "Multiplicar orçamento por valor personalizado"
                    }.get(x)
                )
            
            with col2:
                action_value = None
                if action_type == "custom_budget_multiplier":
                    action_value = st.number_input("Multiplicador de orçamento", min_value=0.1, value=1.5, step=0.1)
            
            # Resumo da regra
            st.subheader("Resumo da Regra")
            
            rule_summary = f"**SE** "
            
            # Formatar condição primária
            primary_metric_name = "CPA" if st.session_state.primary_metric == "cpa" else "Número de Compras"
            operator_symbol = primary_operator
            primary_value_fmt = f"R${primary_value:.2f}" if st.session_state.primary_metric == "cpa" else f"{int(primary_value)}"
            rule_summary += f"{primary_metric_name} {operator_symbol} {primary_value_fmt}"
            
            # Adicionar condição secundária se for regra composta
            if st.session_state.is_composite:
                operator_text = " E " if join_operator == "AND" else " OU "
                secondary_metric_name = "CPA" if st.session_state.secondary_metric == "cpa" else "Número de Compras"
                operator_symbol = secondary_operator
                secondary_value_fmt = f"R${secondary_value:.2f}" if st.session_state.secondary_metric == "cpa" else f"{int(secondary_value)}"
                rule_summary += f"{operator_text}{secondary_metric_name} {operator_symbol} {secondary_value_fmt}"
            
            # Adicionar ação
            action_text = ""
            if action_type == "duplicate_budget":
                action_text = "Duplicar orçamento"
            elif action_type == "triple_budget":
                action_text = "Triplicar orçamento"
            elif action_type == "pause_campaign":
                action_text = "Pausar campanha"
            elif action_type == "halve_budget":
                action_text = "Reduzir orçamento pela metade"
            elif action_type == "custom_budget_multiplier":
                action_text = f"Multiplicar orçamento por {action_value}"
            
            rule_summary += f", **ENTÃO** {action_text}"
            
            st.markdown(rule_summary)
            
            submitted = st.form_submit_button("Criar Regra")
            
            if submitted:
                required_fields_ok = name and primary_value is not None and action_type
                
                if st.session_state.is_composite:
                    required_fields_ok = required_fields_ok and secondary_value is not None
                
                if required_fields_ok:
                    # Pegar os valores das métricas do session_state
                    primary_metric = st.session_state.primary_metric
                    
                    # Garantir que o valor correto seja usado com base na métrica
                    if primary_metric == "cpa":
                        final_primary_value = primary_value
                    else:
                        final_primary_value = int(primary_value)
                    
                    if st.session_state.is_composite:
                        secondary_metric = st.session_state.secondary_metric
                        if secondary_metric == "cpa":
                            final_secondary_value = secondary_value
                        else:
                            final_secondary_value = int(secondary_value)
                    else:
                        secondary_metric = None
                        final_secondary_value = None
                    
                    if add_rule(
                        name, description, "custom", primary_metric, primary_operator, 
                        final_primary_value, action_type, action_value, st.session_state.is_composite, 
                        secondary_metric, secondary_operator, final_secondary_value, join_operator
                    ):
                        st.success("Regra criada com sucesso!")
                        st.rerun()
                else:
                    st.error("Preencha todos os campos obrigatórios.")
    
    # Página: Execuções
    elif page == "Execuções":
        st.header("Histórico de Execuções de Regras")
        
        # Botão para atualizar histórico
        if st.button("Atualizar Histórico"):
            st.rerun()
        
        # Obter histórico de execuções
        executions = get_rule_executions()
        
        if executions:
            execution_data = []
            
            for execution in executions:
                execution_data.append({
                    "ID": execution.get("id"),
                    "Regra": execution.get("rule_name"),
                    "Objeto": f"{execution.get('ad_object_name')} ({execution.get('ad_object_type')})",
                    "Data de Execução": execution.get("executed_at"),
                    "Sucesso": "Sim" if execution.get("was_successful") else "Não",
                    "Mensagem": execution.get("message")
                })
            
            # Exibir tabela de execuções
            execution_df = pd.DataFrame(execution_data)
            st.dataframe(execution_df)
        else:
            st.info("Nenhum histórico de execução encontrado.")
    
    # Página: Dashboard
    elif page == "Dashboard" and account_id:
        st.header("Dashboard")
        
        # Opções de filtro por período
        time_range = st.selectbox(
            "Selecione o período:",
            ["last_7d", "last_30d", "yesterday"],
            format_func=lambda x: {
                "last_7d": "Últimos 7 dias", 
                "last_30d": "Últimos 30 dias", 
                "yesterday": "Ontem"
            }.get(x)
        )
        
        # Botão para atualizar dashboard
        if st.button("Atualizar Dashboard"):
            with st.spinner("Carregando dados..."):
                campaigns = get_facebook_campaigns(account_id)
                
                if campaigns:
                    campaign_ids = [campaign["id"] for campaign in campaigns]
                    insights = get_campaign_insights(account_id, campaign_ids, time_range)
                    
                    if insights:
                        # Métricas gerais
                        total_spend = sum(float(insight.get("spend", 0)) for insight in insights)
                        total_impressions = sum(int(insight.get("impressions", 0)) for insight in insights)
                        total_clicks = sum(int(insight.get("clicks", 0)) for insight in insights)
                        total_purchases = sum(insight.get("purchases", 0) for insight in insights)
                        
                        # Calcular médias
                        avg_ctr = total_clicks / total_impressions * 100 if total_impressions > 0 else 0
                        avg_cpc = total_spend / total_clicks if total_clicks > 0 else 0
                        avg_cpa = total_spend / total_purchases if total_purchases > 0 else 0
                        
                        # Exibir métricas em cards
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.metric("Gasto Total", f"R$ {total_spend:.2f}")
                        
                        with col2:
                            st.metric("Compras", total_purchases)
                        
                        with col3:
                            st.metric("CTR Médio", f"{avg_ctr:.2f}%")
                        
                        with col4:
                            st.metric("CPA Médio", f"R$ {avg_cpa:.2f}")
                        
                        # Gráfico de desempenho por campanha
                        st.subheader("Desempenho por Campanha")
                        
                        campaign_data = []
                        for insight in insights:
                            campaign_data.append({
                                "Campanha": insight.get("campaign_name", "Desconhecida"),
                                "Gasto": float(insight.get("spend", 0)),
                                "Compras": insight.get("purchases", 0),
                                "CPA": float(insight.get("cpa", 0))
                            })
                        
                        campaign_df = pd.DataFrame(campaign_data)
                        
                        # Gráfico de barras para gasto e compras
                        st.bar_chart(campaign_df, x="Campanha", y=["Gasto", "Compras"])
                        
                        # Gráfico de barras para CPA
                        st.subheader("CPA por Campanha")
                        st.bar_chart(campaign_df, x="Campanha", y=["CPA"])
                        
                        # Tabela detalhada
                        st.subheader("Dados Detalhados")
                        st.dataframe(campaign_df)
                    else:
                        st.info("Nenhum insight encontrado para o período selecionado.")
                else:
                    st.info("Nenhuma campanha encontrada.")

if __name__ == "__main__":
    main()