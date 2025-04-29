# %%
# ==============================================================================
# Imports
# ==============================================================================
import time
import json
import math # Necessário para ceil
import re
import logging
import pandas as pd
import pyodbc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import concurrent.futures
import threading
import sys
import random
import decimal
from urllib.parse import urljoin, urlparse

# %%
# ==============================================================================
# CONFIGURAÇÕES GERAIS E DE EXECUÇÃO
# ==============================================================================
BASE_DOMAIN = 'https://www.mundoverde.com.br'

# +++ CONFIGURAÇÃO DOS DEPARTAMENTOS E PAGINAÇÃO +++
DEPARTMENT_CONFIG = {
    "Ofertas": {
        "base_url": f'{BASE_DOMAIN}/ofertas?order=OrderByTopSaleDESC',
        "total_products_to_scrape": 60,
        "page_size": 18 
    },
    "Alimentos-Bebidas": {
        "base_url": f'{BASE_DOMAIN}/alimentos-e-bebidas?order=OrderByTopSaleDESC',
        "total_products_to_scrape": 60,
        "page_size": 12
    },
    "Suplementos": {
        "base_url": f'{BASE_DOMAIN}/suplementos?order=OrderByTopSaleDESC',
        "total_products_to_scrape": 60,
        "page_size": 12
    },
    "Bem-Estar": {
        "base_url": f'{BASE_DOMAIN}/bem-estar?order=OrderByTopSaleDESC',
        "total_products_to_scrape": 60,
        "page_size": 12
    }
}

# --- Configurações DB e Log ---
DB_TABLE_NAME = 'ST_PRODUTO_PRECO_GERAL'
CREDENTIAL_PATH = r'C:\Users\Michael\DW\DATA_MART_VENDAS\ETL_PYTHON\credencial_banco_st.json'
LOG_FILE = 'scraping_mundoverde_depto_page_scroll_jsonld.log' # Log

# --- Configurações Técnicas ---
NUM_WORKERS = 8
MAX_RETRIES = 2
RETRY_DELAY_SECONDS = 10
PAGE_ELEMENT_TIMEOUT = 40 # Aumentado ligeiramente
PAGE_LOAD_TIMEOUT_SELENIUM = 50 # Aumentado ligeiramente
SCROLL_PAUSE_TIME = 1.5 # Tempo de pausa entre scrolls DENTRO de uma página
FINAL_LOAD_WAIT_SECONDS = 6 # Tempo de espera APÓS o scroll da PÁGINA parar

LOG_LEVEL = logging.DEBUG

# ==============================================================================
# CONFIGURAÇÃO DO LOGGING (sem alterações)
# ==============================================================================
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - [%(funcName)s] - %(message)s')
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
for handler in logger.handlers[:]: logger.removeHandler(handler)
try:
    file_handler = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)
except PermissionError: print(f"Erro: Sem permissão para escrever no log '{LOG_FILE}'."); sys.exit(1)
console_handler = logging.StreamHandler(); console_handler.setFormatter(log_formatter); logger.addHandler(console_handler)

# ==============================================================================
# FUNÇÕES HELPER (sem alterações)
# ==============================================================================
def create_chrome_options():
    options = Options()
    # options.add_argument('--headless')
    options.add_argument("--disable-gpu"); options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage"); options.add_argument("--disable-extensions")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    options.add_argument('--window-size=1920,1080'); options.add_argument('--lang=pt-BR')
    return options

def clean_price(price_text):
    if isinstance(price_text, (int, float)): return float(price_text)
    if not price_text: return None
    try:
        cleaned = re.sub(r'[R$\s]', '', str(price_text)).replace('.', '', str(price_text).count('.') - 1).replace(',', '.')
        if cleaned and cleaned != '.': return float(cleaned)
        else: return None
    except: return None

# ==============================================================================
# FUNÇÃO PRINCIPAL DE SCRAPING (POR PÁGINA DE DEPARTAMENTO + SCROLL + JSON-LD)
# ==============================================================================
def scrape_department_page(department_name, base_url, page_num):
    """
    Raspa UMA PÁGINA específica de um departamento, rolando-a até o fim
    e usando JSON-LD para extrair dados.
    """
    page_url = f"{base_url}&page={page_num}"
    page_start_time = time.time()
    logging.info(f"Processando Depto '{department_name}' - Página {page_num}...")
    driver = None
    extracted_data = [] 

    for attempt in range(1, MAX_RETRIES + 1):
        logging.debug(f"Depto '{department_name}' P.{page_num}: Tentativa {attempt}/{MAX_RETRIES}")
        driver = None
        try:
            chrome_options = create_chrome_options()
            try:
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
                driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT_SELENIUM)
                wait = WebDriverWait(driver, PAGE_ELEMENT_TIMEOUT)
                logging.debug(f"Depto '{department_name}' P.{page_num}: WebDriver inicializado.")
            except WebDriverException as init_err:
                logging.error(f"Depto '{department_name}' P.{page_num}, Tentativa {attempt}: Falha WebDriver init: {init_err}")
                raise

            logging.debug(f"Depto '{department_name}' P.{page_num}: Carregando URL {page_url}...")
            driver.get(page_url)

            # --- Espera Carregamento Básico da Página ---
            gallery_selector = ".vtex-search-result-3-x-gallery"
            logging.debug(f"Depto '{department_name}' P.{page_num}: Esperando container '{gallery_selector}'...")
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, gallery_selector)))
                logging.debug(f"Depto '{department_name}' P.{page_num}: Container encontrado.")
                time.sleep(3) # Pausa para renderização inicial
            except TimeoutException:
                 logging.warning(f"Depto '{department_name}' P.{page_num}: Timeout container '{gallery_selector}'. Verificando se página está vazia...")
                 # Se o container não aparece, a página pode estar vazia (além do limite de produtos)
                 # ou houve um erro real. Tentaremos pegar o JSON mesmo assim.

            # <<< SCROLL DA PÁGINA ATUAL >>>
            # Mesmo em páginas fixas, rolar garante que todos os elementos,
            # especialmente os do fim, sejam renderizados e possam carregar dados.
            logging.info(f"Depto '{department_name}' P.{page_num}: Iniciando rolagem da página atual...")
            last_height = driver.execute_script("return document.body.scrollHeight")
            scroll_attempts_page = 0
            max_scroll_attempts_page = 5 # Limite de tentativas de scroll por página

            while scroll_attempts_page < max_scroll_attempts_page:
                logging.debug(f"Depto '{department_name}' P.{page_num}: Rolando. Altura: {last_height}")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(SCROLL_PAUSE_TIME)
                try:
                    new_height = driver.execute_script("return document.body.scrollHeight")
                except Exception as scroll_err:
                     logging.warning(f"Depto '{department_name}' P.{page_num}: Erro ao obter nova altura: {scroll_err}. Continuando.")
                     new_height = last_height # Assume que não mudou para evitar loop

                # Em páginas fixas, a altura não DEVE mudar após o carregamento inicial.
                # Se não mudar, consideramos que chegamos ao fim do conteúdo visível.
                if new_height == last_height:
                    logging.debug(f"Depto '{department_name}' P.{page_num}: Altura permaneceu {new_height}. Fim do scroll da página.")
                    break
                last_height = new_height
                scroll_attempts_page += 1

            if scroll_attempts_page == max_scroll_attempts_page:
                 logging.warning(f"Depto '{department_name}' P.{page_num}: Limite de tentativas de scroll da página atingido.")

            logging.info(f"Depto '{department_name}' P.{page_num}: Rolagem da página concluída. Esperando {FINAL_LOAD_WAIT_SECONDS}s...")
            time.sleep(FINAL_LOAD_WAIT_SECONDS)
            # <<< FIM DO SCROLL DA PÁGINA ATUAL >>>

            # --- Extração JSON-LD ---
            logging.info(f"Depto '{department_name}' P.{page_num}: Iniciando extração JSON-LD...")
            try:
                page_source_html = driver.page_source
                soup = BeautifulSoup(page_source_html, 'html.parser')
            except Exception as ps_err:
                 logging.error(f"Depto '{department_name}' P.{page_num}: Erro ao obter page source: {ps_err}")
                 raise

            script_tags = soup.find_all('script', type='application/ld+json')
            logging.info(f"Depto '{department_name}' P.{page_num}: Encontradas {len(script_tags)} tags JSON-LD.")

            processed_product_urls_page = set()

            if not script_tags:
                 logging.warning(f"Depto '{department_name}' P.{page_num}: Nenhuma tag JSON-LD encontrada!")

            for idx, script_tag in enumerate(script_tags):
                try:
                    script_content = script_tag.string
                    if not script_content: continue
                    data = json.loads(script_content)

                    if data.get('@type') == 'ItemList' and 'itemListElement' in data:
                        for item_list_entry in data['itemListElement']:
                            if item_list_entry.get('@type') == 'ListItem' and \
                               item_list_entry.get('item', {}).get('@type') == 'Product':

                                product_info = item_list_entry['item']
                                product_url = product_info.get('@id') or product_info.get('url')

                                if product_url and product_url in processed_product_urls_page: continue
                                if product_url: processed_product_urls_page.add(product_url)

                                extracted_product = {
                                    'DEPARTAMENTO': department_name,
                                    'PRODUTO': product_info.get('name'),
                                    'MARCA': product_info.get('brand', {}).get('name'),
                                    'PRODUTO_URL': product_url,
                                    'IMAGEM_URL': product_info.get('image'),
                                    'MPN': product_info.get('mpn'),
                                    'SKU': product_info.get('sku'),
                                    'PRECO_ATUAL': None,
                                    'PRECO_ANTERIOR': None,
                                    'RAW_HTML': json.dumps(product_info, ensure_ascii=False)
                                }

                                offers_data = product_info.get('offers', {})
                                if offers_data.get('@type') == 'AggregateOffer' or isinstance(offers_data.get('offers'), list):
                                    current_price = offers_data.get('lowPrice')
                                    list_price = offers_data.get('highPrice')
                                    if current_price is None and isinstance(offers_data.get('offers'), list) and offers_data['offers']:
                                        current_price = offers_data['offers'][0].get('price')
                                        list_price = offers_data['offers'][0].get('priceSpecification', {}).get('listPrice') or list_price
                                    extracted_product['PRECO_ATUAL'] = clean_price(current_price)
                                    extracted_product['PRECO_ANTERIOR'] = clean_price(list_price)
                                elif offers_data.get('@type') == 'Offer':
                                     current_price = offers_data.get('price')
                                     list_price = offers_data.get('priceSpecification', {}).get('listPrice')
                                     extracted_product['PRECO_ATUAL'] = clean_price(current_price)
                                     extracted_product['PRECO_ANTERIOR'] = clean_price(list_price)

                                # Log detalhado do preço para verificar se está vindo vazio
                                logging.debug(f"Depto '{department_name}' P.{page_num} - Produto: '{extracted_product['PRODUTO'][:30]}...' - Preço Atual Extraído: {extracted_product['PRECO_ATUAL']} (Origem: {current_price})")

                                if extracted_product['PRODUTO'] or extracted_product['PRODUTO_URL']:
                                   extracted_data.append(extracted_product)
                                else:
                                    logging.warning(f"Depto '{department_name}' P.{page_num}: Item JSON-LD pulado (sem Nome/URL)")

                except json.JSONDecodeError: pass
                except Exception as json_err:
                    logging.warning(f"Depto '{department_name}' P.{page_num}: Erro processando tag JSON-LD #{idx+1}: {json_err}", exc_info=False)
            # --- FIM DA EXTRAÇÃO JSON-LD ---

            page_duration = time.time() - page_start_time
            logging.info(f"Depto '{department_name}' P.{page_num}: Tentativa {attempt} OK. {len(extracted_data)} itens extraídos em {page_duration:.2f}s.")

            if not extracted_data:
                 logging.info(f"Depto '{department_name}' P.{page_num}, Tentativa {attempt}: Nenhum produto extraído.")

            if driver: driver.quit(); driver = None
            return extracted_data # Retorna dados desta página

        # --- Blocos Except e Finally ---
        except (TimeoutException, WebDriverException, NoSuchElementException, StaleElementReferenceException) as e:
            logging.warning(f"Depto '{department_name}' P.{page_num}: Falha Tentativa {attempt}/{MAX_RETRIES}. Erro: {type(e).__name__} - {str(e).splitlines()[0] if str(e) else 'N/A'}")
            if attempt == MAX_RETRIES: logging.error(f"Depto '{department_name}' P.{page_num}: Falha final após {attempt} tentativas."); break
            if driver:
                try: driver.quit(); driver = None
                except Exception: pass
            logging.info(f"Depto '{department_name}' P.{page_num}: Aguardando {RETRY_DELAY_SECONDS}s...");
            time.sleep(RETRY_DELAY_SECONDS + random.uniform(0, 1))
        except Exception as e:
            logging.error(f"Depto '{department_name}' P.{page_num}: Erro crítico Tentativa {attempt}: {e}", exc_info=True); break
        finally:
             if driver:
                try: driver.quit()
                except Exception: pass
             driver = None

    # Falha final para a página
    page_duration = time.time() - page_start_time
    logging.error(f"Depto '{department_name}' P.{page_num}: Falhou processamento final ({page_duration:.2f}s).")
    return [] # Retorna lista vazia em caso de falha

# ==============================================================================
# EXECUÇÃO PRINCIPAL (POR PÁGINA DE DEPARTAMENTO)
# ==============================================================================
if __name__ == "__main__":
    overall_start_time = time.time()
    results_lock = threading.Lock()
    all_product_data = [] # Lista global

    if not DEPARTMENT_CONFIG:
        logging.error("Nenhuma configuração de departamento definida em DEPARTMENT_CONFIG. Saindo.")
        sys.exit(1)

    logging.info(f"--- Início do Script (Modo Paginação Depto + Scroll + JSON-LD) ---");
    logging.info(f"Departamentos a processar: {list(DEPARTMENT_CONFIG.keys())}")
    logging.info(f"Workers: {NUM_WORKERS}")
    logging.info(f"Tabela Destino: {DB_TABLE_NAME}")

    # --- Cria lista de todas as páginas a serem raspadas ---
    tasks_to_submit = []
    total_pages_calculated = 0
    for dept_name, config in DEPARTMENT_CONFIG.items():
        base_url = config.get("base_url")
        total_prods = config.get("total_products_to_scrape")
        page_size = config.get("page_size")

        if not base_url or not total_prods or not page_size or page_size <= 0: # Adicionado check page_size > 0
            logging.error(f"Configuração inválida ou incompleta para departamento '{dept_name}'. Pulando.")
            continue

        num_pages_for_dept = math.ceil(total_prods / page_size)
        total_pages_calculated += num_pages_for_dept
        logging.info(f"Depto '{dept_name}': Base URL='{base_url}', Prods={total_prods}, Size={page_size} => {num_pages_for_dept} páginas.")

        for page in range(1, num_pages_for_dept + 1):
            tasks_to_submit.append((dept_name, base_url, page))

    if not tasks_to_submit:
        logging.error("Nenhuma tarefa de scraping gerada. Verifique DEPARTMENT_CONFIG.")
        sys.exit(1)

    logging.info(f"--- Total de {len(tasks_to_submit)} páginas de departamento a serem processadas ---")

    # --- Execução Concorrente por Página de Departamento ---
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS, thread_name_prefix='MVDeptoPageScraper') as executor:
        future_to_task = {
            executor.submit(scrape_department_page, task[0], task[1], task[2]): task
            for task in tasks_to_submit
        }
        success_count = 0; fail_count = 0; processed_count = 0

        for future in concurrent.futures.as_completed(future_to_task):
            task_info = future_to_task[future]
            dept_name = task_info[0]; page_num = task_info[2]
            processed_count += 1
            try:
                result_data = future.result() # Retorna [] em caso de falha
                # A tarefa é considerada 'sucesso' se executou sem levantar exceção fatal,
                # mesmo que não tenha extraído dados (página vazia, etc.)
                success_count += 1 # Incrementa sucesso aqui
                if result_data:
                     with results_lock:
                         all_product_data.extend(result_data)
                     logging.debug(f"Depto '{dept_name}' P.{page_num}: OK, {len(result_data)} itens adicionados.")
                else:
                     logging.debug(f"Depto '{dept_name}' P.{page_num}: OK, nenhum item extraído.")

            except Exception as exc: # Captura exceções levantadas explicitamente ou erros inesperados
                logging.error(f"Depto '{dept_name}' P.{page_num}: Tarefa gerou exceção: {exc}", exc_info=True)
                fail_count += 1
                success_count -= 1 # Decrementa sucesso se houve exceção na coleta do resultado

            if processed_count % 10 == 0 or processed_count == len(tasks_to_submit) or fail_count > 0:
                logging.info(f"Progresso: {processed_count}/{len(tasks_to_submit)} páginas concluídas (Sucesso Aparente*: {success_count}, Falha: {fail_count}).")
                # *Sucesso Aparente: significa que a função terminou, mas pode não ter achado dados.

    scraping_duration = time.time() - overall_start_time
    logging.info(f"--- Fase Scraping Concluída ---")
    logging.info(f"Tempo Scraping: {scraping_duration:.2f}s.")
    logging.info(f"Páginas Processadas (tentativa concluída): {success_count}")
    logging.info(f"Páginas com Falha Final ou Exceção: {fail_count}")
    logging.info(f"Total de Itens Coletados: {len(all_product_data)}")

    # --- Processamento DataFrame e Banco de Dados (Nenhuma alteração necessária aqui) ---
    if not all_product_data:
        logging.warning(f"Nenhum produto coletado. Verifique log '{LOG_FILE}'. Saindo.")
        sys.exit(0)

    logging.info("Processando DataFrame...")
    # ... (Cole o bloco "Processamento DataFrame" da resposta anterior aqui) ...
    df_process_start_time = time.time(); df = None
    try:
        df = pd.DataFrame(all_product_data)
        logging.info(f"DataFrame inicial: {len(df)} linhas.")
        initial_rows = len(df)
        subset_dedup = ['PRODUTO_URL']
        if 'PRODUTO_URL' in df.columns:
             df.drop_duplicates(subset=subset_dedup, keep='first', inplace=True)
             rows_after_drop = len(df)
             if initial_rows != rows_after_drop: logging.info(f"Removidas {initial_rows - rows_after_drop} duplicatas baseadas em {subset_dedup}.")
        else: logging.warning("Coluna 'PRODUTO_URL' ausente para deduplicação.")
        expected_str_cols = ['DEPARTAMENTO', 'PRODUTO', 'MARCA', 'PRODUTO_URL', 'IMAGEM_URL', 'MPN', 'SKU', 'RAW_HTML']
        expected_num_cols = ['PRECO_ATUAL', 'PRECO_ANTERIOR']
        for col in expected_str_cols:
             if col not in df.columns: df[col] = ''
             df[col] = df[col].astype(str).fillna('')
        for col in expected_num_cols:
             if col not in df.columns: df[col] = None
             df[col] = pd.to_numeric(df[col], errors='coerce')
        logging.info(f"Colunas DataFrame final: {df.columns.tolist()}")
        pd.set_option('display.max_rows', 10); pd.set_option('display.max_colwidth', 50)
        logging.info(f"\n--- DataFrame Processado (Primeiras Linhas) ---\n{df.head().to_string()}")
        pd.reset_option('display.max_rows'); pd.reset_option('display.max_colwidth')
        logging.info(f"\nTipos (Pandas):\n{df.dtypes}\nNão nulos (Pandas):\n{df.notnull().sum()}")
        logging.info(f"Processamento DataFrame: {time.time() - df_process_start_time:.2f}s.")
    except Exception as e:
        logging.error(f"Erro fatal processando DataFrame: {e}", exc_info=True)
        sys.exit("Erro DataFrame.")

    logging.info("Iniciando operações DB...")
    # ... (Cole o bloco "Operações de Banco de Dados" da resposta anterior aqui) ...
    db_start_time = time.time(); conn_save = None; cursor_save = None
    outer_exception = None
    conn_str_save = None
    try:
        with open(CREDENTIAL_PATH, 'r') as f: cred = json.load(f)
        server=cred['server']; database=cred['database']; driver_sql=cred['driver']
        conn_str_save = f'DRIVER={driver_sql};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    except Exception as cred_err:
         logging.error(f"Erro lendo credenciais para salvar: {cred_err}", exc_info=True)
         sys.exit("Erro Credenciais DB (Save).")
    if not conn_str_save: sys.exit("Falha ao criar string de conexão para salvar.")
    try:
        logging.info(f"Conectando DB para salvar...");
        conn_save = pyodbc.connect(conn_str_save, autocommit=False, timeout=60);
        cursor_save = conn_save.cursor();
        logging.info("Conexão DB (salvar) OK.")
        df_sql_map = {
            'DEPARTAMENTO': 'DEPARTAMENTO', 'PRODUTO': 'PRODUTO', 'MARCA': 'MARCA', 'MPN': 'MPN',
            'PRECO_ATUAL': 'PRECO_ATUAL', 'PRECO_ANTERIOR': 'PRECO_ANTERIOR',
            'PRODUTO_URL': 'PRODUTO_URL', 'IMAGEM_URL': 'IMAGEM_URL', 'RAW_HTML': 'RAW_HTML'
        }
        sql_columns_order = ['DEPARTAMENTO', 'PRODUTO', 'MARCA', 'MPN', 'PRECO_ATUAL', 'PRECO_ANTERIOR', 'PRODUTO_URL', 'IMAGEM_URL', 'RAW_HTML']
        try:
            logging.info(f"Recriando tabela: {DB_TABLE_NAME}");
            cursor_save.execute(f"IF OBJECT_ID(?, 'U') IS NOT NULL DROP TABLE {DB_TABLE_NAME};", (DB_TABLE_NAME,));
            conn_save.commit()
            logging.info(f"DROP TABLE {DB_TABLE_NAME} OK.")
            logging.info(f"Criando tabela: {DB_TABLE_NAME}");
            create_table_sql = f"""
            CREATE TABLE {DB_TABLE_NAME} (
                ID INT IDENTITY(1,1) PRIMARY KEY, DEPARTAMENTO NVARCHAR(100) NULL, PRODUTO NVARCHAR(500) COLLATE Latin1_General_CI_AI NULL,
                MARCA NVARCHAR(200) COLLATE Latin1_General_CI_AI NULL, MPN NVARCHAR(100) NULL, PRECO_ATUAL DECIMAL(10,2) NULL,
                PRECO_ANTERIOR DECIMAL(10,2) NULL, PRODUTO_URL NVARCHAR(1000) NULL, IMAGEM_URL NVARCHAR(1000) NULL,
                RAW_HTML NVARCHAR(MAX) NULL, DATA_HORA_SCRAPE DATETIME DEFAULT GETDATE()
            );"""
            cursor_save.execute(create_table_sql); conn_save.commit(); logging.info(f"CREATE TABLE {DB_TABLE_NAME} OK.")
            df_insert = df.where(pd.notnull(df), None)
            if not df_insert.empty:
                formatted_data_to_insert = []
                decimal_context = decimal.Context(prec=12); decimal.setcontext(decimal_context)
                logging.info("Formatando dados para inserção DB...")
                problematic_rows_count = 0
                for row_tuple in df_insert.itertuples(index=False):
                    try:
                        row_dict = row_tuple._asdict(); sql_ready_row = []
                        for sql_col_name in sql_columns_order:
                            df_col_name = df_sql_map.get(sql_col_name); value = row_dict.get(df_col_name)
                            if sql_col_name in ['PRECO_ATUAL', 'PRECO_ANTERIOR']:
                                if value is not None:
                                    try: value = decimal.Decimal(str(value)).quantize(decimal.Decimal("0.01"), rounding=decimal.ROUND_HALF_UP); value = None if value.is_nan() else value
                                    except: value = None
                            elif sql_col_name in ['DEPARTAMENTO', 'PRODUTO', 'MARCA', 'MPN', 'PRODUTO_URL', 'IMAGEM_URL', 'RAW_HTML']:
                                if value is not None:
                                     value = str(value)
                                     limit = {'DEPARTAMENTO': 100,'PRODUTO': 500, 'MARCA': 200, 'MPN': 100, 'PRODUTO_URL': 1000, 'IMAGEM_URL': 1000}.get(sql_col_name)
                                     if limit and len(value) > limit: value = value[:limit]
                            sql_ready_row.append(value)
                        formatted_data_to_insert.append(tuple(sql_ready_row))
                    except Exception as fmt_err:
                        problematic_rows_count += 1; logging.error(f"Erro formatando linha p/ SQL: {fmt_err}. Linha: {str(row_tuple)[:200]}...", exc_info=False)
                if problematic_rows_count > 0: logging.warning(f"{problematic_rows_count} erros durante formatação.")
                if formatted_data_to_insert:
                     logging.info(f"Preparando para inserir {len(formatted_data_to_insert)} linhas...");
                     placeholders = ', '.join('?' * len(sql_columns_order))
                     sql_insert = f"INSERT INTO {DB_TABLE_NAME} ({', '.join(sql_columns_order)}) VALUES ({placeholders})"
                     try:
                         t_insert_start = time.time()
                         cursor_save.executemany(sql_insert, formatted_data_to_insert)
                         rows_affected = cursor_save.rowcount; t_insert_end = time.time()
                         logging.info(f"Executemany concluído ({rows_affected} linhas) em {t_insert_end - t_insert_start:.2f}s.")
                         conn_save.commit(); logging.info("COMMIT BEM-SUCEDIDO.")
                     except (pyodbc.Error, Exception) as insert_err:
                         logging.warning(f"Executemany falhou ({type(insert_err).__name__}). Tentando linha por linha...")
                         conn_save.rollback(); inserted_count_single = 0
                         for i, row_data in enumerate(formatted_data_to_insert):
                             try: cursor_save.execute(sql_insert, row_data); inserted_count_single += 1
                             except Exception as ex_single:
                                 logging.error(f"Erro inserir linha {i+1}: {ex_single}", exc_info=False); logging.error(f"Dados problemáticos: {tuple(str(d)[:70]+'...' if isinstance(d,str) and len(str(d))>70 else d for d in row_data)}")
                                 conn_save.rollback(); outer_exception = ex_single; raise outer_exception
                         if inserted_count_single > 0: logging.info(f"Inseridas {inserted_count_single} linhas individualmente."); conn_save.commit(); logging.info("COMMIT BEM-SUCEDIDO (individual).")
                else: logging.warning("Lista formatada para inserção vazia.")
            else: logging.warning("DataFrame vazio, nada a inserir.")
        except (pyodbc.Error, Exception) as db_trans_err:
            outer_exception = db_trans_err; logging.error(f"Erro transação DB: {db_trans_err}", exc_info=True)
            try:
                 if conn_save: conn_save.rollback(); logging.info("Rollback DB realizado.")
            except Exception as rb_err: logging.error(f"Falha CRÍTICA no rollback: {rb_err}")
    except (pyodbc.Error, Exception) as db_conn_err:
         outer_exception = db_conn_err; logging.error(f"Erro conexão/config DB: {db_conn_err}", exc_info=True)
    finally:
        if cursor_save: cursor_save.close()
        if conn_save: conn_save.close(); logging.info("Conexão DB (salvar) fechada.")
        db_duration = time.time() - db_start_time; overall_duration = time.time() - overall_start_time
        logging.info(f"Tempo da fase de DB: {db_duration:.2f}s.")
        logging.info(f"--- Script Finalizado ---"); logging.info(f"Tempo total: {overall_duration:.2f} segundos.")
        if outer_exception: logging.error(f"Script finalizado com erro: {type(outer_exception).__name__}")