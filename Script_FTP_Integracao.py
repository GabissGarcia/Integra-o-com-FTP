import os
import psycopg2
from ftplib import FTP
import logging
from dotenv import load_dotenv
import re
from datetime import datetime
import tempfile

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_db():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=int(os.getenv("DB_PORT", 5432))
        )
        logging.info("Conexão com o banco de dados estabelecida.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

def sanitize_filename(filename):
    return re.sub(r'[\\/*?:"<>|]', "", filename)

def formatar_cpf(cpf):
    if not cpf or not cpf.strip():
        return None
    
    cpf = re.sub(r'\D', '', cpf)
    if len(cpf) == 11:
        return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"
    return None

def get_download_directory():
    download_dir = os.path.join(os.path.expanduser("~"), "Downloads", "ftp_hps")
    
    try:
        os.makedirs(download_dir, exist_ok=True)
        
        if os.access(download_dir, os.W_OK):
            logging.info(f"Diretório de download: {download_dir}")
            return download_dir
    except Exception as e:
        logging.warning(f"Não foi possível usar Downloads: {e}")
    
    try:
        download_dir = os.path.join(tempfile.gettempdir(), "ftp_hps")
        os.makedirs(download_dir, exist_ok=True)
        
        if os.access(download_dir, os.W_OK):
            logging.info(f"Usando diretório temporário: {download_dir}")
            return download_dir
    except Exception as e:
        logging.warning(f"Não foi possível usar diretório temporário: {e}")
    
    download_dir = os.path.dirname(os.path.abspath(__file__))
    logging.warning(f"Usando diretório do script: {download_dir}")
    return download_dir

def download_ftp_file():
    ftp = None
    local_filename = None
    
    try:
        ftp = FTP()
        ftp.connect(os.getenv("FTP_HOST"), int(os.getenv("FTP_PORT", 21)), timeout=60)
        ftp.login(os.getenv("FTP_USER"), os.getenv("FTP_PASSWORD"))
        ftp.cwd("/")

        files = ftp.nlst()
        if not files:
            logging.warning("Nenhum arquivo encontrado no FTP.")
            return None

        try:
            latest_file = max(files, key=lambda x: datetime.strptime(ftp.sendcmd(f"MDTM {x}").split()[1], "%Y%m%d%H%M%S"))
            logging.info(f"Arquivo mais recente encontrado (via MDTM): {latest_file}")
        except:
            latest_file = files[-1]
            logging.warning(f"MDTM não suportado, usando último arquivo da lista: {latest_file}")
        
        download_dir = get_download_directory()
        local_filename = os.path.join(download_dir, sanitize_filename(os.path.basename(latest_file)))
        
        if os.path.exists(local_filename):
            try:
                os.remove(local_filename)
                logging.info(f"Arquivo existente removido: {local_filename}")
            except PermissionError:
                logging.error(f"Sem permissão para remover arquivo existente: {local_filename}")
                local_filename = os.path.join(download_dir, 
                    f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{sanitize_filename(os.path.basename(latest_file))}")
                logging.info(f"Usando nome alternativo: {local_filename}")

        logging.info(f"Iniciando download para: {local_filename}")
        with open(local_filename, 'wb') as local_file:
            ftp.retrbinary(f'RETR {latest_file}', local_file.write)

        logging.info(f"Arquivo {latest_file} baixado com sucesso para {local_filename}")
        return local_filename

    except Exception as e:
        logging.error(f"Erro ao baixar arquivo do FTP: {e}")
        
        if local_filename and os.path.exists(local_filename):
            try:
                os.remove(local_filename)
                logging.info(f"Arquivo parcial removido: {local_filename}")
            except:
                pass
                
        return None
    finally:
        if ftp:
            try:
                ftp.quit()
            except:
                pass

def find_motorista(cursor, mot_nom, mot_cpf):
    if mot_cpf:
        cursor.execute("SELECT mot_id, mot_nom FROM motorista WHERE mot_cpf = %s", (mot_cpf,))
        result = cursor.fetchone()
        if result:
            logging.info(f" Motorista encontrado por CPF: ID {result[0]}, Nome: {result[1]}")
            return result[0]
    
    mot_nom_clean = mot_nom.strip().upper()
    
    cursor.execute("SELECT mot_id, mot_nom FROM motorista WHERE UPPER(mot_nom) = %s", (mot_nom_clean,))
    result = cursor.fetchone()
    if result:
        logging.info(f" Motorista encontrado por nome exato: ID {result[0]}, Nome: {result[1]}")
        return result[0]
    
    cursor.execute("""
        SELECT mot_id, mot_nom 
        FROM motorista 
        WHERE UPPER(mot_nom) LIKE %s OR UPPER(mot_nom) LIKE %s
    """, (f"{mot_nom_clean}%", f"%{mot_nom_clean}%"))
    
    results = cursor.fetchall()
    if len(results) == 1:
        logging.info(f" Motorista encontrado por similaridade: ID {results[0][0]}, Nome: {results[0][1]}")
        return results[0][0]
    elif len(results) > 1:
        logging.warning(f" Múltiplos motoristas encontrados para nome similar '{mot_nom}': {[r[1] for r in results]}")
    
    return None

def process_and_insert_data(file_name, conn):
    if not file_name or not os.path.exists(file_name):
        logging.error(f"Arquivo não encontrado: {file_name}")
        return

    logging.info(f"Iniciando processamento do arquivo: {file_name}")
    
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    file_content = None
    used_encoding = None
    
    for enc in encodings:
        try:
            with open(file_name, 'r', encoding=enc) as file:
                file_content = file.readlines()
            used_encoding = enc
            logging.info(f" Arquivo lido com encoding: {enc}")
            break
        except UnicodeDecodeError:
            logging.debug(f"Falha ao ler com encoding {enc}, tentando próximo...")
            continue
    
    if not file_content:
        logging.error(f"✗ Não foi possível ler o arquivo com nenhum encoding suportado")
        return
    
    try:
        for line_number, line in enumerate(file_content, start=1):
            data = line.strip().split(';')
            
            if len(data) < 17:
                logging.warning(f"Erro no layout da linha {line_number}: esperado 17 colunas, recebeu {len(data)}")
                continue

            try:
                vei_id = int(data[0])
                mot_id = int(data[6]) if data[6].isdigit() else None
            except ValueError:
                logging.warning(f"ID_FROTA_HPS ou CODIGO_RDC_MOTORISTA inválido na linha {line_number}: {data[0]}, {data[6]}") 
                continue

            vei_plc = data[1].strip()
            mot_nom = data[4].strip() if data[4] and data[4].strip() != "EM DEFINICAO" else "EM DEFINICAO"
            placa_car = data[5].strip()
            mot_tel = data[7].strip() if data[7] and data[7].strip() else None
            mot_cnh = data[9].strip() if data[9] and data[9].strip() else None
            mot_cpf = formatar_cpf(data[10]) if data[10] else None
            mot_rua = data[12].strip() if data[12] else None
            mot_num = data[13].strip() if data[13] else None
            mot_bai = data[14].strip() if len(data) > 14 and data[14] else None
            mot_cid = data[15][:25].strip() if len(data) > 15 and data[15] else None
            mot_uf = data[16].strip() if len(data) > 16 and data[16] else None

            if vei_id == 51773:
                logging.warning(f"[MONITOR] Veículo 51773 encontrado. Placa recebida do TXT: {vei_plc}")

            if mot_id == 999999:
                mot_id = None

            cli_id = 269

            logging.info(f"Processando: Motorista '{mot_nom}', CPF: {mot_cpf}, Veículo: {vei_id}")

            with conn.cursor() as cursor:
                try:
                    cursor.execute("BEGIN;")

                    existing_mot_id = find_motorista(cursor, mot_nom, mot_cpf)
                    
                    if existing_mot_id:
                        mot_id = existing_mot_id
                        cursor.execute(
                            """
                            UPDATE motorista
                            SET cli_id = %s, mot_nom = %s, mot_tel = %s, mot_cnh = %s, mot_cpf = %s, 
                                mot_rua = %s, mot_num = %s, mot_bai = %s, mot_cid = %s, mot_uf = %s
                            WHERE mot_id = %s
                            """,
                            (cli_id, mot_nom, mot_tel, mot_cnh, mot_cpf, mot_rua, mot_num, mot_bai, mot_cid, mot_uf, mot_id)
                        )
                        logging.info(f" Motorista atualizado: ID {mot_id}, Nome: {mot_nom}")
                    else:
                        cursor.execute(
                            """
                            INSERT INTO motorista (cli_id, mot_nom, mot_tel, mot_cnh, mot_cpf, mot_rua, mot_num, mot_bai, mot_cid, mot_uf, mot_mat)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING mot_id
                            """,
                            (cli_id, mot_nom, mot_tel, mot_cnh, mot_cpf, mot_rua, mot_num, mot_bai, mot_cid, mot_uf, None)
                        )
                        mot_id = cursor.fetchone()[0]
                        logging.info(f" NOVO motorista cadastrado: ID {mot_id}, Nome: {mot_nom}")

                    cursor.execute(
                        """
                        UPDATE grid_ext
                        SET placa_car = %s, mot_nom = %s, mot_id = %s
                        WHERE vei_id = %s
                        """,
                        (placa_car, mot_nom, mot_id, vei_id)
                    )
                    if cursor.rowcount == 0:
                        logging.warning(f" Nenhuma atualização na grid_ext para veículo {vei_id}")
                    else:
                        logging.info(f" Grid_ext atualizado para veículo {vei_id}")

                    cursor.execute(
                        """
                        UPDATE cad_veiculo
                        SET vei_plc = %s
                        WHERE vei_id = %s
                        """,
                        (vei_plc, vei_id)
                    )
                    logging.info(f" Cad_veiculo atualizado: Veículo {vei_id}, Placa: {vei_plc}")

                    cursor.execute(
                        """
                        UPDATE last_datastore
                        SET vei_id = %s
                        WHERE vei_id = %s
                        """,
                        (vei_id, vei_id)
                    )
                    logging.info(f" Last_datastore atualizado para veículo {vei_id}")

                    cursor.execute("COMMIT;")
                    logging.info(f" Transação commitada para linha {line_number}")

                except Exception as e:
                    cursor.execute("ROLLBACK;")
                    logging.error(f"✗ Erro na transação linha {line_number}: {e}")

            logging.debug(f" Linha {line_number} processada.")

    except Exception as e:
        logging.error(f"Erro ao processar o arquivo {file_name}: {e}")

def main():
    logging.info("=" * 80)
    logging.info("Iniciando processo de integração FTP -> Banco de Dados")
    logging.info("=" * 80)
    
    conn = connect_db()
    if not conn:
        logging.error("Não foi possível conectar ao banco de dados. Abortando.")
        input("Pressione ENTER para fechar...")
        return

    file_name = download_ftp_file()
    if not file_name:
        logging.error("Não foi possível baixar o arquivo do FTP. Abortando.")
        conn.close()
        input("Pressione ENTER para fechar...")
        return

    process_and_insert_data(file_name, conn)

    conn.close()
    logging.info("Conexão com o banco de dados fechada.")

    try:
        if os.path.exists(file_name):
            os.remove(file_name)
            logging.info(f"Arquivo {file_name} removido após processamento.")
    except Exception as e:
        logging.warning(f"Não foi possível remover o arquivo {file_name}: {e}")
    
    logging.info("=" * 80)
    logging.info("Processo concluído com sucesso!")
    logging.info("=" * 80)
    
    input("Pressione ENTER para fechar...")

if __name__ == "__main__":
    main()
