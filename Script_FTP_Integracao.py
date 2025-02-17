import os # Biblioteca para manipulação de arquivos e variáveis de ambiente
import psycopg # Biblioteca para conexão com o banco de dados
from ftplib import FTP # Biblioteca para conexão com FTP
import logging # Biblioteca para logs
from dotenv import load_dotenv # Biblioteca para carregar variáveis de ambiente
import re # Biblioteca para expressões regulares
from datetime import datetime # Biblioteca para manipulação de datas

# Carregar variáveis de ambiente
load_dotenv()

# Configuração de log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_db():
    """Conecta ao banco de dados usando variáveis de ambiente."""
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
    """Limpa o nome do arquivo, removendo caracteres inválidos."""
    return re.sub(r'[\\/*?:"<>|]', "", filename)

def formatar_cpf(cpf):
    """Formata o CPF no padrão 000.000.000-00."""
    cpf = re.sub(r'\D', '', cpf)  # Remove todos os caracteres não numéricos
    if len(cpf) == 11:
        return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"
    return cpf  # Retorna o CPF original se não tiver 11 dígitos

def download_ftp_file():
    """Baixa o arquivo mais recente do FTP."""
    ftp = None
    try:
        ftp = FTP()
        ftp.connect(os.getenv("FTP_HOST"), int(os.getenv("FTP_PORT", 21)), timeout=60)
        ftp.login(os.getenv("FTP_USER"), os.getenv("FTP_PASSWORD"))
        ftp.cwd("/")

        files = ftp.nlst()
        if not files:
            logging.warning("Nenhum arquivo encontrado no FTP.")
            return None

        # Encontra o arquivo mais recente
        latest_file = max(files, key=lambda x: datetime.strptime(ftp.sendcmd(f"MDTM {x}").split()[1], "%Y%m%d%H%M%S"))
        local_filename = sanitize_filename(os.path.basename(latest_file))

        with open(local_filename, 'wb') as local_file:
            ftp.retrbinary(f'RETR {latest_file}', local_file.write)

        logging.info(f"Arquivo {latest_file} baixado com sucesso.")
        return local_filename

    except Exception as e:
        logging.error(f"Erro ao baixar arquivo do FTP: {e}")
        return None
    finally:
        if ftp:
            ftp.quit()

def process_and_insert_data(file_name, conn):
    logging.info(f"Iniciando processamento do arquivo: {file_name}")
    try:
        with open(file_name, 'r') as file:
            for line_number, line in enumerate(file, start=1):
                data = line.strip().split(';')
                
                if len(data) < 16:  # Ajuste para 16 colunas
                    logging.warning(f"Erro no layout da linha {line_number}: esperado 16 colunas, recebeu {len(data)} - {line}")
                    continue

                try:
                    vei_id = int(data[0])  # ID_FROTA_HPS
                    mot_id = int(data[6]) if data[6].isdigit() else None  # CODIGO_RDC_MOTORISTA
                except ValueError:
                    logging.warning(f"ID_FROTA_HPS ou CODIGO_RDC_MOTORISTA inválido na linha {line_number}: {data[0]}, {data[6]}") 
                    continue

                vei_plc = data[1]  # PLACA_FROTA
                mot_nom = data[4].strip() if data[4] and data[4].strip() != "EM DEFINICAO" else "EM DEFINICAO"  # NOME_MOTORISTA
                placa_car = data[5]  # PLACA_DESCRIÇÃO_CARRETA
                mot_tel = data[7] if data[7] else None  # TELEFONE_MOTORISTA
                mot_cnh = data[9] if data[9] else None  # CNH_MOTORISTA
                mot_cpf = formatar_cpf(data[10]) if data[10] else None  # CPF_MOTORISTA - CPF formatado
                mot_rua = data[12]  # MOT_RUA (endereço)
                mot_num = data[13]  # NUMERO_MOTORISTA
                mot_bai = data[14] if len(data) > 14 else None  # BAIRRO_MOTORISTA
                mot_cid = data[15][:25]  # MUNICIPIO_MOTORISTA
                mot_uf = data[16]  # UF_MOTORISTA

                if mot_id == 999999:  # Corrigido: verificando o ID de motorista
                    mot_id = None

                # Adiciona cli_id 269 para todos os motoristas
                cli_id = 269

                # Log dos dados a serem inseridos/atualizados
                logging.info(f"Processando dados para o motorista ID: {mot_id if mot_id else 'Novo'}")
                logging.debug(f"Dados a serem inseridos/atualizados: ")
                logging.debug(f"vei_id: {vei_id}, vei_plc: {vei_plc}, mot_nom: {mot_nom}, mot_tel: {mot_tel}, mot_cnh: {mot_cnh}, mot_cpf: {mot_cpf}")
                logging.debug(f"mot_rua: {mot_rua}, mot_num: {mot_num}, mot_bai: {mot_bai}, mot_cid: {mot_cid}, mot_uf: {mot_uf}")
                logging.debug(f"placa_car: {placa_car}")

                # Inserção ou atualização dos dados no banco
                with conn.cursor() as cursor:
                    try:
                        # Inicia a transação para esse motorista
                        cursor.execute("BEGIN;")

                        # Verifica se o motorista existe com base no mot_nom ou mot_cpf (um ou outro)
                        search_query = "SELECT mot_id FROM motorista WHERE mot_nom = %s OR mot_cpf = %s"
                        cursor.execute(search_query, (mot_nom, mot_cpf))
                        result = cursor.fetchone()

                        if result:  # Se o motorista já existir, usa o mot_id para atualizar
                            mot_id = result[0]

                            # Atualiza todos os dados do motorista
                            cursor.execute(
                                """
                                UPDATE motorista
                                SET cli_id = %s, mot_nom = %s, mot_tel = %s, mot_cnh = %s, mot_cpf = %s, mot_rua = %s, 
                                    mot_num = %s, mot_bai = %s, mot_cid = %s, mot_uf = %s
                                WHERE mot_id = %s
                                """,
                                (cli_id, mot_nom, mot_tel, mot_cnh, mot_cpf, mot_rua, mot_num, mot_bai, mot_cid, mot_uf, mot_id)
                            )
                            logging.info(f"Dados do motorista atualizado para o ID {mot_id}")
                        else:  # Se o motorista não existir, insere como novo
                            cursor.execute(
                                """
                                INSERT INTO motorista (cli_id, mot_nom, mot_tel, mot_cnh, mot_cpf, mot_rua, mot_num, mot_bai, mot_cid, mot_uf, mot_mat)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                RETURNING mot_id
                                """,
                                (cli_id, mot_nom, mot_tel, mot_cnh, mot_cpf, mot_rua, mot_num, mot_bai, mot_cid, mot_uf, None)  # mot_mat será None
                            )
                            mot_id = cursor.fetchone()[0]  # Obtém o ID gerado do novo motorista
                            logging.info(f"Motorista inserido com sucesso! ID gerado: {mot_id}")

                        # Atualiza a tabela grid_ext (forçando a atualização do placa_car com a descrição da carreta)
                        logging.debug(f"Atualizando a tabela grid_ext para o veículo ID: {vei_id} e placa_car (descrição carreta): {placa_car}")
                        cursor.execute(
                            """
                            UPDATE grid_ext
                            SET placa_car = %s, mot_nom = %s, mot_id = %s
                            WHERE vei_id = %s
                            """,
                            (placa_car, mot_nom, mot_id, vei_id)
                        )
                        if cursor.rowcount == 0:
                            logging.warning(f"Nenhuma atualização realizada na tabela grid_ext para a placa {placa_car}")
                        else:
                            logging.info(f"Alteração realizada na tabela grid_ext para a placa {placa_car}")

                        # Atualiza a tabela cad_veiculo
                        cursor.execute(
                            """
                            UPDATE cad_veiculo
                            SET vei_plc = %s
                            WHERE vei_id = %s
                            """,
                            (vei_plc, vei_id)
                        )
                        logging.info(f"Alteração realizada na tabela cad_veiculo para o veículo ID {vei_id}")

                        # Atualiza a tabela last_datastore
                        cursor.execute(
                            """
                            UPDATE last_datastore
                            SET vei_id = %s
                            WHERE vei_id = %s
                            """,
                            (vei_id, vei_id)
                        )
                        logging.info(f"Alteração realizada na tabela last_datastore para o veículo ID {vei_id}")

                        # Commit da transação
                        cursor.execute("COMMIT;")

                    except Exception as e:
                        cursor.execute("ROLLBACK;")
                        logging.error(f"Erro ao atualizar os dados do motorista com ID {mot_id}. Dados tentados: Motivação: {e}")
                logging.info(f"Processamento da linha {line_number} finalizado.")

    except Exception as e:
        logging.error(f"Erro ao processar o arquivo {file_name}: {e}")

def main():
    # Conectar ao banco de dados
    conn = connect_db()
    if conn:
        # Baixar arquivo do FTP
        file_name = download_ftp_file()
        if file_name:
            # Processar e inserir os dados no banco de dados
            process_and_insert_data(file_name, conn)

        # Fechar a conexão com o banco de dados
        if conn is not None:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

        # Remover o arquivo processado
        if file_name and os.path.exists(file_name):
            os.remove(file_name)
            logging.info(f"Arquivo {file_name} removido após processamento.")
        
        # Pausar para interação do usuário antes de fechar
        #input("Pressione qualquer tecla para fechar...")

if __name__ == "__main__":
    main()