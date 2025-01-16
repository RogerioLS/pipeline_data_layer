from src.utils import S3Utils
from src.logger import Logger
from src.bronze import BronzeLayer
from src.silver import SilverLayer
from src.gold import GoldLayer
from src import os
from src import requests
from src import datetime

current_date = datetime.now()
year = current_date.strftime("%Y")
month = current_date.strftime("%m")


def download_from_github(url: str, save_path: str) -> None:
    """
    Faz o download de um arquivo do GitHub e salva localmente.

    :param url: URL do arquivo no GitHub.
    :param save_path: Caminho local onde o arquivo será salvo.
    """
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(save_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
    else:
        raise Exception(f"Erro ao baixar o arquivo do GitHub. Status code: {response.status_code}")


if __name__ == "__main__":
    # Configurar o logger
    logger = Logger(log_dir="logs", log_file_prefix="housing_pipeline")

    try:
        # Substitua pelo nome do seu bucket
        BUCKET_NAME = "preco-de-casas-projeto"
        BRONZE_FOLDER = "bronze/"

        LOCAL_BRONZE_FOLDER = "data/bronze/"
        LOCAL_SILVER_FOLDER = "data/silver/"
        LOCAL_GOLD_FOLDER = "data/gold/"

        FOLDER = "data/"

        GITHUB_URL = "https://raw.githubusercontent.com/RogerioLS/pipeline_data_layer/refs/heads/main/data/train.csv"
        LOCAL_TEMP_FILE = "preco_de_casas.csv"

        logger.info("Iniciando a aplicação.")
        logger.info(f"Baixando arquivo do GitHub: {GITHUB_URL}")
        download_from_github(GITHUB_URL, LOCAL_TEMP_FILE)
        logger.info(f"Arquivo baixado e salvo localmente como {LOCAL_TEMP_FILE}")

        # Configurar utilitário S3, bronze layer
        s3_utils = S3Utils(bucket_name=BUCKET_NAME, logger=logger)
        bronze_layer = BronzeLayer(s3_utils=s3_utils, local_bronze_folder=LOCAL_BRONZE_FOLDER, logger=logger)
        silver_layer = SilverLayer(bronze_path=FOLDER, silver_path=LOCAL_SILVER_FOLDER, logger=logger)
        gold_layer = GoldLayer(silver_path=LOCAL_SILVER_FOLDER, gold_path=LOCAL_GOLD_FOLDER, logger=logger)

        # Fazer o upload do arquivo para o S3 na pasta raw/
        logger.info(f"Subindo arquivo {LOCAL_TEMP_FILE} para S3 em 'bronze/{LOCAL_TEMP_FILE}'")
        s3_utils.upload_file(LOCAL_TEMP_FILE, f"bronze/{LOCAL_TEMP_FILE}")
        logger.info(f"Arquivo {LOCAL_TEMP_FILE} enviado com sucesso para o bucket S3.")

        # Listar e baixar arquivos da pasta raw/
        raw_files = s3_utils.list_file(BRONZE_FOLDER)
        raw_files = raw_files[1:]
        logger.info(f"Arquivos na pasta bronze/: {raw_files}")

        for file_key in raw_files:
            bronze_layer.download_and_read_raw_data(file_key)
            data = silver_layer.load_data(file_key)
            cleaned_data = silver_layer.clean_data(data)
            silver_layer.save_to_curated(cleaned_data, LOCAL_TEMP_FILE)
            s3_utils.upload_file(LOCAL_TEMP_FILE, f"silver/{LOCAL_TEMP_FILE}")
            logger.info(f"Arquivo {LOCAL_TEMP_FILE} enviado com sucesso para o bucket S3 camada silver.")

        # Carregar os dados limpos da camada Prata
        curated_data = gold_layer.load_data()
        aggregated_data = gold_layer.aggregate_data(curated_data)
        gold_layer.save_to_gold(aggregated_data, LOCAL_TEMP_FILE)
        s3_utils.upload_file(LOCAL_TEMP_FILE, f"gold/{LOCAL_TEMP_FILE}")
        logger.info(f"Arquivo {LOCAL_TEMP_FILE} enviado com sucesso para o bucket S3 camada gold.")

        # Subir arquivo de log no S3
        log_path = logger.get_log_path()
        logger.info(f"Subindo o arquivo de log {log_path} para o bucket S3.")
        s3_utils.upload_file(log_path, f"logs/{year}/{month}/{os.path.basename(log_path)}")

        logger.info("Aplicação finalizada com sucesso.")

    except Exception as e:
        logger.error(f"Erro durante a execução: {str(e)}")
