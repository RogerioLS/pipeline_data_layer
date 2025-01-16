# src/bronze.py
from src import pd
from src import os
from src.logger import Logger
from src.utils import S3Utils


class BronzeLayer:
    def __init__(self, s3_utils: S3Utils,
                 local_bronze_folder: str, logger: Logger) -> None:
        """
        Inicializa a camada Bronze.

        :param s3_utils: Instância do S3Utils para interação com o S3.
        :param processed_path: Caminho local para armazenar os dados validados.
        :param logger: Instância do logger para registrar eventos.
        """
        self.s3_utils = s3_utils
        self.logger = logger
        self.local_raw_folder = local_bronze_folder

    def download_and_read_raw_data(self, s3_key: str) -> pd.DataFrame:
        """
        Faz o download do arquivo bruto do S3 e o lê em um DataFrame.

        :param s3_key: Chave do arquivo no S3 (caminho no bucket).
        :return: DataFrame contendo os dados do arquivo.
        """
        local_file_path = os.path.join(self.local_raw_folder, os.path.basename(s3_key))
        self.logger.info(f"Baixando arquivo do S3: {s3_key} para {local_file_path}")

        try:
            self.s3_utils.download_file(s3_key, local_file_path)
            self.logger.info(f"Arquivo {s3_key} baixado com sucesso.")
            self.logger.info(f"Arquivo {s3_key} lido com sucesso.")
        except Exception as e:
            self.logger.error(f"Erro ao baixar ou ler o arquivo {s3_key}: {e}")
            raise
