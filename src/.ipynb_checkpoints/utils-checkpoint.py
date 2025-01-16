# src/utils.py
from src import boto3
from src import os, List
from src import NoCredentialsError, PartialCredentialsError
from src.logger import Logger


class S3Utils:
    def __init__(self, bucket_name: str, logger: Logger,
                 aws_access_key: str = None, aws_secret_key: str = None,
                 region_name: str = "us-east-1") -> None:
        """
        Classe utilitária para operações no S3.
        :param bucket_name: Nome do bucket S3
        :param logger: Instância do logger para registrar mensagens.
        :param aws_access_key: Chave de acesso AWS (opcional, se configurada localmente)
        :param aws_secret_key: Chave secreta AWS (opcional, se configurada localmente)
        :param region_name: Região do bucket S3
        """
        self.bucket_name: str = bucket_name
        self.logger: Logger = logger
        try:
            if aws_access_key and aws_secret_key:
                self.s3_client = boto3.client(
                     "s3",
                     aws_access_key_id=aws_access_key,
                     aws_secret_access_key=aws_secret_key,
                     region_name=region_name,
                )
            else:
                self.s3_client = boto3.client("s3", region_name=region_name)
            self.logger.info(f"Conexão com o bucket '{bucket_name}' configurada com sucesso.")
        except (NoCredentialsError, PartialCredentialsError):
            self.logger.error("Credenciais da AWS não configuradas corretamente.")
            raise Exception("Credenciais da AWS não configuradas corretamente.")

    def list_file(self, prefix: str) -> List[str]:
        """
        Lista os arquivos em uma pasta específica do bucket.
         :param prefix: Caminho da pasta no bucket (ex.: raw/ ou processed/)
         :return: Lista de arquivos encontrados no bucket
        """
        try:
            self.logger.info(f"Listando arquivos no bucket '{self.bucket_name}' com prefixo '{prefix}'...")
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            if "Contents" in response:
                file_list = [obj["Key"] for obj in response["Contents"]]
                #self.logger.info = (f"{str(len(file_list))} arquivos encontrados no prefixo '{prefix}'.")
                return file_list
            self.logger.warning(f"Nenhum arquivo no prefixo '{prefix}'.")
            return []
        except Exception as e:
            self.logger.error(f"Erro ao listar arquivos do bucket: {e}")
            raise Exception(f"Erro ao lista arquivos no bucket {e}")

    def download_file(self, s3_key: str, local_path: str) -> None:
        """
        Baixa um arquivo do bucket S3 para um caminho local.
         :param s3_key:Caminho do arquivo no bucket S3
         :param local_path: Caminho local onde o arquivo será salvo
        """
        try:
            self.logger.info(f"Iniciando download do arquivo '{s3_key}' para' {local_path}'...")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            self.logger.info(f"Arquivo '{s3_key}' baixado com sucesso para '{local_path}'.")
        except Exception as e:
            self.logger.error(f"Erro ao baixar arquivo {s3_key}: {e}")
            raise Exception(f"Erro ao baixar arquivo {s3_key}: {e}")

    def upload_file(self, local_path: str, s3_key: str) -> None:
        """
        Faz upload de um arquivo local para o bucket S3.

        :param local_path: Caminho local do arquivo.
        :param s3_key: Caminho do arquivo no bucket S3.
        """
        try:
            self.logger.info(f"Iniciando upload do arquivo '{local_path}' para '{s3_key}'...")
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            self.logger.info(f"Arquivo '{local_path}' enviado com sucesso para '{s3_key}'.")
        except Exception as e:
            self.logger.error(f"Erro ao fazer upload do arquivo '{local_path}': {e}")
            raise Exception(f"Erro ao fazer upload do arquivo '{local_path}': {e}")
