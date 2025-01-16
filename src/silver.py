from src import pd
from src import os
from src.logger import Logger


class SilverLayer:
    def __init__(self, bronze_path: str, silver_path: str, logger: Logger) -> None:
        """
        Inicializa a camada Silver.

        :param bronze_path: Caminho para os dados processados pela camada Bronze.
        :param silver_path: Caminho para salvar os dados limpos.
        :param logger: Instância do logger para registrar eventos.
        """
        self.bronze_path = bronze_path
        self.curated_path = silver_path
        self.logger = logger

    def load_data(self, file_name: str) -> pd.DataFrame:
        """
        Carrega os dados da camada Bronze em um DataFrame.

        :param file_name: Nome do arquivo a ser carregado.
        :return: DataFrame com os dados carregados.
        """
        file_path = os.path.join(self.bronze_path, file_name)
        self.logger.info(f"Lendo o arquivo {file_path} da camada Bronze.")
        try:
            data = pd.read_csv(file_path)
            self.logger.info(f"Arquivo {file_name} carregado com sucesso.")
            return data
        except Exception as e:
            self.logger.error(f"Erro ao carregar o arquivo {file_name}: {e}")
            raise

    def clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza a limpeza e transformação dos dados.

        :param data: DataFrame com os dados a serem limpos.
        :return: DataFrame limpo e transformado.
        """
        self.logger.info("Iniciando o processo de limpeza e transformação dos dados.")

        try:
            # Remover colunas irrelevantes
            if "Id" in data.columns:
                data = data.drop(columns=["Id"])
                self.logger.info("Coluna 'Id' removida.")

            # Tratar valores ausentes (exemplo: preencher valores nulos com 0)
            data = data.fillna(0)
            self.logger.info("Valores ausentes tratados com preenchimento padrão (0).")

            # Conversão de tipos (exemplo: garantir que certas colunas sejam categóricas)
            if "MSZoning" in data.columns:
                data["MSZoning"] = data["MSZoning"].astype("category")
                self.logger.info("Coluna 'MSZoning' convertida para tipo categórico.")

            # Criar uma nova coluna calculada, como preço por metro quadrado
            if "LotArea" in data.columns and "SalePrice" in data.columns:
                data["PricePerLotArea"] = data["SalePrice"] / data["LotArea"]
                self.logger.info("Nova coluna 'PricePerLotArea' criada com sucesso.")

            self.logger.info("Processo de limpeza e transformação concluído.")
            return data
        except Exception as e:
            self.logger.error(f"Erro durante a limpeza e transformação dos dados: {e}")
            raise

    def save_to_curated(self, data: pd.DataFrame, file_name: str) -> None:
        """
        Salva os dados limpos na Camada Ouro.

        :param data: DataFrame com os dados limpos.
        :param file_name: Nome do arquivo a ser salvo.
        """
        curated_file_path = os.path.join(self.curated_path, file_name)
        self.logger.info(f"Salvando os dados limpos em: {curated_file_path}")

        try:
            data.to_csv(curated_file_path, index=False)
            self.logger.info(f"Arquivo {file_name} salvo com sucesso na camada Prata.")
        except Exception as e:
            self.logger.error(f"Erro ao salvar o arquivo {file_name}: {e}")
            raise
