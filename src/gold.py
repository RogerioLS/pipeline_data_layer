from src import pd
from src import os
from src.logger import Logger


class GoldLayer:
    def __init__(self, silver_path: str, gold_path: str, logger: Logger) -> None:
        """
        Inicializa a camada Gold.

        :param silver_path: Caminho para os dados limpos da camada Prata.
        :param gold_path: Caminho para salvar os dados consolidados.
        :param logger: Instância do logger para registrar eventos.
        """
        self.silver_path = silver_path
        self.gold_path = gold_path
        self.logger = logger

    def load_data(self) -> pd.DataFrame:
        """
        Carrega e combina todos os arquivos da camada Prata.

        :return: DataFrame consolidado com todos os dados limpos.
        """
        self.logger.info("Lendo os arquivos da camada Prata para consolidação.")
        try:
            all_files = os.listdir(self.silver_path)
            data_frames = []

            for file_name in all_files:
                file_path = os.path.join(self.silver_path, file_name)
                self.logger.info(f"Lendo arquivo: {file_path}")
                data = pd.read_csv(file_path)
                data_frames.append(data)

            consolidated_data = pd.concat(data_frames, ignore_index=True)
            self.logger.info("Todos os arquivos foram consolidados com sucesso.")
            return consolidated_data
        except Exception as e:
            self.logger.error(f"Erro ao consolidar arquivos da camada Prata: {e}")
            raise

    def aggregate_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza agregações e cálculos avançados.

        :param data: DataFrame consolidado.
        :return: DataFrame com os dados agregados.
        """
        self.logger.info("Iniciando o processo de agregação e cálculo avançado.")
        try:
            # Exemplo: calcular a média de preço por zona (MSZoning)
            if "MSZoning" in data.columns and "SalePrice" in data.columns:
                aggregated_data = data.groupby("MSZoning", as_index=False).agg(
                    AvgSalePrice=("SalePrice", "mean"),
                    TotalLots=("LotArea", "sum"),
                    Count=("SalePrice", "count"),
                )
                self.logger.info("Agregações realizadas com sucesso.")
                return aggregated_data
            else:
                self.logger.warning("As colunas necessárias para agregação não estão presentes no DataFrame.")
                return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
        except Exception as e:
            self.logger.error(f"Erro ao agregar os dados: {e}")
            raise

    def save_to_gold(self, data: pd.DataFrame, file_name: str) -> None:
        """
        Salva os dados agregados na Camada Ouro.

        :param data: DataFrame com os dados agregados.
        :param file_name: Nome do arquivo a ser salvo.
        """
        final_file_path = os.path.join(self.gold_path, file_name)
        self.logger.info(f"Salvando os dados finais em: {final_file_path}")
        try:
            data.to_csv(final_file_path, index=False)
            self.logger.info(f"Arquivo {file_name} salvo com sucesso na camada Ouro.")
        except Exception as e:
            self.logger.error(f"Erro ao salvar o arquivo {file_name}: {e}")
            raise
