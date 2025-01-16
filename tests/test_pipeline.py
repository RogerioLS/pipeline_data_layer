from tests import pd
from tests import os
from tests import pytest
from tests import MagicMock, patch
from tests import Logger
from tests import S3Utils
from tests import BronzeLayer
from tests import SilverLayer
from tests import GoldLayer


@pytest.fixture
def mock_logger():
    """Cria um logger mockado para os testes."""
    logger = Logger(log_dir="logs", log_file_prefix="test_pipeline")
    logger.info = MagicMock()
    logger.error = MagicMock()
    return logger


@pytest.fixture
def mock_s3_utils(mock_logger):
    """Cria um mock para a classe S3Utils."""
    s3_utils = S3Utils(bucket_name="mock-bucket", logger=mock_logger)
    s3_utils.upload_file = MagicMock()
    s3_utils.download_file = MagicMock()
    s3_utils.list_file = MagicMock(return_value=["raw/mock_file.csv"])
    return s3_utils


def test_logger_initialization():
    """Testa se o logger é inicializado corretamente."""
    logger = Logger(log_dir="logs", log_file_prefix="test_logger")
    assert logger is not None
    assert os.path.exists("logs"), "A pasta de logs deveria ser criada."


def test_s3_utils_list_file(mock_s3_utils):
    """Testa se a listagem de arquivos no S3 retorna o esperado."""
    files = mock_s3_utils.list_file("raw/")
    assert files == ["raw/mock_file.csv"], "A listagem de arquivos está incorreta."
    mock_s3_utils.logger.info.assert_called_with("Conexão com o bucket 'mock-bucket' configurada com sucesso.")


def test_bronze_layer_download_and_read(mock_s3_utils, mock_logger):
    """Testa o download e a leitura de dados na camada Bronze."""
    bronze_layer = BronzeLayer(s3_utils=mock_s3_utils, local_bronze_folder="data/bronze", logger=mock_logger)

    # Simular o download e a leitura
    with patch("builtins.open", new_callable=MagicMock):
        bronze_layer.download_and_read_raw_data("raw/mock_file.csv")

    # Verificar chamadas
    mock_s3_utils.download_file.assert_called_with("raw/mock_file.csv", "data/bronze/mock_file.csv")
    mock_logger.info.assert_called_with("Arquivo raw/mock_file.csv lido com sucesso.")


def test_silver_layer_clean_data_simple(mock_logger):
    """Testa o processo básico de limpeza de dados na camada Silver."""
    # Instância da camada Silver
    silver_layer = SilverLayer(
        bronze_path="data/bronze",
        silver_path="data/silver",
        logger=mock_logger
    )

    # Dados simulados
    raw_data = pd.DataFrame({
        "Id": [1, 2, 3],
        "LotArea": [5000, None, 10000],
        "SalePrice": [250000, 375000, None],
        "MSZoning": ["RL", None, "RM"]
    })

    # Executa a limpeza
    cleaned_data = silver_layer.clean_data(raw_data)

    # Verificações simples
    assert "Id" not in cleaned_data.columns, "A coluna 'Id' não foi removida."
    assert cleaned_data["LotArea"].isnull().sum() == 0, "Valores ausentes em 'LotArea' não foram tratados."
    assert cleaned_data["SalePrice"].isnull().sum() == 0, "Valores ausentes em 'SalePrice' não foram tratados."
    assert cleaned_data["MSZoning"].dtype.name == "category", "A coluna 'MSZoning' não foi convertida para categórico."
    assert "PricePerLotArea" in cleaned_data.columns, "A coluna 'PricePerLotArea' não foi criada."

    # Verifica se o logger foi chamado
    mock_logger.info.assert_any_call("Iniciando o processo de limpeza e transformação dos dados.")
    mock_logger.info.assert_any_call("Coluna 'Id' removida.")
    mock_logger.info.assert_any_call("Valores ausentes tratados com preenchimento padrão (0).")


def test_gold_layer_aggregate_data(mock_logger):
    """Testa o processo de agregação de dados na camada Ouro."""
    # Instância da camada Gold
    gold_layer = GoldLayer(silver_path="data/silver", gold_path="data/gold", logger=mock_logger)

    # Dados simulados como DataFrame
    cleaned_data = pd.DataFrame({
        "SalePrice": [100, 200, 300],
        "MSZoning": ["RL", "RM", "RL"],
        "LotArea": [100, 200, 300],
    })

    # Executa o método de agregação
    aggregated_data = gold_layer.aggregate_data(cleaned_data)

    # Verificações simples
    assert "MSZoning" in aggregated_data.columns, "A coluna 'MSZoning' deveria estar no DataFrame agregado."
    assert "AvgSalePrice" in aggregated_data.columns, "A coluna 'AveragePrice' deveria estar no DataFrame agregado."
    assert len(aggregated_data) > 0, "O DataFrame agregado não deveria estar vazio."

    # Verifica se o logger foi chamado
    mock_logger.info.assert_any_call("Iniciando o processo de agregação e cálculo avançado.")


def test_log_upload_to_s3(mock_s3_utils, mock_logger):
    """Testa o upload dos arquivos de log no S3."""
    log_path = mock_logger.get_log_path()

    # Simular upload
    current_date = "2025-01-10"
    year, month = current_date.split("-")[:2]
    log_s3_path = f"logs/{year}/{month}/test_pipeline.log"

    mock_s3_utils.upload_file(log_path, log_s3_path)
    mock_s3_utils.upload_file.assert_called_with(log_path, log_s3_path)
    mock_logger.info.assert_called_with(f"Conexão com o bucket 'mock-bucket' configurada com sucesso.")
