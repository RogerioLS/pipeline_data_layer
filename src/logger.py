#src/logger.py

from src import logging
from src import os
from src import datetime

class Logger:
    """
    Classe para gerenciar logs em arquivos e no console, com suporte para criar logs em diretórios específicos e
    salvar logs em arquivos com timestamp. Ideal para projetos reutilizáveis.
    """
    def __init__(self, log_dir: str = "logs", log_file_prefix: str = "application") -> None:
        """
        Inicializa o logger com configuração para console e arquivo.

        :param log_dir: Diretório onde os arquivos de log serão armazenados (padrão: "logs").
        :param log_file_prefix: Prefixo do nome do arquivo de log (padrão: "application").
        """
        # Cria o diretório para logs, se não existir
        os.makedirs(log_dir, exist_ok=True)

        # Nome do arquivo de log com timestamp
        log_filename: str = f"{log_file_prefix}_{datetime.now().strftime('%Y-%m-%d_%H-%M:%S')}.log"
        self.log_path: str = os.path.join(log_dir, log_filename)

        # Configuracao do logger
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(self.log_path, encoding="utf-8")
            ]
        )
        self.logger: logging.Logger = logging.getLogger()

    def info(self, message: str) -> None:
        """
        Registra uma mensagem de nível INFO no log.

        :param message: Mensagem a ser registrada.
        """
        self.logger.info(message)

    def warning(self, message: str) -> None:
        """
        Registra uma mensagem de nível WARNING no log.

        :param message: Mensagem a ser registrada.
        """
        self.logger.warning(message)

    def error(self, message: str) -> None:
        """
        Registra uma mensagem de nível ERROR no log.

        :param message: Mensagem a ser registrada.
        """
        self.logger.error(message)

    def critical(self, message: str) -> None:
        """
        Registra uma mensagem de nível CRITICAL no log.

        :param message: Mensagem a ser registrada.
        """
        self.logger.critical(message)

    def get_log_path(self) -> str:
        """
        Retorna o caminho completo do arquivo de log atualmente em uso.

        :return: Caminho do arquivo de log.
        """
        return self.log_path
