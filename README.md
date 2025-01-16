# SageMaker Data Pipeline Project

## Descrição
Este projeto implementa uma pipeline de dados utilizando o SageMaker da AWS, no qual foca nas três camadas de dados: Bronze, Prata e Ouro. O objetivo é ler arquivos do S3, processá-los e salvá-los em diferentes camadas conforme o nível de processamento e qualidade dos dados.

Todo o desenvolvimento foi feito seguindo boas práticas de engenharia de software, tais como:
- **POO (Programação Orientada a Objetos)**: Cada camada é implantada como uma classe Python.
- **Modularização**: Código separado em arquivos e funções específicas.
- **Logging**: Rastreamento de ações com logging.
- **Testes Automatizados**: Testes com `pytest` para validação de cada etapa.

## Estrutura do projeto
```plaintext
housing_pipeline/
├── data/
│   ├── bronze/        # Dados originais (bronze)
│   ├── silver/        # Dados transformados (prata)
│   └── gold/          # Dados finais (ouro)
├── src/
│   ├── __init__.py    # Inicialização do pacote
│   ├── bronze.py      # Scripts para leitura/validação de dados
│   ├── silver.py      # Scripts para limpeza e transformação
│   ├── gold.py        # Scripts para agregação e preparação final
│   ├── logger.py      # Loggin do projeto sendo utilizado nas outras classes
│   └── utils.py       # Funções auxiliares (paths, etc.)
├── tests/
│   ├── __init__.py
│   └── test_pipeline.py
├── requirements.txt   # Dependências do projeto
├── README.md          # Documentação básica
└── main.py            # Script principal para execução
```

## Fluxo da Pipeline (camadas)
### 1. Bronze Layer
- Conecta ao S3 e baixa os arquivos brutos para o SageMaker (pasta `data/bronze`).
- Realiza validações básicas como integridade do arquivo.

### 2. Silver Layer
- Processa os dados da camada Bronze.
- Limpeza de valores nulos e normalização.
- Salva os arquivos transformados na pasta `data/silver`.

### 3. Gold Layer
- Realiza agregações e gera datasets prontos para análise.
- Salva os arquivos finais na pasta `data/gold`.

## Tecnologias utilizadas
- Python
- AWS SageMaker
- `boto3` (para interação com o S3)
- `pandas` (para manipulação de dados)
- `logging` (para rastreamento de ações)
- `pytest` (para testes automatizados)