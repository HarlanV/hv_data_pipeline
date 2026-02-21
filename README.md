# # hv_data_pipeline
Respositorio para desafio da BeAnalytics

## Resumo

### Datasets e Fontes
- Aqui foi utilizado o `Mapa de Controle Operacional (MCO)`, com todos os dados disponiveis de 2025 (até o mês 9)
- Ao invés de um segundo dataframe, optou-se incluir tabelas disponiveis no PDF de dicionario de dados do MCO. 
- Os dados foram salvos em csv para serem consumidos normalmente pelo pipeline

### Objetivo
Montar o pipeline desde o consumo da raw até a gold. Focaremos em uma unica fato com relações com as dimensões.

### Ferramentas do projeto

### Técnicas e Regras aplicadas
Aqui usamos a arquitetura Medallion com as camadas Gold, Silver e Bronze, sem necessidade de views. Para facilitar as consultas e integração, 

Para esse projeto usaremos como serviços principais os da Amazon AWS Free Tier. Foi escolhido a Amazon devido a gratuidade e principalmente devido a gratuidade por 12 meses
Aqui usaremos:
- S3 para o Data Lake
- AWS Glue Job para o processamento
- Athena para visualização de tabelas
- Pyspark + Delta

### Arquitetura de Dados
Este projeto segue a arquitetura Medallion (Bronze, Silver e Gold) utilizando AWS S3 e AWS Glue.

- *Raw*: Arquivos CSV armazenados no S3
- *Bronze*: Conversão para Parquet, mantendo dados no estado original
- *Silver*: Dados tratados e padronizados utilizando Delta Lake
- *Gold*: Modelagem dimensional (fatos e dimensões) em Delta Lake

As camadas Silver e Gold foram registradas no catalog por exigirem governança, controle estrutural e otimização para consumo analítico. A camada Bronze, por conter apenas dados brutos, sem controle de schema ou regras de negócio, não possui catalog.

Estrutura simplificada:

Raw (CSV - S3)  
        |
Bronze (Parquet - S3)  
        |
Silver (Delta - S3)  
        |
Gold (Delta - S3)




#### Chaves de acesso S3
Nestre processo seletivo, para possibilitar avaliação, as chaves geradas para usuário IAM serão disponibilizadas via e-mail na conclusão do desafio. Por segurança, será mantido funcional pelos 7 dias seguintes.


## Configurações de Projeto
Aqui um passo a passo com as principais etapas de configuração do ambiente do projeto, para caso de possivel replicação.

### S3
- Foi definido a região de `us-east-1` para o bucket por motivos de convenção apenas.
- Criamos as pastas da nossa arquitetura medallion: `bronze`, `silver` e `gold`.
- Criamos uma pasta `raw` com subpastas para nossos arquivos CSVs fontes de dados.
Ex.: raw\mapa_controle_operacional\

Nestre projeto foi criado um bucket chamado `hv-challenge`. Caso seja criado algum com outro nome, atentar para mudança durante configuração.

#### Pastas do S3
Aqui precisamos criar as pastas que iremos usar na nossa arquitetura medallion
- Bronze
- Silver
- Gold

Também precisamos de uma estrutura de pastas para os arquivos CSVs que usaremos como fonte. Usando os Mapa de controle operacional como exemplo, temos:
- raw
    - mapa_controle_operacional
    - outros_datasets_utilizados

Para manter a simplicidade, incluimos manualmente os arquivos CSV dentro da raw.

#### Policy de usuário IAM
Aqui vamos utilizar uma policy personalizada. Para isso, utilize o arquivo `policy_user.json`.
Esta policy concente permissões de:

- Listagem para o bucket
- Leitura para a raw
- Leitura + escrita + deleção para bronze
- Leitura + escrita + deleção para silver
- Leitura + escrita + deleção para gold

#### Credentials do usuário IAM (Access key e Secret access key)
- No usuário IAM criado, vá em 'Security Credentials' e crie uma 'Access key'
- Selecione Application Running outside AWS
- Copie e salve o Access key e Secret access key ao final (só aparecem uma vez)

#### Criação de Role IAM
Aqui criaremos uma role para ser usada posteriormente pelo AWS Glue. Por isso escolhemos:
- Trusted entity type: AWS Service
- Use case: Glue

#### Policies Role IAM
Aqui usaremos a police padrão `AWSGlueServiceRole`. Também incluiremos uma policy personalizada json. Para isso utilize o arquivo `policy_role_glue.json`. Esta policy concede permissão de:

- Listagem para o bucket
- Leitura para a raw
- Leitura + escrita + deleção para bronze
- Leitura + escrita + deleção para silver
- Leitura + escrita + deleção para gold



### Glue

#### Job Details
Para este projeto, foram configurados o seguinte:

- Engine: Spark
- Python version: 3.11 (Glue 5.1 runtime)
- Job type: Spark ETL
- Worker type: G.1X
- Number of workers: 2 (minimum required for Glue 5.1)
- Auto Scaling: enabled
- CloudWatch logs: enabled
- Spark UI: enabled
- Job metrics: enabled
- Job insights: disabled (cost optimization)
- Glue Data Catalog: enabled
- Job bookmark: disabled
- IAM Role: role criado na etapa do S3
- Glue Version: 5.1
- 2 workers (minimo permitido)
- Retries: 0 (devido custo)
- timeout: 10 min
- Script path: `s3://hv-challenge/scripts/`
- Script filename: `main.py`
- Python library path: `s3://hv-challenge/scripts/hv_pipeline.zip`


### CI/CD:
- GitGub Actions
- Trigger automático a cada push na branch `main`
- Build do `hv_pipeline.zip` contendo as camadas (bronze, silver, gold, common)
- Upload do ZIP para o S3 (`scripts/`)
- Upload do `main.py` como entrypoint do Glue Job

A execução do job não é automática após o deploy, garantindo maior controle operacional e evitando execuções acidentais em ambiente produtivo.
