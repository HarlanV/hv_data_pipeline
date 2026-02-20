# # hv_data_pipeline
Respositorio para desafio da BeAnalytics

# Configuração de projeto

Para esse projeto usaremos como serviços principais os da Amazon AWS, devido disponibilidade de uso gratuito de alguns serviços por 12 meses + creditos para uso de teste.
Aqui usaremos:
- S3 para o Data Lake
- AWS Glue Job para o processamento
- Pyspark + Delta

Não usaremos ferramentas como Databricks devido a extrema limitação na versão gratuita, que impossibilita uma comunição adequada com Services de Lakes.


## Configuração Inicial

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

#### Chaves de acesso
Nestre processo seletivo, para possibilitar avaliação, as chaves geradas para usuário IAM serão disponibilizadas via e-mail na conclusão do desafio. Por segurança, será mantido funcional pelos 7 dias seguintes.

### Glue

#### Job Details
Para este projeto, foram configurados o seguinte:

- IAM Role: role criado na etapa do S3
- Glue Version: 5.1
- 2 workers (minimo permitido)
- Retries: 0 (devido custo)
- timeout: 10 min


#### Arquitetura (em construção):

hv-data-pipeline/
│
├── bronze/
│   ├── bronze_mco.py
│   └── bronze_mapa_empresa.py
│
├── silver/
│   ├── silver_mapa_controle_oper.py
│   └── silver_mapa_empresa.py
│
├── gold/
│   ├── dim_empresa.py
│   ├── dim_tempo.py
│   ├── dim_justificativa.py
│   └── fato_viagem.py
│
├── common/
│   ├── spark_session.py
│   ├── paths.py
│   └── utils.py
│
├── main.py
└── tests/





### CI/CD (em construção)


Por segurança, vamos utilizar o storage credentials do nosso catalog. Para isso vamos em 'catalgo' > 'System' > 'information_schema' > 'storage_catalog'. Clique em Create e selecione a opção SQL.
Execute a consulta abaixo, substituindo os valores de Access key e Secret key pelos gerados anteriormente no IAM.


### Comunicação Databricks-AWS

Devido às limitações do Databricks Free (Serverless), utilizamos pre-signed URLs para leitura do S3.
Em ambiente produtivo, o acesso ao S3 seria feito nativamente pelo Databricks, utilizando IAM Role associada ao cluster (sem chaves estáticas) e leitura via s3a://. Os dados seriam armazenados em Delta Lake, garantindo versionamento, controle transacional e governança adequada.

### Configurações iniciais 

#### Credenciais de acesso
Devido a impossibilidade de utilizarmos o secret manager na versão gratuita, utilizamos um arquivo .py com as chaves de acesso ao S3. Para isso:

- Copie o arquivo lib\secret_manager_example.py e renomeie para secret_manager.py
- Substitua os valores de Access key e Secret Access key pelos da sua base
- Caso necessario, substitua também o nome da região do seu S3

#### Pipeline