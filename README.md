# io-data-engineer

PROMPT — Implementação de uma biblioteca de acesso a tabelas no AWS Glue com PySpark

1. Objetivo

Desenvolva uma biblioteca Python profissional, modular, reutilizável e preparada para produção, responsável pela leitura e escrita de tabelas no ambiente AWS Glue com PySpark.

A biblioteca deverá oferecer suporte a quatro categorias de armazenamento:

1. Apache Hudi.
2. Delta Lake.
3. Apache Iceberg.
4. Formatos convencionais: Parquet, CSV, JSON e ORC.

O objetivo é centralizar as operações de leitura e escrita, evitando duplicação de código e separando as particularidades de cada tecnologia.

IMPORTANTE: A implementação deve seguir rigorosamente a documentação oficial da AWS Glue, Apache Hudi, Delta Lake e Apache Iceberg. Não invente métodos, parâmetros, opções ou comportamentos.

2. Arquitetura obrigatória

Implemente a seguinte estrutura:

glue_table_access/
│
├── README.md
├── requirements.txt
├── config.py
├── exceptions.py
├── common.py
│
├── hudi/
│   ├── __init__.py
│   ├── reader.py
│   └── writer.py
│
├── delta/
│   ├── __init__.py
│   ├── reader.py
│   └── writer.py
│
├── iceberg/
│   ├── __init__.py
│   ├── reader.py
│   └── writer.py
│
├── standard/
│   ├── __init__.py
│   ├── reader.py
│   └── writer.py
│
└── examples/
    ├── hudi_example.py
    ├── delta_example.py
    ├── iceberg_example.py
    └── standard_parquet_example.py

3. Responsabilidades das classes

Cada tecnologia deverá possuir duas classes independentes.

3.1 Hudi

HudiReader

Responsável por:

* Ler tabelas Hudi registradas no AWS Glue Data Catalog.
* Utilizar glueContext.create_data_frame.from_catalog().
* Retornar um Spark DataFrame.
* Aceitar transformation_ctx.

HudiWriter

Responsável por:

* Escrever utilizando glueContext.write_data_frame.from_catalog().
* Suportar INSERT, UPSERT, BULK_INSERT e DELETE.
* Configurar record key.
* Configurar precombine field.
* Configurar partition field.
* Suportar COPY_ON_WRITE e MERGE_ON_READ, conforme compatibilidade.
* Configurar sincronização com Glue Data Catalog.
* Permitir opções adicionais de escrita.

3.2 Delta Lake

DeltaReader

Responsável por:

* Ler tabelas Delta por caminho S3.
* Ler tabelas Delta registradas em catálogo Spark.
* Retornar Spark DataFrame.

DeltaWriter

Responsável por:

* Escrever em formato Delta.
* Suportar APPEND, OVERWRITE, ERROR e IGNORE.
* Permitir escrita por caminho S3 ou tabela catalogada.
* Implementar MERGE INTO.
* Permitir atualização e inserção de registros conforme chaves de negócio.

3.3 Apache Iceberg

IcebergReader

Responsável por:

* Ler tabelas através do catálogo Spark configurado.
* Suportar Glue Data Catalog como catálogo Iceberg.
* Retornar Spark DataFrame.

IcebergWriter

Responsável por:

* Implementar APPEND.
* Implementar OVERWRITE.
* Implementar OVERWRITE PARTITIONS.
* Implementar MERGE INTO quando suportado pela versão.
* Utilizar preferencialmente DataFrameWriterV2.
* Preservar as propriedades transacionais do Iceberg.

3.4 Formatos convencionais

StandardReader

Responsável por:

* Ler Parquet.
* Ler CSV.
* Ler JSON.
* Ler ORC.
* Aceitar opções de leitura.
* Permitir leitura por caminho S3.

StandardWriter

Responsável por:

* Escrever Parquet.
* Escrever CSV.
* Escrever JSON.
* Escrever ORC.
* Suportar APPEND, OVERWRITE, ERROR e IGNORE.
* Permitir particionamento.
* Aceitar opções adicionais de escrita.

Utilizar as APIs nativas:

spark.read.format(...)
df.write.format(...)

Não utilizar APIs específicas de Hudi, Delta ou Iceberg para formatos convencionais.

4. Configuração e abstração

Criar classes de configuração utilizando dataclasses.

Implementar:

* HudiConfig
* DeltaConfig
* IcebergConfig
* StandardConfig

As configurações devem validar parâmetros obrigatórios e impedir combinações inválidas.

Evitar parâmetros espalhados pelo projeto.

5. Requisitos de engenharia

A implementação deve observar os seguintes princípios:

* SOLID.
* Clean Code.
* Separação de responsabilidades.
* Baixo acoplamento.
* Alta coesão.
* Type hints.
* Docstrings.
* Tratamento de exceções específicas.
* Logs estruturados.
* Validação de schemas.
* Validação de colunas obrigatórias.
* Configurações imutáveis quando apropriado.
* Compatibilidade com o ambiente gerenciado do AWS Glue.

Não introduzir abstrações desnecessárias.

Não criar uma classe genérica excessivamente complexa para esconder as particularidades de cada formato.

6. Requisitos de confiabilidade

Prever os seguintes cuidados:

1. Validar se o DataFrame recebido é válido.
2. Validar a existência das colunas obrigatórias.
3. Validar chaves de negócio.
4. Tratar DataFrames vazios de maneira configurável.
5. Evitar ações Spark desnecessárias durante validações.
6. Tratar duplicidades conforme a estratégia de cada formato.
7. Não permitir que operações OVERWRITE sejam executadas sem uma decisão explícita.
8. Registrar logs de leitura e escrita.
9. Preservar a consistência de particionamento.
10. Não assumir que uma tabela Parquet convencional pode ser utilizada diretamente como Hudi, Delta ou Iceberg.

7. Compatibilidade com AWS Glue

Investigue e documente a compatibilidade de cada tecnologia com as versões do AWS Glue.

Para cada formato, apresentar:

* Versões compatíveis.
* Dependências necessárias.
* Parâmetros de inicialização do Job.
* Configurações Spark.
* Configurações do Glue Data Catalog.
* Limitações conhecidas.
* Particularidades de leitura e escrita.

Não assumir que todas as versões do Glue suportam os quatro formatos nativamente.

Não adicionar dependências incompatíveis com o runtime gerenciado.

8. Exemplos de utilização

Criar exemplos executáveis para cada formato.

Cada exemplo deve demonstrar:

1. Inicialização do Spark e GlueContext.
2. Configuração da tabela.
3. Instanciação do Reader.
4. Instanciação do Writer.
5. Leitura dos dados.
6. Transformação ilustrativa.
7. Escrita dos dados.
8. Tratamento de exceções.

Utilizar nomes ilustrativos de tabelas, como:

* tbl_processado_operacao_sor
* tbl_processado_operacao_apropriada
* tbl_processado_operacao_consolidada

Utilizar default_database como database de exemplo.

9. Testes automatizados

Criar testes com pytest para:

* Validação das configurações.
* Validação de DataFrames.
* Validação de colunas.
* Seleção das operações de escrita.
* Construção das opções de cada formato.
* Tratamento de exceções.
* Comportamento dos Readers e Writers utilizando mocks.

Separar testes unitários de testes de integração.

Não afirmar que os testes de integração foram executados em AWS Glue se não houver um ambiente real disponível.

10. Documentação

O README.md deverá conter:

* Objetivo da biblioteca.
* Arquitetura.
* Responsabilidades das classes.
* Instalação e configuração.
* Exemplos de leitura e escrita.
* Matriz comparativa dos formatos.
* Configuração de cada tecnologia no AWS Glue.
* Limitações e cuidados de produção.

Incluir referências diretas à documentação oficial.

11. Auditoria técnica obrigatória

Antes de finalizar, faça uma auditoria de todas as APIs utilizadas.

Verifique especialmente:

* glueContext.create_data_frame.from_catalog()
* glueContext.write_data_frame.from_catalog()
* APIs de leitura e escrita Hudi.
* APIs Delta Lake.
* DataFrameWriterV2 do Iceberg.
* MERGE INTO.
* Compatibilidade das opções de escrita com as versões escolhidas.

Não confundir métodos de leitura convencional com métodos de streaming.

Não utilizar write_data_frame.from_catalog() como solução universal para todos os formatos.

Se determinada operação não for suportada diretamente pela API escolhida, implemente-a utilizando a API oficial apropriada e explique a decisão.

12. Entrega

Entregar:

1. Projeto completo com todos os arquivos Python.
2. README.md detalhado.
3. Exemplos de utilização.
4. Testes automatizados.
5. requirements.txt compatível.
6. Arquivo ZIP contendo toda a estrutura.

O ZIP deverá ter o nome:

aws_glue_table_access.zip

Antes de entregar, verificar:

* Imports.
* Sintaxe Python.
* Organização dos módulos.
* Consistência das assinaturas.
* Compatibilidade entre classes e configurações.
* Ausência de métodos fictícios.
* Ausência de configurações contraditórias.

Critério principal de qualidade: prefira uma implementação tecnicamente correta e compatível com AWS Glue a uma implementação aparentemente elegante, mas baseada em APIs ou opções inexistentes.
