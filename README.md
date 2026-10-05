# Bronze → Silver: SC7 com Python e Iceberg

Script Python sem Spark, com pandas e awswrangler. Faz limpeza, deduplicação por
R_E_C_N_O_, merge no Iceberg e exclusão de registros marcados com *.

## Configuração local

Complete seu .env usando .env.example como referência, sem sobrescrever suas credenciais.
O script agora usa variáveis de ambiente; não usa table.example.json nem configuração JSON no S3.
As colunas selecionadas e seus nomes na silver ficam em `COLUMN_MAP`, no script.
Os tratamentos de tipos ficam em CODES, NUMBERS e DATES.

### Seleção, nomes e evolução do schema

Edite `COLUMN_MAP` em `jobs/bronze_to_silver.py`: a chave é o nome de origem
normalizado em snake_case e o valor é o nome desejado na silver. Exemplo:

```python
"c7_num": "numero_pedido",
"c7_total": "valor_total",
"s_t_a_m_p": "atualizado_em",
```

O exemplo não é aplicado automaticamente. Os nomes atuais foram preservados.
Colunas extras do Parquet, como `s_t_a_m_p` e `airbyte_meta`, são ignoradas enquanto
não forem incluídas no mapa. Cada coluna selecionada deve existir no arquivo.
As chaves de merge, RECNO, marca de exclusão, data de extração e year/month/day
são mantidas automaticamente. Para renomeá-las, inclua seus nomes normalizados
no mesmo mapa; merge, exclusão, tipos e partições acompanham os nomes de destino.
Os nomes de destino devem ser únicos e estar em snake_case.

O envio usa `schema_evolution=True`: novas colunas selecionadas podem ser adicionadas
à tabela existente; mudanças de tipo dependem da compatibilidade do Iceberg/Athena.
Não se preenchem colunas ausentes com nulos (`fill_missing_columns_in_df=False`).
Se a tabela existente contiver colunas fora do mapa, a gravação ainda pode falhar
por colunas ausentes; a seleção não remove colunas já presentes no catálogo.
Alterar um nome no mapa também não renomeia a coluna existente nem migra seu histórico:
é necessário alinhar o schema da tabela antes de usar o novo nome. Isso vale também
para nomes das chaves e das partições. Nenhuma tabela AWS é alterada pelo deploy;
a evolução ocorre durante a execução do job.

Variáveis obrigatórias: SOURCE_BUCKET (ou BRONZE_BUCKET), SOURCE_PREFIX, DATABASE, TABLE,
TABLE_LOCATION, TEMP_PATH, S3_OUTPUT e WORKGROUP.
Os três caminhos aceitam URIs completas ou prefixos separados do SILVER_BUCKET:

```dotenv
BRONZE_BUCKET=meu-bucket-bronze
SILVER_BUCKET=meu-bucket-silver
TABLE_LOCATION=iceberg/sc7/
TEMP_PATH=staging/sc7/
S3_OUTPUT=athena-results/
```

Nesse exemplo, TABLE_LOCATION vira `s3://meu-bucket-silver/iceberg/sc7/`.
SILVER_BUCKET é obrigatório quando algum desses caminhos não começa com `s3://`.
URIs completas são preservadas e podem apontar para buckets diferentes.
SOURCE_BUCKET tem prioridade sobre BRONZE_BUCKET quando ambos estão definidos.
Essas mesmas regras valem nas Variables do GitHub; o deploy envia os caminhos já
resolvidos ao Glue. ARTIFACTS_BUCKET aceita `nomebucket`, `nomebucket/pasta` ou `s3://nomebucket/pasta`.
Por exemplo, `meu-bucket/scripts` publica em
`s3://meu-bucket/scripts/releases/<job>/<commit>/<execução>/bronze_to_silver.py`.
Sem SOURCE_KEY nem --source-key, o job processa todos os arquivos `.parquet` do
SOURCE_PREFIX, incluindo subpastas, um por vez. Não é necessário informar `*.parquet`.
SOURCE_KEY ou --source-key restringe a execução a um arquivo específico.
SOURCE_VERSION_ID é opcional e só pode ser usado com um arquivo específico.

Padrões configuráveis: MERGE_KEYS=R_E_C_N_O_, DELETION_COLUMN=D_E_L_E_T_,
DATE_COLUMN=_airbyte_extracted_at, DATE_FORMAT=ISO8601, CSV_SEPARATOR=";",
ENCODING=utf-8, DECIMAL_SEPARATOR=. e formatos C7_EMISSAO_FORMAT/C7_DATPRF_FORMAT=%Y%m%d.

O .env do diretório de execução é carregado somente em main. Use --env-file para outro caminho.
Variáveis já definidas no processo têm prioridade. O .env pessoal não é lido nos testes.

As credenciais locais são obtidas pelo boto3, incluindo AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY e AWS_SESSION_TOKEN, quando temporárias.
Configure AWS_REGION ou AWS_DEFAULT_REGION. No Glue, use o papel IAM do job.
O script não imprime credenciais nem as inclui na configuração da tabela.

## Executar e testar

Na raiz do projeto:

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements-dev.txt
.\.venv\Scripts\python -m pytest --cov=jobs --cov-report=term-missing
.\.venv\Scripts\python jobs/bronze_to_silver.py
.\.venv\Scripts\python jobs/bronze_to_silver.py --source-key compras/sc7/arquivo.csv
```

As duas últimas linhas acessam a AWS e alteram a silver: a primeira lê os Parquets do
prefixo; a segunda lê somente o arquivo informado. Os testes usam somente dados sintéticos,
mocks e bloqueio de conexões de rede. Não validam o engine Athena real.

## Regras SC7

- Códigos são texto, preservando zeros à esquerda; códigos já numéricos são rejeitados.
- C7_QUANT, C7_QUJE, C7_PRECO e C7_TOTAL usam decimal(18,6), ajustável em CODES, NUMBERS e DATES.
  Não há arredondamento silencioso; separadores de milhar são rejeitados.
- C7_EMISSAO e C7_DATPRF são gravadas como texto, preservando valores inválidos e vazios. Nulos já presentes na origem permanecem nulos. Os parâmetros de formato dessas duas colunas não são aplicados nesta etapa.
- `_airbyte_extracted_at` é a coluna de origem obrigatória que define year/month/day em UTC. A normalização dos nomes a transforma em `airbyte_extracted_at` na silver. Configure `DATE_COLUMN=_airbyte_extracted_at` caso exista uma sobrescrita no ambiente ou no GitHub Actions.
- Deduplica por RECNO dentro do arquivo, mantendo a última ocorrência na ordem de leitura, sem comparar _airbyte_extracted_at. Chaves nulas causam erro.
- Se a tabela Iceberg existente tiver C7_EMISSAO/C7_DATPRF como date, será necessário adequar o schema antes de gravar texto. O job não migra tabelas existentes automaticamente.
- D_E_L_E_T_ em branco indica ativo; * remove a chave da silver. A deduplicação precede
  essa separação. A extração precisa incluir linhas completas, inclusive as excluídas.
- Os campos de origem `D_E_L_E_T_` e `R_E_C_N_O_` são normalizados na silver para
  `d_e_l_e_t` e `r_e_c_n_o`, respectivamente.
- Uma tabela silver deve receber uma única origem física de SC7, evitando colisões de RECNO.

Um arquivo específico pode ser CSV, Parquet, JSON tabular, JSONL ou NDJSON.
No modo por prefixo, somente Parquets são selecionados, com listagem paginada do S3.
Um prefixo sem Parquets termina sem escrita e informa zero arquivos processados.
Falhas interrompem a execução; arquivos já processados não são revertidos. A deduplicação
continua por arquivo e não garante prioridade da _airbyte_extracted_at entre arquivos diferentes.
Para rodar todos pelo console do Glue, publique o código atualizado e clique em Run sem
o parâmetro --source-key. Se esse parâmetro foi cadastrado manualmente, remova-o.
Buckets, banco Glue e workgroup Athena engine 3 precisam existir. A tabela Iceberg é criada
no primeiro merge de ativos. Origem, destino, staging e resultados usam prefixos separados.

## Primeiro teste AWS, sem EventBridge

Todo o CI/CD está em `.github/workflows/deploy.yml`, sem script separado de deploy.
O workflow cria o job Glue Python Shell 3.9 quando ele não existe, usando a role existente
informada em `AWS_ROLE_ARN`. Se já existe, atualiza o job e preserva sua role.
Buckets, papel IAM, banco Glue e workgroup Athena engine 3
também devem existir. O job precisa alcançar o índice pip para instalar dependências.

No GitHub, configure o environment `production` com estes Secrets:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN`, apenas se as credenciais forem temporárias.

Configure em Variables ou Secrets `AWS_REGION`, `GLUE_JOB_NAME`, `ARTIFACTS_BUCKET`,
`AWS_ROLE_ARN` (obrigatório para criar o job) e todas as
variáveis obrigatórias de processamento listadas acima. As opcionais podem ser configuradas
com os mesmos nomes; na ausência, o workflow aplica os padrões SC7.
O `.env` pessoal não é publicado nem lido pelo deploy; suas configurações precisam ser
cadastradas no GitHub. O workflow aceita as configurações de processamento em Secrets
também, com prioridade para Variables quando o mesmo nome existe nos dois lugares.
Credenciais AWS continuam exclusivamente em Secrets. Antes de autenticar, o workflow
valida os campos obrigatórios e informa somente os nomes ausentes, sem imprimir valores.

Após testes aprovados, um push em `main` publica somente o script em um caminho S3
por commit/execução e cria ou atualiza o job. Um job novo usa Python Shell 3.9, 1 DPU,
timeout de 60 minutos e uma execução por vez. O deploy instala as dependências fixadas
de `requirements.txt` via `--additional-python-modules`, com `--library-set=none`.
As configurações não secretas são enviadas como parâmetros `--env-NOME`; o script
converte esses parâmetros em variáveis do processo antes de validar a configuração.
Esses parâmetros têm prioridade sobre o ambiente e o `.env`.

O deploy preserva os campos atualizáveis da definição existente, incluindo papel IAM,
conexões, timeout e configuração de segurança. Ajusta o script, os argumentos gerenciados
e a concorrência máxima para 1. Remove argumentos antigos de arquivo/configuração;
um conflito com `NonOverridableArguments` interrompe o deploy antes do upload.
Não modifica triggers, EventBridge, bancos, buckets ou papéis IAM.

Para testar, abra Actions → Test and deploy Glue → Run workflow, selecione `main` e
informe `source_key`. Opcionalmente informe `source_version_id`. O workflow publica,
inicia uma execução do Glue e acompanha seu resultado. Sem `source_key`, apenas publica.
Falhas no job fazem o workflow falhar. Se o acompanhamento exceder 65 minutos, o workflow
falha e informa o ID; o job pode continuar executando e deve ser consultado no Glue.
Consulte os logs do job no CloudWatch para diagnóstico.

Permissões do principal de deploy: `s3:PutObject` no prefixo `<pasta>/releases/` (ou `releases/`
quando não houver pasta configurada) do bucket de
artefatos, `glue:GetJob`, `glue:CreateJob`, `glue:UpdateJob` no job e `iam:PassRole` para
seu papel de execução. A role informada deve permitir que `glue.amazonaws.com` a assuma.
Para o teste manual: `glue:StartJobRun` e `glue:GetJobRun`. Políticas de bucket e KMS,
se presentes, também precisam autorizar esse acesso.

O papel do Glue precisa de `s3:ListBucket` no bucket bronze para listar o prefixo,
ler o script e os objetos bronze, gravar/ler/apagar staging e dados silver,
executar consultas no workgroup Athena, gerenciar tabelas e tabelas temporárias no banco
Glue e escrever logs no CloudWatch. Lake Formation e SSE-KMS exigem suas permissões
correspondentes. O deploy não altera essas permissões.

O upload antecede a atualização do job: uma falha no upload não muda a definição.
Se a atualização falhar, o artefato pode permanecer no S3. Para rollback, reverta o commit
e publique novamente. Alterações em dados por uma execução manual não são revertidas
automaticamente. Scripts antigos permanecem no bucket de artefatos.

Use inicialmente arquivos sintéticos e uma tabela/prefixo silver de teste.
Reprocesse o mesmo arquivo para verificar ausência de duplicatas; depois processe
uma atualização e um registro marcado com * para verificar merge e exclusão.

## Git e infraestrutura

O workflow executa testes em PRs, sem credenciais AWS. Em main, publica somente após
os testes. Não usa OIDC, CloudFormation ou EventBridge.

A pasta infra/ está no .gitignore e foi preservada apenas localmente, incluindo uma cópia
do workflow anterior. Esses rascunhos usam a configuração antiga e exigem revisão antes
de reutilização. O .env e suas variantes privadas são ignorados; .env.example é público.
Nenhum commit foi realizado.

## Limites

- Reprocessamento por chave não garante ordem entre arquivos. Eventos antigos podem
  sobrescrever dados recentes, reinserir registros excluídos ou apagar versões mais novas.
- Merge e delete são operações separadas; leitores podem observar estado intermediário.
  Uma falha propaga erro para permitir reprocessamento. Snapshots antigos não são expurgados.
- Não há controle de concorrência ou orquestração nesta etapa; evite escritores simultâneos.
- Cada arquivo precisa caber na memória. Mais de 100 partições escritas por consulta Athena
  exigem divisão em lotes, ainda não implementada.
- Não há manutenção de snapshots, compactação ou limpeza automática de staging após falhas.

## Referências do deploy

- [Bibliotecas e configuração do Glue Python Shell](https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html)
- [UpdateJob substitui a definição anterior](https://docs.aws.amazon.com/cli/latest/reference/glue/update-job.html)
- [CreateJob cria o job e associa seu papel IAM](https://docs.aws.amazon.com/cli/latest/reference/glue/create-job.html)
- [Autenticação AWS no GitHub Actions, incluindo access keys](https://github.com/aws-actions/configure-aws-credentials#non-oidc-authentication-options)
