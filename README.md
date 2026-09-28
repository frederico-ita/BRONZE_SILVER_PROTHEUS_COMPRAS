# Bronze → Silver: SC7 com Python e Iceberg

Script Python sem Spark, com pandas e awswrangler. Faz limpeza, deduplicação por
R_E_C_N_O, merge no Iceberg e exclusão de registros marcados com *.

## Configuração local

Complete seu .env usando .env.example como referência, sem sobrescrever suas credenciais.
O script agora usa variáveis de ambiente; não usa table.example.json nem configuração JSON no S3.
O esquema das colunas fica em SC7_DTYPES, no script.

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
resolvidos ao Glue. ARTIFACTS_BUCKET continua sendo somente o bucket do script.
SOURCE_KEY informa o objeto a processar e pode ser substituída por --source-key.
SOURCE_VERSION_ID é opcional.

Padrões configuráveis: MERGE_KEYS=r_e_c_n_o, DELETION_COLUMN=d_e_l_e_t_d,
DATE_COLUMN=extraction_date, DATE_FORMAT=ISO8601, CSV_SEPARATOR=";",
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
.\.venv\Scripts\python jobs/bronze_to_silver.py --source-key compras/sc7/arquivo.csv
```

A última linha acessa a AWS e altera a silver. Os testes usam somente dados sintéticos,
mocks e bloqueio de conexões de rede. Não validam o engine Athena real.

## Regras SC7

- Códigos são texto, preservando zeros à esquerda; códigos já numéricos são rejeitados.
- C7_QUANT, C7_QUJE, C7_PRECO e C7_TOTAL usam decimal(18,6), ajustável em SC7_DTYPES.
  Não há arredondamento silencioso; separadores de milhar são rejeitados.
- C7_EMISSAO e C7_DATPRF são datas. Branco/nulo vira nulo; datas inválidas causam erro.
- extraction_date continua obrigatória além dos campos SC7 e define year/month/day em UTC.
- Mantém a maior data por RECNO dentro do arquivo. Empates conflitantes e chaves nulas causam erro.
- D_E_L_E_T_D em branco indica ativo; * remove a chave da silver. A deduplicação precede
  essa separação. A extração precisa incluir linhas completas, inclusive as excluídas.
- Se sua origem usa D_E_L_E_T_, configure DELETION_COLUMN=d_e_l_e_t.
- Uma tabela silver deve receber uma única origem física de SC7, evitando colisões de RECNO.

Formatos: CSV, Parquet, JSON tabular, JSONL e NDJSON. Cada execução lê somente o objeto informado.
Buckets, banco Glue e workgroup Athena engine 3 precisam existir. A tabela Iceberg é criada
no primeiro merge de ativos. Origem, destino, staging e resultados usam prefixos separados.

## Primeiro teste AWS, sem EventBridge

O deploy está em `.github/workflows/deploy.yml`, com implementação em
`scripts/deploy_glue.py`. Não cria infraestrutura; exige um job Glue Python Shell 3.9
já existente, com papel IAM configurado. Buckets, banco Glue e workgroup Athena engine 3
também devem existir. O job precisa alcançar o índice pip para instalar dependências.

No GitHub, configure o environment `production` com estes Secrets:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN`, apenas se as credenciais forem temporárias.

Configure em Variables ou Secrets `AWS_REGION`, `GLUE_JOB_NAME`, `ARTIFACTS_BUCKET` e todas as
variáveis obrigatórias de processamento listadas acima. As opcionais podem ser configuradas
com os mesmos nomes; na ausência, o workflow aplica os padrões SC7.
O `.env` pessoal não é publicado nem lido pelo deploy; suas configurações precisam ser
cadastradas no GitHub. O workflow aceita as configurações de processamento em Secrets
também, com prioridade para Variables quando o mesmo nome existe nos dois lugares.
Credenciais AWS continuam exclusivamente em Secrets. Antes de autenticar, o workflow
valida os campos obrigatórios e informa somente os nomes ausentes, sem imprimir valores.

Após testes aprovados, um push em `main` publica somente o script em um caminho S3
por commit/execução e atualiza o job existente. O deploy instala as dependências fixadas
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

Permissões do principal de deploy: `s3:PutObject` no prefixo `releases/` do bucket de
artefatos, `glue:GetJob`, `glue:UpdateJob` no job e `iam:PassRole` para seu papel de execução.
Para o teste manual: `glue:StartJobRun` e `glue:GetJobRun`. Políticas de bucket e KMS,
se presentes, também precisam autorizar esse acesso.

O papel do Glue precisa ler o script e a bronze, gravar/ler/apagar staging e dados silver,
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
- [Autenticação AWS no GitHub Actions, incluindo access keys](https://github.com/aws-actions/configure-aws-credentials#non-oidc-authentication-options)
