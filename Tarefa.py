"""# Bibliotecas"""
import pandas as pd
import urllib.request
import json
import ckanapi

"""# Constantes"""
package_name = "relacao-de-empreendimentos-de-geracao-distribuida"
data_set = "https://dadosabertos.aneel.gov.br"

"""# Consumo de Dados """
def get_resourse_url(dataset):
  key = 'datastore_active'
  for item in dataset['resources']:
      if item[key] == True:
          result = item
  return(json.dumps(result['url'])[1:-1])

# create ckan client
ckan = ckanapi.RemoteCKAN(data_set)

# get dataset
dataset = ckan.action.package_show(id=package_name)

# get resource url
url = get_resourse_url(dataset)

# create dataframe
df = pd.read_csv(url,
                 infer_datetime_format=True,
                 encoding = "ISO-8859-1",
                 encoding_errors = "ignore",
                 on_bad_lines='warn',
                 sep=';',
                 decimal=',')

df["DthAtualizaCadastralEmpreend"] = pd.to_datetime(df["DthAtualizaCadastralEmpreend"], format='%Y-%m-%d')
df['MdaPotenciaInstaladaKW'] = pd.to_numeric(df['MdaPotenciaInstaladaKW'].fillna(0), errors='coerce')

# Agregação Mensal
df['MthAtualizaCadastralEmpreend'] = df['DthAtualizaCadastralEmpreend'].dt.strftime('%Y-%m')

"""# Verificação de Consistência dos Dados"""

""" Durante a atualização da potência instalada de um empreendimento, estou duplicando dados já existentes na base?
    Quando ocorre atualização do mesmo empreendimento na mesmo mês, ocorre duplicação.

# 2015-05 RJ Empreendimento X 150000000
# .
# .
# .
# 2023-02 RJ Empreendimento X 200000000"""
# Check  dos DADOS: Busca de duplicadas nessas 3 colunas:
#mask = df.duplicated(subset=['MthAtualizaCadastralEmpreend', 'SigUF', 'CodEmpreendimento'], keep=False)

"""# Agregar os dados em base mensal: Total de empreendimentos por classe (Residencial, comercial, “...”) e estado federativo;"""

# Agregar por mês o total de empreendimentos por classe
df['MthAtualizaCadastralEmpreend'] = df['DthAtualizaCadastralEmpreend'].dt.strftime('%Y-%m')
agg_classe = df.groupby(['SigUF','DscClasseConsumo','MthAtualizaCadastralEmpreend'])['DscClasseConsumo'].count()

"""# Agregar os dados em base mensal: Potência instalada de cada estado;"""

# Agregar por mês a potência instalada de cada estado
agg_pot = df.groupby(['SigUF','MthAtualizaCadastralEmpreend'])['MdaPotenciaInstaladaKW'].sum()

# Exportação do Agregação Mensal em CSV
agg_classe.to_csv('/content/agregacao_base_mensal_contagem_empreendimentos_classe_estado.csv')
agg_classe.to_csv('/content/agregacao_base_mensal_soma_potencia_instalada_estado.csv')

# Exportação do Agregação Mensal em JSON
agg_classe.to_json('/content/agregacao_base_mensal_contagem_empreendimentos_classe_estado.json', orient='split', lines=False, indent=4)
agg_classe.to_json('/content/agregacao_base_mensal_soma_potencia_instalada_estado.json', orient='split', lines=False, indent=4)