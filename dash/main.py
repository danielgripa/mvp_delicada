import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st
from datetime import datetime
import os

# Função para organizar o resumo de transferências e compras em um DataFrame
def organizar_resumo_ajuste(df_deficit_atual, df_cobertura_ok_atual, df_produto_ABC):
    resumo_ajuste = []
    produtos_compra = []

    for (produto_grade, nm_produto), df_deficit in df_deficit_atual.groupby(['nk_produto_grade', 'nm_produto']):
        df_cobertura = df_cobertura_ok_atual[
            (df_cobertura_ok_atual['nk_produto_grade'] == produto_grade) &
            (df_cobertura_ok_atual['nm_produto'] == nm_produto)
        ]

        total_deficit = df_deficit['saldo_versus_cobertura'].sum()
        total_cobertura = df_cobertura['saldo_versus_cobertura'].sum()

        if total_deficit + total_cobertura >= 0:
            doadores = dict(zip(df_cobertura['nm_entidade'], df_cobertura['saldo_versus_cobertura']))
            receptores = dict(zip(df_deficit['nm_entidade'], df_deficit['saldo_versus_cobertura']))

            doadores_str = '\n'.join([f"{entidade}: {saldo}" for entidade, saldo in doadores.items()])
            receptores_str = '\n'.join([f"{entidade}: {saldo}" for entidade, saldo in receptores.items()])

            transferencia_total = min(total_deficit, total_cobertura)

            resumo_ajuste.append({
                "id_produto": produto_grade,
                "descricao": nm_produto,
                "doadores": doadores_str,
                "receptores": receptores_str,
                "transferencia_total": transferencia_total
            })
        else:
            saldo_necessario = abs(total_deficit + total_cobertura)
            produtos_compra.append({
                "id_produto": produto_grade,
                "descricao": nm_produto,
                "saldo_necessario": saldo_necessario
            })

    df_resumo_ajuste = pd.DataFrame(resumo_ajuste)
    df_produtos_compra = pd.DataFrame(produtos_compra)

    # Adicionar Curva ABC
    df_resumo_ajuste = df_resumo_ajuste.merge(df_produto_ABC, left_on = "id_produto", right_on = "nk_produto_grade")
    df_produtos_compra = df_produtos_compra.merge(df_produto_ABC, left_on = "id_produto", right_on = "nk_produto_grade")

    # Remover a coluna nk_produto_grade pois ja existe um id_produto na tabela anterior
    df_resumo_ajuste.drop('nk_produto_grade', axis = 1, inplace = True)
    df_produtos_compra.drop('nk_produto_grade', axis = 1, inplace = True)

    # Resetar o índice de ambos os DataFrames
    df_resumo_ajuste.reset_index(drop=True, inplace=True)
    df_produtos_compra.reset_index(drop=True, inplace=True)

    return df_resumo_ajuste, df_produtos_compra

# Classificando os produtos de acordo com a Curva ABC
def classificar_curva_abc(proporcao):
    if proporcao <= 0.7:
        return 'A'
    elif proporcao <= 0.9:
        return 'B'
    else:
        return 'C'

# Dados de conexão com o banco de dados
db_uri = os.environ.get("URL")

# Criar uma engine SQLAlchemy
engine = create_engine(db_uri)

# Definir a consulta SQL
query = os.environ.get("QUERY")

# Executar a consulta e carregar o resultado em um DataFrame
with engine.connect() as conn:
    result = conn.execute(text(query))
    rows = result.fetchall()
    df = pd.DataFrame(rows, columns=result.keys())

df = df.drop_duplicates(subset=['nk_estoque'])

# Convertendo 'sk_data' para tipo datetime
df['sk_data'] = pd.to_datetime(df['sk_data'].astype(str), format='%Y%m%d')


# Obtendo a data atual e convertendo para um período mensal
ultimo_mes = pd.to_datetime(datetime.today()).to_period('M')


# Filtrando dados para o mês anterior ao último mês
df_last_month = df[df['sk_data'].dt.to_period('M') == ultimo_mes - 1]

# Agrupando por 'nk_entidade' e 'nk_produto_grade' e somando as vendas
sales_last_month = df_last_month.groupby(['nk_entidade', 'nk_produto_grade'])['venda'].sum().reset_index()
sales_last_month.columns = ['nk_entidade', 'nk_produto_grade', 'vendas_ultimo_mes']

# Mesclando de volta ao DataFrame original com base em 'nk_entidade' e 'nk_produto_grade'
df = pd.merge(df, sales_last_month, on=['nk_entidade', 'nk_produto_grade'], how='left')

# Calculando cobertura (dobro das vendas do último mês)
df['cobertura'] = 2 * df['vendas_ultimo_mes']

# Calculando o saldo versus cobertura
df['saldo_versus_cobertura'] = df['saldo'] - df['cobertura']

# Ordenando o DataFrame por vendas em ordem decrescente
df = df.sort_values(by='vendas_ultimo_mes', ascending=False)

# Calculando a proporção cumulativa das vendas
df['proporcao_vendas_cumulativa'] = df['vendas_ultimo_mes'].cumsum() / df['vendas_ultimo_mes'].sum()

df['CURVA_ABC'] = df['proporcao_vendas_cumulativa'].apply(classificar_curva_abc)

# Ordenando o DataFrame para manter apenas as linhas mais recentes de estoque de cada produto e entidade
df = df.sort_values(by=['nk_entidade', 'nk_produto_grade', 'sk_data'], ascending=[True, True, False])

# Removendo as duplicatas mais antigas ficando apenas com o estoque mais recente
df_atual = df.drop_duplicates(subset=['nk_entidade', 'nk_produto_grade'])

# Criando coluna Curva_ABC
# Ordenando o DataFrame por vendas em ordem decrescente
df_atual = df_atual.sort_values(by='vendas_ultimo_mes', ascending=False)


# Criando um dataframe de produto_ABC para calcular a curva ABC

# Agrupando por produto e somando as vendas
df_produto_ABC = df_atual.groupby('nk_produto_grade')['vendas_ultimo_mes'].sum().reset_index()
# Classificando as qtd de vendas do maior para o menor
df_produto_ABC = df_produto_ABC.sort_values(by="vendas_ultimo_mes", ascending=False)
# Calculando o acúmulo das vendas
df_produto_ABC['proporcao_vendas_cumulativa'] = df_produto_ABC['vendas_ultimo_mes'].cumsum() / df_produto_ABC['vendas_ultimo_mes'].sum() 
# Aplicando as regras para A B e C baseado nos acúmulos 70/20/10
df_produto_ABC['CURVA_ABC'] = df_produto_ABC['proporcao_vendas_cumulativa'].apply(classificar_curva_abc)
# Salvando o arquivo de produto_abc para excel
df_produto_ABC.to_excel("produto_ABC.xlsx", index=False)
# Removendo as colunas de cálculo do ABC para reintegrar a informação para o df_estoque
df_produto_ABC.drop(['proporcao_vendas_cumulativa', 'vendas_ultimo_mes'], axis=1, inplace=True)
# Adicionando a coluna CURVA_ABC em estoque por meio do merge
df_atual = df_atual.merge(df_produto_ABC, on='nk_produto_grade', how='left')


# Criando os dataframes que precisam de estoque
df_deficit_atual = df_atual[df_atual['saldo_versus_cobertura'] < 0]

# Criando os dataframes que possuem estoque em excesso
df_cobertura_ok_atual = df_atual[df_atual['saldo_versus_cobertura'] >= 0]

# Fazendo a operação que avalia quais transferências podem ser feitas ou compradas
df_resumo_ajuste, df_resumo_compra = organizar_resumo_ajuste(df_deficit_atual, df_cobertura_ok_atual, df_produto_ABC)

# Organizando as transferências do maior para o menor em quantidade, logo importância.
df_resumo_ajuste = df_resumo_ajuste.sort_values(by='transferencia_total', ascending=True)
df_resumo_compra = df_resumo_compra.sort_values(by='saldo_necessario', ascending=False)

# Salvando os arquivos em excel para possibilitar os downloads
caminho_transferencias = 'resumo_ajuste.xlsx'
caminho_compras = 'compras.xlsx'
caminho_base = 'base-geral.xlsx'

df_resumo_ajuste.to_excel(caminho_transferencias, index=False)
df_resumo_compra.to_excel(caminho_compras, index=False)
df_atual.to_excel(caminho_base, index=False)
df_deficit_atual.to_excel("base_deficit.xlsx", index = False)
df_cobertura_ok_atual.to_excel("base_excedente.xlsx", index = False)


# Parte do Streamlit
st.set_page_config(page_title="Ajustes de Estoque", layout="wide")


st.download_button(
    label="Clique aqui para exportar a avaliação completa do estoque",
    data=open(caminho_base, 'rb').read(),
    file_name="base-geral.xlsx",
    mime="text/xlsx"
)
st.title("Ajustes de Estoque")
st.dataframe(df_resumo_ajuste)

st.download_button(
    label="Baixar Resumo de Transferências em Excel",
    data=open(caminho_transferencias, 'rb').read(),
    file_name="resumo_ajuste.xlsx",
    mime="text/xlsx"
)

st.title("Recomendação de compras")
st.dataframe(df_resumo_compra.head(len(df_resumo_ajuste)))

st.download_button(
    label="Baixar Resumo de Compras em Excel",
    data=open(caminho_compras, 'rb').read(),
    file_name="compras.xlsx",
    mime="text/xlsx"
)
