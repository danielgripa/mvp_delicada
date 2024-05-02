import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st
import os

# Função para organizar o resumo de transferências e compras em um DataFrame
def organizar_resumo_ajuste(df_deficit_atual, df_cobertura_ok_atual):
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
                "produto": f"{produto_grade} - {nm_produto}",
                "doadores": doadores_str,
                "receptores": receptores_str,
                "transferencia_total": transferencia_total
            })
        else:
            saldo_necessario = abs(total_deficit + total_cobertura)
            produtos_compra.append({
                "produto": f"{produto_grade} - {nm_produto}",
                "saldo_necessario": saldo_necessario
            })

    df_resumo_ajuste = pd.DataFrame(resumo_ajuste)
    df_produtos_compra = pd.DataFrame(produtos_compra)

    return df_resumo_ajuste, df_produtos_compra

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

df['sk_data'] = pd.to_datetime(df['sk_data'].astype(str), format='%Y%m%d')
df_last_month = df[df['sk_data'].dt.month == (df['sk_data'].dt.month.max() - 1)]
sales_last_month = df_last_month.groupby(['nk_entidade', 'nk_produto_grade'])['venda'].sum().reset_index()
sales_last_month.columns = ['nk_entidade', 'nk_produto_grade', 'vendas_ultimo_mes']
df = pd.merge(df, sales_last_month, on=['nk_entidade', 'nk_produto_grade'], how='left')
df['cobertura'] = 2 * df['vendas_ultimo_mes']
df['saldo_versus_cobertura'] = df['saldo'] - (2 * df['vendas_ultimo_mes'])
df = df.sort_values(by=['nk_entidade', 'nk_produto_grade', 'sk_data'], ascending=[True, True, False])
df_atual = df.drop_duplicates(subset=['nk_entidade', 'nk_produto_grade'])
df_deficit_atual = df_atual[df_atual['saldo_versus_cobertura'] < 0]
df_cobertura_ok_atual = df_atual[df_atual['saldo_versus_cobertura'] >= 0]

df_resumo_ajuste, df_resumo_compra = organizar_resumo_ajuste(df_deficit_atual, df_cobertura_ok_atual)
df_resumo_ajuste = df_resumo_ajuste.sort_values(by='transferencia_total', ascending=True)
df_resumo_compra = df_resumo_compra.sort_values(by='saldo_necessario', ascending=False)

caminho_transferencias = 'resumo_ajuste.csv'
caminho_compras = 'compras.csv'

df_resumo_ajuste.to_csv(caminho_transferencias, index=False)
df_resumo_compra.to_csv(caminho_compras)

st.set_page_config(page_title="Ajustes de Estoque", layout="wide")

st.title("Ajustes de Estoque")
st.dataframe(df_resumo_ajuste)

st.download_button(
    label="Baixar Resumo de Transferências em CSV",
    data=open(caminho_transferencias, 'rb').read(),
    file_name="resumo_ajuste.csv",
    mime="text/csv"
)

st.title("Recomendação de compras")
st.dataframe(df_resumo_compra.head(len(df_resumo_ajuste)))

st.download_button(
    label="Baixar Resumo de Compras em CSV",
    data=open(caminho_compras, 'rb').read(),
    file_name="compras.csv",
    mime="text/csv"
)
