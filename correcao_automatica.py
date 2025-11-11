# correcao_automatica.py

import pandas as pd
import numpy as np
import re
from datetime import datetime
from typing import Tuple, Dict

# ----------------------------------------------------------------------
# DADOS DE ENTRADA (Os DataFrames de Exemplo Problemáticos da Análise)
# ----------------------------------------------------------------------

# NOTE: Estes DataFrames contêm os problemas originais para testar a correção.
df_clientes = pd.DataFrame({
    'id_cliente': [1, 2, np.nan, 4, 1, 6],  # nulo, duplicado (1)
    'nome': ['Ana', 'Bruno', 'Carlos', 'Diana', 'Ana', 'Eva'],
    'email': ['a@a.com', 'b@b.com', 'c@c.com', 'd@d.com', 'a@a.com', 'e@e.com'], # duplicado (a@a.com)
    'estado': ['SP', 'RJ', 'SP', 'XX', 'RJ', 'MG'] # inválido ('XX')
})
df_produtos = pd.DataFrame({
    'id_produto': [101, 102, 103, 104, 105],
    'nome_produto': ['Laptop', np.nan, 'Mouse', 'Teclado', 'Cadeira'], # nulo
    'preco': [1000.50, -50.00, 20.00, 100.00, 300.00], # inválido (-50.00)
    'categoria': ['Eletrônico', 'Livro', 'Eletrônico', 'Roupa', 'Móvel'] # inválido ('Roupa')
})
df_vendas = pd.DataFrame({
    'id_venda': [1001, 1002, 1003, 1004, 1005, 1006, 1007],
    'id_cliente': [1, 2, 999, 4, 1, 6, 777],  # chaves órfãs (999, 777)
    'id_produto': [101, 102, 103, 105, 101, 999, 104], # chave órfã (999)
    'quantidade': [1, 0, 3, 2, 1, 5, 1], # inválido (0)
    'valor_unitario': [1000.5, 50.0, 20.0, 150.0, 1000.5, 10.0, 100.0],
    'valor_total': [1000.5, 0.0, 60.0, 300.0, 1000.5, 50.0, 100.0],
    'data_venda': ['2023-01-10', '2023-10-01', '2023-12-31', '2025-01-01', '2023-01-10', '2023-05-20', '2023-01-10'],
    'status': ['Concluída', 'Concluída', 'Pendente', 'Cancelada', 'Concluída', 'Concluída', 'Concluída']
})


def padronizar_formatos_e_preencher_nulos(df_c, df_p, df_v):
    """Padroniza formatos de dados e preenche campos vazios (Completude)."""

    print("\n--- 1. Padronização e Preenchimento ---")
    
    # 1.1 Clientes
    # Preencher campos vazios: (Simular preenchimento de nulos com valor padrão ou regra)
    df_c['id_cliente'] = df_c['id_cliente'].fillna(0).astype(int) # Temporariamente preenche nulos com 0 para converter para int
    df_c['nome'] = df_c['nome'].fillna("Nome_Desconhecido")
    
    # Padronizar Estado: (Exemplo: Consistência)
    # Valores inválidos ('XX') serão mapeados para 'N/A' ou removidos. Aqui vamos para 'N/A'.
    df_c['estado'] = df_c['estado'].apply(lambda x: x if len(str(x)) == 2 else 'NA')
    print(f"Clientes: Estados não padrão corrigidos/marcados: {len(df_c[df_c['estado'] == 'NA'])}")

    # 1.2 Produtos
    # Preencher campos vazios: (Completude)
    df_p['nome_produto'] = df_p['nome_produto'].fillna("Produto_Sem_Nome")
    
    # Corrigir Validade: Preço negativo (regra de negócio)
    df_p['preco'] = df_p['preco'].apply(lambda x: 0.01 if x <= 0 else x)
    print(f"Produtos: Preços <= 0 corrigidos para 0.01.")

    # 1.3 Vendas
    # Padronizar Datas:
    df_v['data_venda'] = pd.to_datetime(df_v['data_venda'], errors='coerce')
    
    # Corrigir Validade: Data futura (regra de negócio)
    hoje = pd.to_datetime(datetime.now().date())
    registros_futuros = df_v[df_v['data_venda'] > hoje]
    df_v.loc[registros_futuros.index, 'data_venda'] = hoje # Altera data futura para a data de hoje
    print(f"Vendas: {len(registros_futuros)} datas futuras corrigidas para a data de hoje.")
    
    # Corrigir Validade: Quantidade zero (regra de negócio - venda deve ter min 1 unidade)
    df_v['quantidade'] = df_v['quantidade'].apply(lambda x: 1 if x == 0 else x)
    print("Vendas: Quantidades zero corrigidas para 1.")
    
    return df_c, df_p, df_v

def remover_duplicatas_com_logica(df_c, df_p, df_v) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Remove duplicatas com lógica inteligente (Unicidade)."""
    
    print("\n--- 2. Remoção de Duplicatas ---")

    # 2.1 Clientes: Priorizar a manutenção do registro mais completo ou mais recente.
    # Neste caso, vamos priorizar o id_cliente *válido* mais baixo (keep='first' após ordenação).
    
    # Etapa 1: Tratar duplicatas de id_cliente (caso mais crítico)
    df_c_dups = df_c[df_c['id_cliente'].duplicated(keep=False)]
    df_c_limpo = df_c.drop_duplicates(subset=['id_cliente'], keep='first')
    df_c_limpo = df_c_limpo[df_c_limpo['id_cliente'] != 0] # Remove registros que tiveram id nulo e foram preenchidos com 0

    print(f"Clientes: Registros originais: {len(df_clientes)}. Duplicatas/Nulos removidos: {len(df_clientes) - len(df_c_limpo)}.")
    
    # Etapa 2: Tratar duplicatas de e-mail (chave secundária importante)
    duplicatas_email_count = df_c_limpo.duplicated(subset=['email']).sum()
    df_c_limpo = df_c_limpo.drop_duplicates(subset=['email'], keep='first')
    print(f"Clientes: E-mails duplicados removidos: {duplicatas_email_count}.")

    # 2.2 Produtos e Vendas: Assumimos que a unicidade da PK (id_produto, id_venda) é tratada pelo Schema Validation.
    # Apenas garantimos que não haja duplicação de linha completa.
    
    # Produtos
    dups_p = df_p.duplicated().sum()
    df_p_limpo = df_p.drop_duplicates()
    print(f"Produtos: Duplicatas de linha completa removidas: {dups_p}.")

    # Vendas
    dups_v = df_v.duplicated().sum()
    df_v_limpo = df_v.drop_duplicates()
    print(f"Vendas: Duplicatas de linha completa removidas: {dups_v}.")

    return df_c_limpo.reset_index(drop=True), df_p_limpo.reset_index(drop=True), df_v_limpo.reset_index(drop=True)

def corrigir_e_validar_relacionamentos(df_c, df_p, df_v) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Corrige inconsistências e valida relacionamentos entre datasets (Integridade/Consistência)."""
    
    print("\n--- 3. Correção de Inconsistências e FKs ---")

    # 3.1 Integridade Referencial (FKs) em Vendas
    
    clientes_validos_id = df_c['id_cliente'].unique()
    produtos_validos_id = df_p['id_produto'].unique()
    
    # Corrigir Vendas com id_cliente órfão
    vendas_clientes_invalidos = df_v[~df_v['id_cliente'].isin(clientes_validos_id)]
    
    # Regra de Correção: Descartar vendas sem um cliente válido (crítico)
    df_v_limpo = df_v[df_v['id_cliente'].isin(clientes_validos_id)]
    print(f"Vendas: {len(vendas_clientes_invalidos)} registros descartados por FK inválida (id_cliente).")

    # Corrigir Vendas com id_produto órfão
    vendas_produtos_invalidos = df_v_limpo[~df_v_limpo['id_produto'].isin(produtos_validos_id)]
    
    # Regra de Correção: Descartar vendas sem um produto válido
    df_v_limpo = df_v_limpo[df_v_limpo['id_produto'].isin(produtos_validos_id)]
    print(f"Vendas: {len(vendas_produtos_invalidos)} registros descartados por FK inválida (id_produto).")
    
    # 3.2 Corrigir Inconsistência de Regra de Negócio: valor_total
    
    # Recalcula o valor_total e corrige o campo
    df_v_limpo['valor_calculado'] = df_v_limpo['quantidade'] * df_v_limpo['valor_unitario']
    inconsistentes_valor = df_v_limpo[np.abs(df_v_limpo['valor_total'] - df_v_limpo['valor_calculado']) > 0.01]
    
    # Regra de Correção: Sobrescrever valor_total com o valor calculado
    df_v_limpo['valor_total'] = df_v_limpo['valor_calculado']
    df_v_limpo = df_v_limpo.drop(columns=['valor_calculado'])
    print(f"Vendas: {len(inconsistentes_valor)} valores totais inconsistentes corrigidos por recálculo.")
    
    return df_c, df_p, df_v_limpo.reset_index(drop=True)


if __name__ == "__main__":
    
    print("==================================================")
    print("🚀 INÍCIO DO SISTEMA DE CORREÇÃO AUTOMÁTICA")
    print("==================================================")
    
    df_c_corr, df_p_corr, df_v_corr = df_clientes.copy(), df_produtos.copy(), df_vendas.copy()
    
    # PASSO 1: Padronização e Preenchimento
    df_c_corr, df_p_corr, df_v_corr = padronizar_formatos_e_preencher_nulos(df_c_corr, df_p_corr, df_v_corr)
    
    # PASSO 2: Remoção de Duplicatas
    df_c_corr, df_p_corr, df_v_corr = remover_duplicatas_com_logica(df_c_corr, df_p_corr, df_v_corr)

    # PASSO 3: Correção de Inconsistências e FKs
    df_c_final, df_p_final, df_v_final = corrigir_e_validar_relacionamentos(df_c_corr, df_p_corr, df_v_corr)

    print("\n==================================================")
    print("✅ FIM DA CORREÇÃO AUTOMÁTICA")
    print("==================================================")

    print("\n📋 RESUMO FINAL:")
    print(f"Clientes (Origem: {len(df_clientes)} -> Final: {len(df_c_final)})")
    print(f"Produtos (Origem: {len(df_produtos)} -> Final: {len(df_p_final)})")
    print(f"Vendas (Origem: {len(df_vendas)} -> Final: {len(df_v_final)})")

    print("\n--- Amostra do Cliente Final ---")
    print(df_c_final)
    print("\n--- Amostra da Venda Final ---")
    print(df_v_final)