# pipeline_ingestao.py

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Series
import logging
from datetime import datetime

# ----------------------------------------------------------------------
# 1. Configuração e Logging
# ----------------------------------------------------------------------

# Configurar o sistema de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline_ingestao.log"), # Log de auditoria para arquivo
        logging.StreamHandler() # Saída de log para o console
    ]
)

def log_auditoria(operacao, status, detalhes=""):
    """Função para registrar logs de auditoria."""
    logging.info(f"AUDIT | {operacao} | STATUS: {status} | DETALHES: {detalhes}")

# ----------------------------------------------------------------------
# 2. Definição do Schema Validation Rigoroso (usando Pandera)
# ----------------------------------------------------------------------

# Schema para Clientes
class ClientesSchema(pa.SchemaModel):
    id_cliente: Series[int] = pa.Field(nullable=False, unique=True, ge=1)
    nome: Series[str] = pa.Field(nullable=False)
    email: Series[str] = pa.Field(nullable=False, unique=True, regex=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    estado: Series[str] = pa.Field(nullable=False, str_length=2)
    # Garante que não haja colunas extras que não estejam no schema
    class Config:
        strict = True 
        name = "ClientesSchema"

# Schema para Produtos
class ProdutosSchema(pa.SchemaModel):
    id_produto: Series[int] = pa.Field(nullable=False, unique=True, ge=100)
    nome_produto: Series[str] = pa.Field(nullable=False)
    preco: Series[float] = pa.Field(nullable=False, gt=0) # Preço deve ser maior que zero
    categoria: Series[str] = pa.Field(nullable=False, isin=['Eletrônico', 'Livro', 'Vestuário', 'Móvel', 'Acessório'])
    class Config:
        strict = True
        name = "ProdutosSchema"

# Schema para Vendas
class VendasSchema(pa.SchemaModel):
    id_venda: Series[int] = pa.Field(nullable=False, unique=True)
    id_cliente: Series[int] = pa.Field(nullable=False) # Será validado integridade referencial separadamente
    id_produto: Series[int] = pa.Field(nullable=False)
    quantidade: Series[int] = pa.Field(nullable=False, ge=1) # Quantidade deve ser >= 1
    valor_unitario: Series[float] = pa.Field(nullable=False, gt=0)
    valor_total: Series[float] = pa.Field(nullable=False, gt=0)
    data_venda: Series[datetime] = pa.Field(nullable=False)
    status: Series[str] = pa.Field(nullable=False, isin=['Concluída', 'Pendente', 'Cancelada'])
    class Config:
        strict = True
        name = "VendasSchema"

# Mapeamento de arquivos para Schemas
DATASET_MAP = {
    'clientes.csv': {'schema': ClientesSchema, 'name': 'Clientes'},
    'produtos.csv': {'schema': ProdutosSchema, 'name': 'Produtos'},
    'vendas.csv': {'schema': VendasSchema, 'name': 'Vendas'},
}

# ----------------------------------------------------------------------
# 3. Pipeline de Ingestão com Tratamento de Erros e Schema Validation
# ----------------------------------------------------------------------

def executar_pipeline_ingestao(file_paths: list[str]) -> dict:
    """
    Carrega dados de múltiplas fontes, aplica schema validation e trata erros.
    
    Args:
        file_paths: Lista de caminhos para os arquivos CSV.

    Returns:
        Um dicionário com os DataFrames válidos e DataFrames rejeitados.
    """
    dataframes_validos = {}
    dataframes_rejeitados = {}
    
    log_auditoria("INICIO_PIPELINE", "SUCESSO", f"Iniciando ingestão de {len(file_paths)} arquivos.")

    for file_path in file_paths:
        file_name = file_path.split('/')[-1]
        
        if file_name not in DATASET_MAP:
            log_auditoria("CARGA", "FALHA", f"Arquivo {file_name} não mapeado para um schema.")
            continue
            
        config = DATASET_MAP[file_name]
        schema = config['schema']
        dataset_name = config['name']
        
        log_auditoria("CARGA", "INFO", f"Tentando carregar dados do dataset: {dataset_name}")

        try:
            # Carrega o arquivo
            df = pd.read_csv(file_path)
            
            # Converte data_venda para datetime ANTES da validação se for o DF de Vendas
            if dataset_name == 'Vendas':
                df['data_venda'] = pd.to_datetime(df['data_venda'], errors='coerce')
                
            log_auditoria("CARGA", "SUCESSO", f"Dados de {dataset_name} carregados ({len(df)} registros).")
            
            # Aplica Schema Validation (Testes de Schema Automatizados)
            # errors='filter' instrui o Pandera a retornar apenas os dados válidos
            try:
                # O validate(lazy=True) tenta aplicar todas as validações de uma vez
                df_validado = schema.validate(df, lazy=True)
                
                # df_validado (se 'lazy=True' não for usado na validação que retorna o df)
                # Para fins práticos de ingestão, usamos .validate para obter o DF final
                
                # Se usarmos .validate com o try/except, ele lança a exceção e tratamos tudo como falha de validação
                df_validado = schema.validate(df)
                dataframes_validos[dataset_name] = df_validado
                log_auditoria("VALIDACAO_SCHEMA", "SUCESSO", f"Schema de {dataset_name} OK. {len(df_validado)} registros válidos.")
            
            except pa.errors.SchemaErrors as err:
                # Tratar Erros de Formato e Dados Corrompidos (rejeição no lote)
                log_auditoria("VALIDACAO_SCHEMA", "FALHA", f"Falha de Schema em {dataset_name}. {len(err.failure_cases)} violações.")
                
                # Captura os dados que falharam na validação (dados corrompidos)
                df_rejeitado = df.iloc[err.failure_cases.index]
                dataframes_rejeitados[dataset_name] = {'data': df_rejeitado, 'erros': err.failure_cases}
                
                # Tenta obter e salvar o DataFrame válido que passou na validação (se houver)
                try:
                    df_validado = schema.validate(df, lazy=False) # Tenta novamente, sem lazy, para capturar o válido
                    dataframes_validos[dataset_name] = df_validado
                except pa.errors.SchemaErrors as e:
                     log_auditoria("VALIDACAO_SCHEMA", "ALERTA", f"Nenhum dado válido pôde ser extraído de {dataset_name} após falha de Schema.")


        except Exception as e:
            # Trata erros genéricos (e.g., arquivo corrompido, erro de codificação)
            log_auditoria("CARGA_GENERICA", "ERRO", f"Erro inesperado ao processar {file_name}: {e}")
            dataframes_rejeitados[dataset_name] = {'data': None, 'erros': str(e)}

    log_auditoria("FIM_PIPELINE", "SUCESSO", "Processamento da ingestão concluído.")
    return dataframes_validos, dataframes_rejeitados

# ----------------------------------------------------------------------
# 4. Execução Principal (Simulação)
# ----------------------------------------------------------------------

if __name__ == "__main__":
    # Simulação da execução com seus arquivos
    # (ATENÇÃO: Crie arquivos com estes nomes ou ajuste a lista)
    arquivos_para_processar = ['clientes.csv', 'produtos.csv', 'vendas.csv', 'logistica.csv'] 
    
    # 🚨 DICA: Crie arquivos CSV de teste para ver o tratamento de erros em ação!
    
    validos, rejeitados = executar_pipeline_ingestao(arquivos_para_processar)
    
    print("\n===========================================")
    print("RESUMO DA INGESTÃO:")
    print("===========================================")
    
    # Exibir DataFrames Válidos
    print("\n✅ DataFrames VÁLIDOS (Prontos para Processamento/ETL):")
    for name, df in validos.items():
        print(f"- {name}: {len(df)} registros.")
        # Simulação de armazenamento (e.g., salvar no Data Lake/Stage Area)
        # df.to_parquet(f'data_stage/{name.lower()}.parquet')

    # Exibir DataFrames Rejeitados
    if rejeitados:
        print("\n❌ DataFrames REJEITADOS (Dados Corrompidos ou Inválidos):")
        for name, info in rejeitados.items():
            if info.get('data') is not None:
                print(f"- {name}: {len(info['data'])} registros rejeitados. Detalhe do Erro: {info['erros'].head(3)}...")
            else:
                print(f"- {name}: Falha geral na carga. Erro: {info['erros']}")
                
        print("\nRecomenda-se mover os dados rejeitados para uma 'quarantine zone' para análise e correção.")
    else:
        print("\nNenhum dado foi rejeitado na validação de schema.")