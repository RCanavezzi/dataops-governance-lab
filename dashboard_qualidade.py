# dashboard_qualidade.py

import os
import pandas as pd
from datetime import datetime
from great_expectations.data_context import FileDataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import Checkpoint

# --- Simulação de Carregamento dos Dados Limpos (Saída do Pipeline) ---
# Usamos dados de exemplo que PASSAM na maioria das Expectativas
df_clientes_limpo = pd.DataFrame({
    'id_cliente': [1, 2, 3], 'nome': ['Ana', 'Bruno', 'Carlos'], 'email': ['a@a.com', 'b@b.com', 'c@c.com'],
    'telefone': ['911111111', '922222222', '933333333'], 'estado': ['SP', 'RJ', 'MG']
})
df_produtos_limpo = pd.DataFrame({
    'id_produto': [101, 102, 103], 'nome_produto': ['Laptop', 'Mouse', 'Teclado'],
    'categoria': ['Eletrônico', 'Acessório', 'Acessório'], 'preco': [1000.50, 20.00, 100.00], 'estoque': [10, 50, 30]
})
df_vendas_limpo = pd.DataFrame({
    'id_venda': [1001, 1002], 'id_cliente': [1, 2], 'id_produto': [101, 102], 'quantidade': [1, 2],
    'valor_unitario': [1000.5, 50.0], 'valor_total': [1000.5, 100.0], 'data_venda': ['2023-01-10', '2023-10-01'], 'status': ['Concluída', 'Pendente']
})

# --- Configuração de Caminhos ---
GE_ROOT_DIR = "great_expectations"
DATA_STAGE_DIR = os.path.join(GE_ROOT_DIR, "data_stage")


def configurar_ambiente_e_checkpoint():
    """Inicializa Data Context, cria DataSources e define Checkpoint."""
    
    print("--- 1. Configurando Great Expectations (Modo Arquivo) ---")
    
    # Simula a criação do ambiente de contexto se ele não existir
    if not os.path.isdir(GE_ROOT_DIR):
        print(f"ATENÇÃO: Criando diretório raiz do GE em '{GE_ROOT_DIR}'...")
        os.makedirs(GE_ROOT_DIR, exist_ok=True)
        # Em um setup real, você executaria 'great_expectations init'

    # 1.1 Conecta ao Data Context existente
    context = FileDataContext(context_root_dir=GE_ROOT_DIR)
    
    # 1.2 Cria a pasta para simular o Stage Area (onde os dados limpos estariam)
    os.makedirs(DATA_STAGE_DIR, exist_ok=True)
    df_clientes_limpo.to_csv(os.path.join(DATA_STAGE_DIR, "clientes_stage.csv"), index=False)
    df_produtos_limpo.to_csv(os.path.join(DATA_STAGE_DIR, "produtos_stage.csv"), index=False)
    df_vendas_limpo.to_csv(os.path.join(DATA_STAGE_DIR, "vendas_stage.csv"), index=False)
    print(f"Dados salvos em '{DATA_STAGE_DIR}' para validação.")
    
    # 1.3 Adiciona File System Datasource (Se ainda não estiver configurado)
    # Nota: Este bloco assume que você configurou o Datasource via CLI ou API,
    # caso contrário, precisa adicionar a configuração completa aqui.
    try:
        context.get_datasource("techcommerce_data")
    except LookupError:
        print("Adicionando Datasource 'techcommerce_data'...")
        context.sources.add_pandas("techcommerce_data")

    # 1.4 Configura o Checkpoint para integrar todas as Expectations Suites
    checkpoint_name = "daily_quality_check"
    
    try:
        context.get_checkpoint(checkpoint_name)
        print(f"Checkpoint '{checkpoint_name}' já existe.")
    except LookupError:
        print(f"Criando Checkpoint '{checkpoint_name}'...")
        
        # O Checkpoint define quais Expectation Suites serão validadas
        checkpoint_config = {
            "name": checkpoint_name,
            "config_version": 1,
            "class_name": "SimpleCheckpoint",
            "batch_request": {},
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "techcommerce_data",
                        "data_asset_name": os.path.join(DATA_STAGE_DIR, "clientes_stage.csv"),
                    },
                    "expectation_suite_name": "clientes_suite"
                },
                {
                    "batch_request": {
                        "datasource_name": "techcommerce_data",
                        "data_asset_name": os.path.join(DATA_STAGE_DIR, "produtos_stage.csv"),
                    },
                    "expectation_suite_name": "produtos_suite"
                },
                {
                    "batch_request": {
                        "datasource_name": "techcommerce_data",
                        "data_asset_name": os.path.join(DATA_STAGE_DIR, "vendas_stage.csv"),
                    },
                    "expectation_suite_name": "vendas_suite"
                },
            ],
            # 1.5 Configurar Data Docs: Gera relatórios HTML automaticamente
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
                },
            ],
        }
        context.add_checkpoint(**checkpoint_config)
    
    return context, checkpoint_name


def gerar_data_docs_e_dashboard(context: FileDataContext, checkpoint_name: str):
    """Executa o Checkpoint e gera os Data Docs e dashboard."""
    
    print("\n--- 2. Executando Validação e Gerando Data Docs ---")
    
    # Executa o Checkpoint
    checkpoint_result = context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        batch_request={}
    )
    
    if checkpoint_result.success:
        print("✅ Validação do Checkpoint concluída com SUCESSO. Todas as expectativas PASSARAM.")
    else:
        print("⚠️ Validação do Checkpoint concluída com FALHAS. Verifique os Data Docs para detalhes.")

    # Gera e Abre os Data Docs
    # Esta função garante que o HTML seja gerado e atualizado
    context.build_data_docs()
    
    # Abre o Data Docs no navegador (o ponto focal do "Dashboard")
    context.open_data_docs() 
    
    print("\n--- 3. Relatórios Gerados ---")
    data_docs_path = os.path.join(GE_ROOT_DIR, "uncommitted/data_docs/local_site/index.html")
    print(f"✅ Data Docs (Dashboard) gerado em: {data_docs_path}")
    
    # 4. Exportar relatórios executivos em PDF (Simulação)
    # A exportação direta para PDF não é nativa do GE. Isso exige bibliotecas externas (como weasyprint).
    # Para o desafio, simularíamos o processo:
    print("--- 4. Exportação para PDF (Simulação) ---")
    print("Requisito: PDF exige conversão de HTML. Arquivo 'relatorio_executivo.pdf' simulado com sucesso.")

if __name__ == "__main__":
    
    # Para este script funcionar, você precisaria de uma pasta 'great_expectations'
    # e das 'Expectation Suites' salvas (como a saída da Parte 2.2).
    
    # Etapa 1: Configurar e Adicionar Checkpoint
    ge_context, cp_name = configurar_ambiente_e_checkpoint()
    
    # Etapa 2: Gerar Data Docs e Dashboard
    gerar_data_docs_e_dashboard(ge_context, cp_name)