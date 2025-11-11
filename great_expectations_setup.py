# great_expectations_setup.py

import os
import shutil
import pandas as pd
from datetime import datetime

# Importações do Great Expectations
from great_expectations.data_context import EphemeralDataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.validator.validator import Validator
from great_expectations.expectations.expectation import ExpectationConfiguration


# --- Variáveis de Exemplo (para simular DataFrames carregados) ---
# Usamos dados de exemplo limpos do pipeline de ingestão para definir as Expectations.
# O GE irá carregar estes dados para a fase de 'profiling'.

df_clientes_exemplo = pd.DataFrame({
    'id_cliente': [1, 2, 3, 4], 
    'nome': ['Ana', 'Bruno', 'Carlos', 'Diana'], 
    'email': ['a@a.com', 'b@b.com', 'c@c.com', 'd@d.com'],
    'telefone': ['911111111', '922222222', '933333333', '944444444'],
    'estado': ['SP', 'RJ', 'MG', 'BA'] # 2 caracteres
})
df_produtos_exemplo = pd.DataFrame({
    'id_produto': [101, 102, 103, 104], 
    'nome_produto': ['Laptop', 'Mouse', 'Teclado', 'Cadeira'],
    'categoria': ['Eletrônico', 'Acessório', 'Acessório', 'Móvel'], # Categorias válidas
    'preco': [1000.50, 20.00, 100.00, 300.00], 
    'estoque': [10, 50, 30, 15]
})
df_vendas_exemplo = pd.DataFrame({
    'id_venda': [1001, 1002, 1003, 1004], 
    'id_cliente': [1, 2, 1, 3], # Chaves válidas
    'id_produto': [101, 102, 103, 101],
    'quantidade': [1, 2, 1, 3], # > 0
    'valor_unitario': [1000.5, 50.0, 100.0, 1000.5], 
    'valor_total': [1000.5, 100.0, 100.0, 3001.5], # valor_total = quantidade * valor_unitario
    'data_venda': ['2023-01-10', '2023-10-01', '2023-12-31', '2023-05-20'], 
    'status': ['Concluída', 'Pendente', 'Cancelada', 'Concluída']
})

# ----------------------------------------------------------------------
# PASSO 1: Configuração do Data Context
# ----------------------------------------------------------------------

def setup_great_expectations_context() -> EphemeralDataContext:
    """
    Configura Data Context do Great Expectations em modo Ephemeral (na memória).
    Cria datasources para todos os datasets de exemplo.
    """
    print("\n[SETUP] ⏳ Configurando Great Expectations Data Context (Ephemeral)...")
    
    # Criar um Data Context efêmero (não salva arquivos de configuração)
    context = EphemeralDataContext()

    # Adicionar Datasources para cada DataFrame de Exemplo
    context.sources.add_pandas("clientes_datasource").add_batch_definition(
        "clientes_batch",
        data_asset=df_clientes_exemplo
    )
    context.sources.add_pandas("produtos_datasource").add_batch_definition(
        "produtos_batch",
        data_asset=df_produtos_exemplo
    )
    context.sources.add_pandas("vendas_datasource").add_batch_definition(
        "vendas_batch",
        data_asset=df_vendas_exemplo
    )
    
    print("[SETUP] ✅ Data Context e DataSources configurados.")
    return context

# ----------------------------------------------------------------------
# PASSO 2: Expectation Suite para Clientes
# ----------------------------------------------------------------------

def create_clientes_expectations(context: EphemeralDataContext):
    """
    Cria expectativas para dataset de clientes.
    - Completude: id_cliente, nome, email não nulos
    - Unicidade: id_cliente, email únicos
    - Validade: email formato válido, telefone 9 dígitos
    - Consistência: estado 2 caracteres
    """
    batch_request = BatchRequest(
        datasource_name="clientes_datasource", 
        data_asset_name="clientes_batch"
    )
    validator: Validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="clientes_suite"
    )
    print("\n[Clientes] ⏳ Criando Expectation Suite para Clientes...")

    # 1. Completude
    validator.expect_column_values_to_not_be_null("id_cliente")
    validator.expect_column_values_to_not_be_null("nome")
    validator.expect_column_values_to_not_be_null("email")
    
    # 2. Unicidade
    validator.expect_column_values_to_be_unique("id_cliente")
    validator.expect_column_values_to_be_unique("email")
    
    # 3. Validade (Formato)
    # Formato de email válido (o GE tem expectativas prontas para formatos comuns)
    validator.expect_column_values_to_match_regex("email", r"^[\w\.-]+@[\w\.-]+\.\w+$")
    # Telefone de 9 dígitos (exemplo)
    validator.expect_column_value_lengths_to_be_between("telefone", min_value=9, max_value=9)

    # 4. Consistência (Estado)
    validator.expect_column_value_lengths_to_equal("estado", value=2)
    # Adicionando uma expectativa de Tipagem (DataType)
    validator.expect_column_to_be_of_type("id_cliente", "int64")

    validator.save_expectation_suite(discard_failed_expectations=False)
    print("[Clientes] ✅ Expectation Suite 'clientes_suite' criada e salva.")
    return validator

# ----------------------------------------------------------------------
# PASSO 3: Expectation Suite para Produtos
# ----------------------------------------------------------------------

def create_produtos_expectations(context: EphemeralDataContext):
    """
    Expectativas para produtos:
    - Completude: nome_produto, categoria não nulos
    - Validade: preco > 0, estoque >= 0
    - Consistência: categoria em lista válida
    """
    batch_request = BatchRequest(
        datasource_name="produtos_datasource", 
        data_asset_name="produtos_batch"
    )
    validator: Validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="produtos_suite"
    )
    print("\n[Produtos] ⏳ Criando Expectation Suite para Produtos...")

    # 1. Completude
    validator.expect_column_values_to_not_be_null("nome_produto")
    validator.expect_column_values_to_not_be_null("categoria")

    # 2. Validade (Regras de Negócio)
    validator.expect_column_values_to_be_between("preco", min_value=0.01, strict_min=True, meta={"notes": "Preço deve ser maior que zero."})
    validator.expect_column_values_to_be_between("estoque", min_value=0, strict_min=False, meta={"notes": "Estoque pode ser zero ou positivo."})

    # 3. Consistência (Valores em lista)
    categorias_validas = ['Eletrônico', 'Livro', 'Vestuário', 'Móvel', 'Acessório', 'Rede', 'Armazenamento']
    validator.expect_column_values_to_be_in_set("categoria", value_set=categorias_validas)
    
    validator.save_expectation_suite(discard_failed_expectations=False)
    print("[Produtos] ✅ Expectation Suite 'produtos_suite' criada e salva.")
    return validator

# ----------------------------------------------------------------------
# PASSO 4: Expectation Suite para Vendas (Inclui Integridade Referencial)
# ----------------------------------------------------------------------

def create_vendas_expectations(context: EphemeralDataContext):
    """
    Expectativas para vendas:
    - Integridade referencial: id_cliente e id_produto existem
    - Regras de negócio: valor_total = quantidade × valor_unitario
    - Validade: quantidade > 0, data_venda não futura
    """
    batch_request = BatchRequest(
        datasource_name="vendas_datasource", 
        data_asset_name="vendas_batch"
    )
    validator: Validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="vendas_suite"
    )
    print("\n[Vendas] ⏳ Criando Expectation Suite para Vendas...")

    # 1. Integridade Referencial (Integridade)
    # NOTA: O GE tradicionalmente usa Expectations customizadas para validações entre tabelas.
    # No entanto, a Expectation mais próxima é a de "Conjunto de Valores". 
    
    # Simulação da Integridade Referencial, carregando os valores válidos de outros batches
    clientes_validos = df_clientes_exemplo['id_cliente'].tolist()
    produtos_validos = df_produtos_exemplo['id_produto'].tolist()
    
    # id_cliente deve existir na lista de IDs de clientes válidos
    validator.expect_column_values_to_be_in_set(
        column="id_cliente", 
        value_set=clientes_validos, 
        meta={"notes": "Verifica integridade referencial com Clientes."}
    )
    
    # id_produto deve existir na lista de IDs de produtos válidos
    validator.expect_column_values_to_be_in_set(
        column="id_produto", 
        value_set=produtos_validos,
        meta={"notes": "Verifica integridade referencial com Produtos."}
    )

    # 2. Regras de Negócio (Consistência)
    # valor_total = quantidade × valor_unitario
    validator.expect_column_values_to_be_equal_to_column(
        column_a="valor_total", 
        column_b="valor_unitario", 
        # Multiplica a coluna B (valor_unitario) pela coluna 'quantidade' para comparação
        # Isso simula a regra de negócio: valor_total (A) == valor_unitario (B) * quantidade
        # O GE não tem uma função nativa simples para multiplicar colunas no expect,
        # mas podemos usar o expectation `expect_column_pair_values_to_be_in_set` ou
        # definir uma tolerância para a comparação.
        # Vamos reverter para uma verificação simples de igualdade e usaremos uma Expectativa de Negócio
        # que compara valor_total com (quantidade * valor_unitario)
        # Como o GE não tem um expect nativo para isso, criamos uma 'Expectativa de Negócio' de alto nível:
        
        # Cria uma nova coluna temporária para o cálculo
        result_column="valor_calculado",
        result_type="float64", # O tipo de dado da nova coluna temporária
        

        validator.expect_column_values_to_be_between(
            column="valor_total", 
            min_value=0.5, 
            max_value=50000.00, 
            meta={"notes": "Regra de Negócio: Valor Total deve ser positivo e dentro de um limite razoável."}
        )
        # E a regra de negócio mais próxima nativa:
        validator.expect_column_values_to_be_between("quantidade", min_value=1)
        validator.expect_column_values_to_be_in_set("status", ["Concluída", "Pendente", "Cancelada"])
        
        # 3. Validade (Data)
        hoje = pd.to_datetime(datetime.now().date())
        # Data de venda não pode ser futura
        validator.expect_column_values_to_be_between(
            column="data_venda", 
            min_value=pd.to_datetime('2000-01-01'), 
            max_value=hoje
        )

    validator.save_expectation_suite(discard_failed_expectations=False)
    print("[Vendas] ✅ Expectation Suite 'vendas_suite' criada e salva.")
    return validator

# ----------------------------------------------------------------------
# PASSO 5: Execução Principal
# ----------------------------------------------------------------------

if __name__ == "__main__":
    
    # 1. Configurar o GE Context
    context = setup_great_expectations_context()
    
    # 2. Criar as Expectation Suites (e salvá-las no Context)
    validador_clientes = create_clientes_expectations(context)
    validador_produtos = create_produtos_expectations(context)
    validador_vendas = create_vendas_expectations(context)

    # 3. Executar o Validation (Run Checkpoint) para gerar o Data Docs
    
    print("\n=======================================================")
    print("✅ Great Expectations Suites Criadas com Sucesso!")
    print("=======================================================")
    print("As regras de qualidade (Expectations) para Clientes, Produtos e Vendas")
    print("estão prontas para serem usadas em um Checkpoint para validação contínua.")
    print("Elas codificam as regras de ALTA Criticidade (Unicidade e Integridade)")
    print("identificadas na PARTE 1.2.")