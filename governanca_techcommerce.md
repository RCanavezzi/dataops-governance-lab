# 🏛️ Documento de Governança de Dados - TechCommerce

Este documento estabelece a estrutura de governança, as políticas de qualidade e o glossário de negócios para os principais domínios de dados da TechCommerce, garantindo que os dados sejam ativos confiáveis e estratégicos.

---

## 1. Organograma de Dados

Define os papéis e responsabilidades para os principais domínios de dados da empresa.

| Domínio | Data Owner (Estratégico) | Data Steward (Tático/Operacional) | Data Custodian (Técnico) |
| :--- | :--- | :--- | :--- |
| **Clientes** | Diretor(a) de Marketing | Analista de CRM | Engenheiro(a) de Dados (DBA) |
| **Produtos** | Diretor(a) Comercial/Produtos | Gerente de Categoria | Administrador(a) de Banco de Dados de Produtos |
| **Vendas** | Diretor(a) Financeiro(a) | Analista de Receita/BI | Engenheiro(a) de Dados (Pipeline) |
| **Logística** | Diretor(a) de Operações | Coordenador(a) de Estoque/Envio | Equipe de Infraestrutura Cloud |

---

## 2. Políticas de Qualidade de Dados

Define as dimensões da qualidade, os limites aceitáveis e as ações corretivas padrão.

| Dimensão | Definição (TechCommerce) | Limite Aceitável | Ações Corretivas (Exemplo) |
| :--- | :--- | :--- | :--- |
| **Completude** | Todos os campos críticos (e.g., `id_cliente`, `email`, `valor_total`) devem estar preenchidos. | Máximo de **2%** de dados incompletos nos campos críticos. | **Erro de Ingestão:** Rejeitar registro e notificar a fonte. **Dados Existentes:** Enviar para área de *Data Remediation* para imputação ou exclusão. |
| **Unicidade** | Cada entidade (e.g., `id_cliente`, `SKU` de produto) deve ter uma representação única no sistema. | **0%** de duplicidade em chaves primárias e campos de identificação. | **Na Ingestão:** Usar lógica de *upsert* ou deduplicação. **No Dataset:** Investigar a origem da duplicação e aplicar rotina de merge/exclusão. |
| **Validade** | Os valores dos dados devem estar em um formato e intervalo aceitável (e.g., data no passado, preço $> 0$). | Máximo de **1%** de valores inválidos (fora de formato/range). | **Na Ingestão:** Transformar para formato padrão ou marcar o registro. **No Dataset:** Corrigir ou remover registros que violam regras de negócio críticas. |
| **Consistência** | Os dados são coerentes entre si (e.g., o `valor_total` em Vendas é igual à soma dos itens; `estado` em Clientes é válido). | Máximo de **0.5%** de inconsistência entre datasets/campos relacionados. | **Cálculos:** Rodar rotinas diárias de checagem de regras de negócio e re-calcular campos derivados. **Valores:** Mapear e padronizar valores em um catálogo de referência. |
| **Integridade** | Relacionamentos entre tabelas (chaves estrangeiras) são mantidos. | **0%** de *Orphan Records* (registros sem pai, e.g., uma Venda sem um `id_cliente` correspondente). | **Na Ingestão:** Validar a existência da chave pai antes de inserir o filho. **No Dataset:** Remover ou isolar registros órfãos para investigação. |

---

## 3. Glossário de Negócios

Define termos chave e os padrões de formato para garantir o entendimento comum dos dados.

| Termo | Definição Clara | Padrão de Formato / Regra |
| :--- | :--- | :--- |
| **Cliente Ativo** | Um cliente que realizou pelo menos uma venda nos últimos 12 meses. | **Regra:** `data_ultima_venda` > (Data Atual - 1 ano). |
| **Venda Válida** | Uma transação de venda que foi concluída (`status = 'Concluída'`) e não foi estornada. | **Regra:** `status` $\in$ \{"Concluída"\}, `valor_total` $> 0$. |
| **SKU** | *Stock Keeping Unit* - Código de identificação único de um produto em estoque. | **Padrão:** Alfanumérico, máximo de 10 caracteres. Deve ser único no domínio de Produtos. |
| **Email** | Endereço de correio eletrônico do cliente. | **Padrão:** Formato `nome@dominio.extensao`. (Regex: `r"^[\w\.-]+@[\w\.-]+\.\w+$"`). |
| **Telefone** | Número de contato do cliente. | **Padrão:** Formato nacional de 11 dígitos (DDD + 9 dígitos). Apenas números. |
| **Data de Venda** | A data e hora exatas em que a transação foi registrada. | **Padrão:** ISO 8601 (YYYY-MM-DD) ou (YYYY-MM-DD HH:MM:SS). Não pode ser data futura. |
| **Regra de Relacionamento** | A relação entre Vendas, Clientes e Produtos é de N:1 (Vendas para Clientes) e N:1 (Vendas para Produtos), garantida por chaves estrangeiras (`id_cliente`, `id_produto`). | **Regra:** Todos os `id_cliente` e `id_produto` na tabela **Vendas** devem existir (Integridade Referencial) nas tabelas **Clientes** e **Produtos**, respectivamente. |