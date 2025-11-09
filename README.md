# 🚀 DataOps: Governança e Qualidade de Dados

## 📋 Sobre o Repositório

Este repositório contém o material completo do curso **DataOps: Governança e Qualidade de Dados**, incluindo conceitos teóricos, laboratórios práticos com **PySpark** e **Great Expectations**, e um desafio final abrangente.

### 🎯 Objetivos do Curso
- Dominar os **conceitos fundamentais** de DataOps: Governança de Dados
- Implementar **testes de qualidade** automatizados com Great Expectations
- Aplicar as **6 dimensões da qualidade** de dados na prática
- Criar **pipelines DataOps** profissionais e escaláveis
- Desenvolver **soluções completas** de monitoramento e auditoria

## 📁 Estrutura do Repositório

```
aulaGovernança/
├── 📚 Conceitos.md                             # Fundamentos teóricos
├── 🎯 Desafio_Final_DataOps.md                 # Desafio prático completo
├── 📊 datasets/                                # Dados para o desafio
│   ├── clientes.csv                            # Base de clientes (16 registros)
│   ├── produtos.csv                            # Catálogo de produtos (20 registros)
│   ├── vendas.csv                              # Transações de vendas (25 registros)
│   ├── logistica.csv                           # Dados de entrega (22 registros)
│   └── README.md                               # Documentação dos datasets
├── 📓 notebooks/                               # Laboratórios práticos
│   ├── Lab_DataOps_Governanca_Qualidade.ipynb  # Lab com Great Expectations
│   └── exporaDataSets.ipynb                    # Exploração dos datasets
├── 🐳 Dockerfile                               # Configuração do ambiente
├── 🐳 docker-compose.yml                       # Orquestração dos serviços
└── 📖 README.md                                # Este arquivo
```

## 🛠️ Tecnologias Utilizadas

- **Apache Spark** (PySpark) - Processamento distribuído de dados
- **Great Expectations** - Framework profissional de validação de dados
- **Jupyter Notebook** - Ambiente interativo de desenvolvimento
- **Docker** - Containerização e isolamento do ambiente
- **Pandas** - Manipulação de dados em Python

## 🏗️ Arquitetura do Ambiente

### Visão Geral da Arquitetura

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Jupyter Lab   │    │   Apache Spark   │    │ Great Expect.   │
│   (Port 8888)   │◄──►│   + Iceberg      │◄──►│  Data Context   │
│                 │    │   (Port 4040)    │    │   Data Docs     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────────┐
                    │   Docker Network    │
                    │  (172.16.240.0/24)  │
                    └─────────────────────┘
                                 │
                    ┌─────────────────────┐
                    │    Data Sources     │
                    │  CSV Files + Spark  │
                    │   Iceberg Tables    │
                    └─────────────────────┘
```

### Arquitetura Docker Compose

```
┌─────────────────────────────────────────────────────────────────┐
│                        Host Machine                             │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              Docker Compose Environment                   │  │
│  │                                                           │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │           pyspark_aula_container                    │  │  │
│  │  │                                                     │  │  │
│  │  │  ┌─────────────────┐  ┌─────────────────┐           │  │  │
│  │  │  │   Jupyter Lab   │  │  Apache Spark   │           │  │  │
│  │  │  │   (Port 8888)   │  │  (Port 4040)    │           │  │  │
│  │  │  │                 │  │  + Iceberg      │           │  │  │
│  │  │  └─────────────────┘  └─────────────────┘           │  │  │
│  │  │                                                     │  │  │
│  │  │  ┌─────────────────┐  ┌─────────────────┐           │  │  │
│  │  │  │Great Expectations│  │   Data Warehouse│          │  │  │
│  │  │  │  Data Context   │  │ /opt/warehouse  │           │  │  │
│  │  │  │   Data Docs     │  │                 │           │  │  │
│  │  │  └─────────────────┘  └─────────────────┘           │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  │                                                           │  │
│  │  Network: plataform-network (172.16.240.0/24)             │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Volume Mappings:                                               │
│  ./notebooks  ↔  /home/tavares/work                             │
│  ./data       ↔  /home/tavares/data                             │
└─────────────────────────────────────────────────────────────────┘
```

### Componentes da Arquitetura

#### 🐳 **Container Layer**
- **Base Image**: `jupyter/pyspark-notebook`
- **Custom User**: `tavares` (UID: 1001)
- **Working Directory**: `/home/tavares/work`
- **Volumes Mapeados**:
  - `./notebooks` → `/home/tavares/work`
  - `./data` → `/home/tavares/data`

#### 🔥 **Apache Spark Configuration**
- **Versão**: Spark 3.3.0 com Hadoop 3
- **Executor Memory**: 4GB
- **Driver Memory**: 4GB
- **Iceberg Support**: Habilitado para Data Lakehouse
- **PostgreSQL Driver**: Incluído para conectividade

#### 🎯 **Great Expectations Setup**
- **Data Context**: Configurado automaticamente
- **Datasources**: Pandas e Spark
- **Expectation Suites**: Para cada dataset
- **Checkpoints**: Automação de validações
- **Data Docs**: Relatórios HTML profissionais

#### 📊 **Data Architecture**
```
Data Flow:
📄 Raw CSV → 🐍 Pandas/Spark → 🎯 Great Expectations → 📈 Data Docs
                    ↓
                🗄️ Warehouse (Iceberg Tables)
```

### Portas e Serviços

| Serviço | Porta | Descrição |
|---------|-------|----------|
| **Jupyter Notebook** | 8888 | Interface principal de desenvolvimento |
| **Spark Web UI** | 4040 | Monitoramento de jobs Spark |
| **Data Docs** | File System | Relatórios Great Expectations |

### Fluxo de Dados

```
┌─────────────┐───►┌─────────────┐───►┌─────────────┐───►┌─────────────┐
│   Raw CSV   │    │   PySpark   │    │Great Expect.│    │ Data Docs   │
│   Datasets  │    │  DataFrame  │    │ Validation  │    │  Reports    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                           │
                           ▼
                   ┌─────────────┐
                   │   Iceberg   │
                   │  Warehouse  │
                   └─────────────┘
```

### Persistência de Dados

```
Volume Mapping:
📁 Host                    📦 Container
./notebooks/          →   /home/tavares/work/
./data/               →   /home/tavares/data/
./datasets/           →   Acessível via notebooks
```

## 🚀 Configuração do Ambiente

### Opção 1: GitHub Codespaces (Recomendado)

1. **Abra o repositório no GitHub Codespaces:**
   ```bash
   # Clique em "Code" > "Codespaces" > "Create codespace on main"
   # Ou use o link direto: https://github.com/AleTavares/dataops-governance-lab/codespaces
   ```

2. **Execute o ambiente com Docker Compose:**
   ```bash
   # No terminal do Codespace
   docker-compose up -d
   ```

3. **Acesse o Jupyter Notebook:**
   - URL: http://localhost:8888
   - Token: `tavares1234`
   - Spark UI: http://localhost:4040

4. **Acesse o laboratório:**
   - Abra `notebooks/Lab_DataOps_Governanca_Qualidade.ipynb`
   - Execute as células sequencialmente

### Opção 2: Docker (Ambiente Local)

1. **Clone o repositório:**
   ```bash
   git clone https://github.com/AleTavares/dataops-governance-lab.git
   cd aulaGovernanca
   ```

2. **Construa e execute o ambiente:**
   ```bash
   # Construir a imagem Docker
   docker-compose build
   
   # Iniciar os serviços
   docker-compose up -d
   ```

3. **Acesse o Jupyter Notebook:**
   - URL: http://localhost:8888
   - Token: `tavares1234`
   - Spark UI: http://localhost:4040

4. **Verifique os volumes:**
   ```bash
   # Os notebooks estão em ./notebooks/
   # Os dados estão em ./data/
   # Arquivos são persistidos automaticamente
   ```

## 📚 Como Usar Este Repositório

### 1. 📖 Estude os Conceitos
- (Leia primeiro os fundamentos teóricos)[Conceitos.md]

### 2. 🧪 Execute os Laboratórios
- (Explore os datasets primeiro)[notebooks/exporaDataSets.ipynb]
- (Execute o laboratório principal)[notebooks/Lab_DataOps_Governanca_Qualidade.ipynb]

### 3. 🎯 Realize o Desafio
(Desafio)[Desafio_Final_DataOps.md]

## 🎓 Roteiro de Aprendizagem

### Módulo 1: Fundamentos
1. 📚 Leia (Conceitos)[Conceitos.md] completamente
2. 🎯 Entenda os 4 pilares da governança
3. 📊 Memorize as 6 dimensões da qualidade

### Módulo 2: Prática
1. 🚀 Configure o ambiente (Docker ou Codespaces)
2. 📊 (Execute a exploração dos dados)[exporaDataSets.ipynb]
3. 🧪 Execute o laboratório principal passo a passo
4. 🔍 Experimente com Great Expectations
5. 📈 Analise os Data Docs gerados

### Módulo 3: Desafio
1. 📋 (Leia)[Desafio_Final_DataOps.md] 
2. 🔍 Analise os datasets fornecidos
3. 🏗️ Implemente a solução completa
4. 📊 Crie relatórios profissionais

## 🔧 Solução de Problemas

### Problema: Spark não inicia
```bash
# Verifique o Java
java -version

# Configure JAVA_HOME
export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
```

### Problema: Great Expectations não funciona
```bash
# 1. Reconstrua a imagem Docker com Great Expectations
docker-compose down
docker-compose build --no-cache
docker-compose up -d

# 2. Ou instale manualmente no container
docker exec -it pyspark_aula_container pip install great-expectations==0.18.8 sqlalchemy==1.4.46

# 3. Verifique a instalação no notebook
import great_expectations as gx
print(gx.__version__)
```

### Problema: Jupyter não acessa
```bash
# Verifique se a porta está livre
netstat -tulpn | grep 8888

# Use porta alternativa
jupyter notebook --port=8889
```

### Problema: Docker não funciona
```bash
# Verifique se Docker está rodando
docker --version
docker-compose --version

# Reconstrua a imagem
docker-compose down
docker-compose build --no-cache
docker-compose up
```

## 📊 Notebooks Disponíveis

### 🔍 **exporaDataSets.ipynb** - Exploração Inicial dos Dados
Notebook introdutório que demonstra como:
- Carregar os 4 datasets do projeto TechCommerce
- Verificar estrutura e quantidade de registros
- Identificar valores nulos e problemas básicos
- Criar análises consolidadas dos dados

**Ideal para**: Primeiro contato com os dados e compreensão da estrutura.

### 🧪 **Lab_DataOps_Governanca_Qualidade.ipynb** - Laboratório Principal
Laboratório completo com Great Expectations para:
- Implementar validações de qualidade
- Aplicar as 6 dimensões da qualidade
- Criar expectativas automatizadas
- Gerar relatórios profissionais

**Ideal para**: Aprendizado prático de DataOps e Great Expectations.

### 🧪 **test_great_expectations.ipynb** - Teste do Ambiente
Notebook de diagnóstico para:
- Verificar instalação do Great Expectations
- Testar Data Context
- Validar configuração do ambiente
- Executar testes básicos

**Ideal para**: Diagnóstico de problemas e verificação do ambiente.

## 📊 Datasets do Desafio

Os datasets simulam uma empresa de e-commerce (**TechCommerce**) com problemas reais de qualidade:

| Dataset | Registros | Problemas Principais |
|---------|-----------|---------------------|
| **clientes.csv** | 16 | Duplicatas, emails inválidos, campos vazios |
| **produtos.csv** | 20 | Preços negativos, categorias vazias, duplicatas |
| **vendas.csv** | 25 | Integridade referencial, datas futuras, cálculos incorretos |
| **logistica.csv** | 22 | Datas inconsistentes, campos vazios, duplicatas |

## 🤝 Contribuição

### Como Contribuir
1. Fork o repositório
2. Crie uma branch para sua feature
3. Faça commit das mudanças
4. Abra um Pull Request

### Reportar Problemas
- Use as **Issues** do GitHub
- Inclua logs de erro completos
- Descreva o ambiente utilizado

### Recursos Adicionais
- 📚 [Documentação Great Expectations](https://docs.greatexpectations.io/)
- 🔥 [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- 🐳 [Docker Documentation](https://docs.docker.com/)

## 📄 Licença
Este projeto está licenciado sob a **MIT License** - veja o arquivo [LICENSE](LICENSE) para detalhes.

## 🙏 Agradecimentos

- **Apache Spark** pela plataforma de processamento distribuído
- **Great Expectations** pelo framework de qualidade de dados
- **Jupyter Project** pelo ambiente interativo
- **Docker** pela containerização
