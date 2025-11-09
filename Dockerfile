# Usa a imagem oficial do Jupyter/PySpark como base
# Esta imagem já configura a SparkSession e variáveis de ambiente
FROM jupyter/pyspark-notebook

# Instala pacotes Python adicionais que podem ser úteis
# Ex: pandas para visualização local, matplotlib para plotting
# OBS: A instalação é feita como root para evitar problemas de permissão
# Se precisar de mais pacotes, adicione-os aqui
USER root
RUN pip install --no-cache-dir \
    pandas \
    matplotlib \
    findspark \
    great-expectations==0.18.8 \
    sqlalchemy==1.4.46
# Ajustado para Iceberg
RUN apt-get update
RUN apt-get install openjdk-8-jdk-headless -qq > /dev/null
RUN wget -q https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
RUN tar xf spark-3.3.0-bin-hadoop3.tgz -C /opt/
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.6.1/iceberg-spark-runtime-3.3_2.12-1.6.1.jar -P /opt/spark-3.3.0-bin-hadoop3/jars/
RUN wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.2/postgresql-42.7.2.jar -P /opt/spark-3.3.0-bin-hadoop3/jars/
RUN mkdir -p /opt/warehouse

# Configura variáveis de ambiente do Spark
ENV SPARK_HOME=/opt/spark-3.3.0-bin-hadoop3
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH
ENV WAREHOUSE_PATH=/home/tavares/warehouse

# A imagem base já define o usuário 'jovyan', mas vamos criar um usuário customizado para a nossa imagem.
USER root

# Cria o usuário 'tavares', seu diretório home e o adiciona ao grupo 'users' (GID 100)
# A UID 1001 é usada para evitar conflito com a UID do jovyan (1000)
RUN useradd -ms /bin/bash -g 100 -u 1001 tavares && \
    mkdir -p /home/tavares && \
    mkdir -p /home/tavares/warehouse && \
    chown -R tavares:users /home/tavares && \
    chown -R tavares:users /opt/warehouse

# Copia o script de inicialização
COPY init-great-expectations.py /home/tavares/
RUN chown tavares:users /home/tavares/init-great-expectations.py

# Retorna para o usuário 'tavares' e define o diretório de trabalho
USER tavares
WORKDIR /home/tavares/work

# Executa o script de inicialização
RUN python /home/tavares/init-great-expectations.py

# Porta 8888 (Jupyter Notebook) já é exposta pela imagem base.