# Começamos com a imagem oficial do Airflow que já estamos a usar.
FROM apache/airflow:2.9.2

# Mudamos para o utilizador root para podermos instalar software.
USER root

# Atualizamos os pacotes e instalamos o Java 17 (headless é mais leve).
# Depois, limpamos o cache para manter a imagem pequena.
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# Definimos a variável de ambiente JAVA_HOME dentro da imagem.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Voltamos para o utilizador 'airflow' por segurança.
USER airflow