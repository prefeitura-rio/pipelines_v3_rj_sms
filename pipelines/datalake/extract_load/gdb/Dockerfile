FROM python:3.13-slim

#####################
## Dockerfile padrão
#####################
ENV DO_NOT_TRACK=1
# Instala `uv` (Python compatível)
COPY --from=ghcr.io/astral-sh/uv:0.10 /uv /uvx /bin/
# Copia dependências do projeto, instala
WORKDIR /opt/prefect/pipelines_v3_rj_sms/
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev
# Adiciona .venv ao PATH
ENV PATH="/opt/prefect/pipelines_v3_rj_sms/.venv/bin:$PATH"
# Copia os flows
COPY ./pipelines /opt/prefect/pipelines_v3_rj_sms/pipelines

#####################
## Firebird
#####################
# Situação: os arquivos que recebemos são gerados na versão 1.5
# do Firebird, via Windows 32-bit; nossos flows são Linux 64-bit
# Além disso, a versão 1.5 é de 2009[1]. Então, o passo a passo é:
# - Baixar Firebird v2.5 32-bit (é de 2019! muito melhor) 
# - Usando gbak 32-bit, exportar o .GDB como .FBK (Gemini diz que é bitness agnostic)
# - Baixar Firebird v2.5 64-bit
# - Usando gbak 64-bit, restaurar o .FBK como .FDB (agora 64-bit)
# - Usar a biblioteca `fdb` (moderna!) pra executar queries no arquivo
# [1] https://www.firebirdsql.org/en/discontinued-versions/

# Precisamos avisar ao Linux que vamos rodar algumas coisas 32-bit
# e instalar dependências do Firebird
RUN dpkg --add-architecture i386 && \
	apt-get update && \
	apt-get install -y \
		libc6:i386 libstdc++6:i386 libncurses6:i386 libtinfo6:i386 \
		libncurses6 libtinfo6 \
		wget tar

# libncurses5 só vai até Debian 12 (bookworn); estamos no Debian 13 (trixie)
# Aqui enganamos o `gbak` fingindo que a v5 existe, apontando pra v6, e funciona!
RUN ln -s /usr/lib/i386-linux-gnu/libncurses.so.6 /usr/lib/i386-linux-gnu/libncurses.so.5 && \
	ln -s /usr/lib/i386-linux-gnu/libtinfo.so.6 /usr/lib/i386-linux-gnu/libtinfo.so.5
RUN ln -s /usr/lib/x86_64-linux-gnu/libncurses.so.6 /usr/lib/x86_64-linux-gnu/libncurses.so.5 && \
	ln -s /usr/lib/x86_64-linux-gnu/libtinfo.so.6 /usr/lib/x86_64-linux-gnu/libtinfo.so.5

# 1. Baixa Firebird Classic Server 2.5.9, 32-bit (jun/2019)
# 2. Extrai .tar.gz pra uma pasta
# 3. Cria /opt/32bit/firebird/tmp
# 4. Extrair buildroot.tar.gz pra pasta criada
# 5. Apaga a pasta de (2)
# 6. Move bibliotecas de (...)/tmp/usr pra /usr
# 7. Move binários etc de (...)/tmp/opt/firebird pra /opt/32bit/firebird
RUN wget -q https://github.com/FirebirdSQL/firebird/releases/download/R2_5_9/FirebirdCS-2.5.9.27139-0.i686.tar.gz && \
	tar -xzf FirebirdCS-2.5.9.27139-0.i686.tar.gz && \
	mkdir -p /opt/32bit/firebird/tmp && \
	tar -xzf FirebirdCS-2.5.9.27139-0.i686/buildroot.tar.gz -C /opt/32bit/firebird/tmp && \
	rm -rf FirebirdCS-2.5.9.27139-0.i686* && \
	mv /opt/32bit/firebird/tmp/usr/lib/* /usr/lib && \
	mv /opt/32bit/firebird/tmp/usr/include/* /usr/include && \
	mv /opt/32bit/firebird/tmp/opt/firebird/* /opt/32bit/firebird

# Repete os mesmos passos pra versão 64-bit
RUN wget -q https://github.com/FirebirdSQL/firebird/releases/download/R2_5_9/FirebirdCS-2.5.9.27139-0.amd64.tar.gz && \
	tar -xzf FirebirdCS-2.5.9.27139-0.amd64.tar.gz && \
	mkdir -p /opt/64bit/firebird/tmp && \
	tar -xzf FirebirdCS-2.5.9.27139-0.amd64/buildroot.tar.gz -C /opt/64bit/firebird/tmp && \
	rm -rf FirebirdCS-2.5.9.27139-0.amd64* && \
	mv /opt/64bit/firebird/tmp/usr/lib64/* /usr/lib64 && \
	mv /opt/64bit/firebird/tmp/usr/include/* /usr/include && \
	mv /opt/64bit/firebird/tmp/opt/firebird/* /opt/64bit/firebird

# Linking de bibliotecas que ele reclama
RUN /sbin/ldconfig -n /opt/32bit/firebird/lib && \
	ln -sf /opt/32bit/firebird/lib/libfbembed.so.2.5 /opt/32bit/firebird/lib/libfbclient.so.2 && \
	ln -sf /opt/32bit/firebird/lib/libfbembed.so.2.5 /opt/32bit/firebird/lib/libfbclient.so
RUN /sbin/ldconfig -n /opt/64bit/firebird/lib && \
	ln -sf /opt/64bit/firebird/lib/libfbembed.so.2.5 /opt/64bit/firebird/lib/libfbclient.so.2 && \
	ln -sf /opt/64bit/firebird/lib/libfbembed.so.2.5 /opt/64bit/firebird/lib/libfbclient.so

# Permissão abrangente pro flow conseguir rodar o gbak sem estresse
RUN chmod -R 777 /opt/32bit/firebird && \
	chmod -R 777 /opt/64bit/firebird

# Ainda é preciso configurar algumas variáveis de ambiente:
## ENV PATH="/opt/32bit/firebird:/opt/32bit/firebird/bin:/opt/32bit/firebird/lib:$PATH"
## ENV FIREBIRD="/opt/32bit/firebird"
## ENV LD_LIBRARY_PATH="/opt/32bit/firebird/lib"
# Mas porque a gente precisa trocar de 32 pra 64 bit, carregamos via código
