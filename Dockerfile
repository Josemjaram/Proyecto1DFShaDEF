FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dfsha ./dfsha
COPY scripts ./scripts
# Un solo imagen para los tres roles: el rol se elige con el comando (controller | datanode | cli)
ENTRYPOINT ["python", "-m"]
CMD ["dfsha.cli", "--help"]
