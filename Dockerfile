FROM python:3.9-slim

WORKDIR /app

RUN pip install pdm

COPY pyproject.toml pdm.lock* ./
RUN pdm install --prod

COPY . .

EXPOSE 3000

CMD ["pdm", "run", "dagit", "-h", "0.0.0.0", "-p", "3000"]