# data_integrator

 

## Configuração inicial

### 1. Clone o repositório

```bash
git clone <repo-url>
cd <repo-name>
```

---

### 2. Criar arquivo `.env`

Crie um arquivo `.env` na raiz do projeto:

```env
DEBUG=1

POSTGRES_DB=mydatabase
POSTGRES_USER=myuser
POSTGRES_PASSWORD=mypassword
POSTGRES_HOST=db
POSTGRES_PORT=5432
```

---

### 3. Build dos containers

```bash
docker-compose build
```
  
---

### 4. Rodar migrations

```bash
docker-compose run web python manage.py migrate
```

---

## Rodando o projeto

```bash
docker-compose up
```

A aplicação estará disponível em:

http://localhost:8000

---

## Comandos úteis

### Criar uma nova app Django

```bash
docker-compose run web python manage.py startapp <app_name>
```

---

### Criar superusuário

```bash
docker-compose run web python manage.py createsuperuser
```

---

### Rodar migrations

```bash
docker-compose run web python manage.py migrate
```

---

### Criar novas migrations

```bash
docker-compose run web python manage.py makemigrations
```

---

## Banco de dados

* Host: db
* Porta: 5432
* Nome, usuário e senha definidos no `.env`

---

## Observações

* O projeto usa volumes Docker para persistência do banco de dados
* O arquivo `.env` não deve ser versionado
* Caso o banco ainda não esteja pronto, o Django pode falhar na primeira tentativa

---

## Parar containers

```bash
docker-compose down
```

---

## Dicas

* Sempre rode comandos Django através do container (`docker-compose run web`)
* Evite rodar Django localmente fora do Docker para manter consistência

---

## Tecnologias

* Django
* PostgreSQL
* Docker

---

 