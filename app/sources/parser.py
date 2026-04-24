import json
import pandas as pd
import requests


def extract_columns(source):
    """
    Lê uma amostra da fonte e retorna lista de colunas.
    Suporta CSV, JSON (upload ou URL) e API REST.
    """

    if source.origin == 'upload' and source.file:
        return _from_file(source)

    elif source.origin in ('url', 'endpoint') and source.connection_string:
        return _from_url(source)

    elif source.origin == 'database' and source.connection_string:
        return _from_database(source)

    raise ValueError(f"Fonte {source.label} sem dados configurados.")


def _from_file(source):
    path = source.file.path

    if source.data_type == 'csv':
        df = pd.read_csv(path, nrows=5)

    elif source.data_type == 'json':
        df = pd.read_json(path)
        if isinstance(df, pd.DataFrame):
            df = df.head(5)

    else:
        raise ValueError(f"Tipo de arquivo não suportado: {source.data_type}")

    return list(df.columns)


def _from_url(source):
    headers = {}
    if source.headers:
        try:
            headers = json.loads(source.headers)
        except json.JSONDecodeError:
            pass

    response = requests.get(source.connection_string, headers=headers, timeout=10)
    response.raise_for_status()

    if source.data_type == 'csv':
        from io import StringIO
        df = pd.read_csv(StringIO(response.text), nrows=5)

    elif source.data_type in ('json', 'api'):
        data = response.json()
        if isinstance(data, list):
            df = pd.DataFrame(data[:5])
        elif isinstance(data, dict):
            # tenta encontrar a lista de registros dentro do objeto
            for value in data.values():
                if isinstance(value, list) and len(value) > 0:
                    df = pd.DataFrame(value[:5])
                    break
            else:
                df = pd.json_normalize(data)
        else:
            raise ValueError("Formato JSON não reconhecido.")

    else:
        raise ValueError(f"Tipo não suportado via URL: {source.data_type}")

    return list(df.columns)


def _from_database(source):
    from sqlalchemy import create_engine, inspect

    engine = create_engine(source.connection_string)
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    if not tables:
        raise ValueError("Nenhuma tabela encontrada no banco.")

    # retorna colunas da primeira tabela encontrada
    # futuramente o usuário poderá escolher a tabela
    columns = [col['name'] for col in inspector.get_columns(tables[0])]
    return columns


def read_dataframe(source):
    """
    Lê o dataframe completo de uma fonte.
    Usado na etapa de execução do pipeline.
    """

    if source.origin == 'upload' and source.file:
        if source.data_type == 'csv':
            return pd.read_csv(source.file.path)
        elif source.data_type == 'json':
            return pd.read_json(source.file.path)

    elif source.origin in ('url', 'endpoint') and source.connection_string:
        headers = {}
        if source.headers:
            try:
                headers = json.loads(source.headers)
            except json.JSONDecodeError:
                pass
        response = requests.get(source.connection_string, headers=headers, timeout=30)
        response.raise_for_status()
        if source.data_type == 'csv':
            from io import StringIO
            return pd.read_csv(StringIO(response.text))
        else:
            data = response.json()
            if isinstance(data, list):
                return pd.DataFrame(data)
            elif isinstance(data, dict):
                for value in data.values():
                    if isinstance(value, list):
                        return pd.DataFrame(value)
            return pd.json_normalize(data)

    elif source.origin == 'database' and source.connection_string:
        from sqlalchemy import create_engine, inspect
        engine = create_engine(source.connection_string)
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return pd.read_sql_table(tables[0], engine)

    raise ValueError(f"Não foi possível ler os dados da fonte {source.label}.")