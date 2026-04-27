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
        import json as _json
        with open(path, encoding='utf-8') as f:
            data = _json.load(f)

        if isinstance(data, list):
            df = pd.json_normalize(data[:5])
        elif isinstance(data, dict):
            for value in data.values():
                if isinstance(value, list) and len(value) > 0:
                    df = pd.json_normalize(value[:5])
                    break
            else:
                df = pd.json_normalize(data)
        else:
            raise ValueError('Formato JSON não reconhecido.')

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
            df = pd.json_normalize(data[:5])
        elif isinstance(data, dict):
            # tenta encontrar a lista de registros dentro do objeto
            for value in data.values():
                if isinstance(value, list) and len(value) > 0:
                    df = pd.json_normalize(value[:5])
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
            df = pd.read_csv(source.file.path)
        elif source.data_type == 'json':
            import json as _json
            with open(source.file.path, encoding='utf-8') as f:
                data = _json.load(f)
            if isinstance(data, list):
                df = pd.json_normalize(data)
            elif isinstance(data, dict):
                for value in data.values():
                    if isinstance(value, list) and len(value) > 0:
                        df = pd.json_normalize(value)
                        break
                else:
                    df = pd.json_normalize(data)
        else:
            raise ValueError(f"Tipo de arquivo não suportado: {source.data_type}")

        print(f"[DEBUG] read_dataframe upload source={source.label} type={source.data_type} shape={df.shape} cols={list(df.columns)}")
        return df

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
            df = pd.read_csv(StringIO(response.text))
        else:
            data = response.json()
            if isinstance(data, list):
                df = pd.json_normalize(data)
            elif isinstance(data, dict):
                for value in data.values():
                    if isinstance(value, list):
                        df = pd.json_normalize(value)
                        break
                else:
                    df = pd.json_normalize(data)
            else:
                raise ValueError("Formato JSON não reconhecido.")

        print(f"[DEBUG] read_dataframe url/source={source.label} origin={source.origin} type={source.data_type} url={source.connection_string} shape={df.shape} cols={list(df.columns)}")
        return df

    elif source.origin == 'database' and source.connection_string:
        from sqlalchemy import create_engine, inspect
        engine = create_engine(source.connection_string)
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        df = pd.read_sql_table(tables[0], engine)
        print(f"[DEBUG] read_dataframe database source={source.label} conn={source.connection_string} table={tables[0]} shape={df.shape} cols={list(df.columns)}")
        return df

    raise ValueError(f"Não foi possível ler os dados da fonte {source.label}.")


def execute_pipeline(integration):
    config = integration.join_config
    sources = {source.label: source for source in integration.sources.all()}
    source_a = sources.get('A')
    source_b = sources.get('B')

    if not source_a or not source_b:
        raise ValueError('As duas fontes A e B precisam estar configuradas.')

    df_a = read_dataframe(source_a)
    df_b = read_dataframe(source_b)

    print(f"[DEBUG] execute_pipeline integration={integration.pk} name={integration.name}")
    print(f"[DEBUG] source A cols={list(df_a.columns)}")
    print(f"[DEBUG] source B cols={list(df_b.columns)}")
    print(f"[DEBUG] join keys A={config.key_source_a} B={config.key_source_b} type={config.join_type}")

    if config.key_source_a not in df_a.columns:
        raise ValueError(f"Chave '{config.key_source_a}' não encontrada em Fonte A.")

    if config.key_source_b not in df_b.columns:
        raise ValueError(f"Chave '{config.key_source_b}' não encontrada em Fonte B.")

    result = pd.merge(
        df_a,
        df_b,
        how=config.join_type,
        left_on=config.key_source_a,
        right_on=config.key_source_b,
        suffixes=('_A', '_B')
    )

    print(f"[DEBUG] merge result shape={result.shape} cols={list(result.columns)}")

    if config.columns_to_keep:
        selected_columns = []
        for raw in config.columns_to_keep:
            if ':' in raw:
                prefix, col = raw.split(':', 1)
                if prefix == 'A' and f"{col}_A" in result.columns:
                    selected_columns.append(f"{col}_A")
                elif prefix == 'B' and f"{col}_B" in result.columns:
                    selected_columns.append(f"{col}_B")
                elif col in result.columns:
                    selected_columns.append(col)
            else:
                col = raw
                if col in result.columns:
                    selected_columns.append(col)
                elif f"{col}_A" in result.columns:
                    selected_columns.append(f"{col}_A")
                elif f"{col}_B" in result.columns:
                    selected_columns.append(f"{col}_B")

        selected_columns = list(dict.fromkeys(selected_columns))
        print(f"[DEBUG] selected_columns resolved={selected_columns}")
        if selected_columns:
            result = result.loc[:, [c for c in selected_columns if c in result.columns]]

    return result


def make_preview(df):
    preview = df.copy()
    preview = preview.where(pd.notnull(preview), None)
    return preview.to_dict(orient='records')


def build_chart_data(df, max_categories=8):
    if df.empty:
        return {}

    first_column = df.columns[0]
    counts = df[first_column].fillna('N/A').astype(str).value_counts().head(max_categories)
    return {
        'labels': counts.index.tolist(),
        'values': counts.tolist(),
        'label': f'Contagem por {first_column}',
    }