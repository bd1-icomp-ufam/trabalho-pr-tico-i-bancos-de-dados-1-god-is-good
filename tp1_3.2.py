# Este código transforma os dados do arquivo de texto para um banco de dados

# Decisões importantes que tomei:
# 1. Tratei as categorias como uma árvore (tipo, categoria pai e filhas).
# 2. Mantive os produtos descontinuados no banco. Eles aparecem em outras tabelas, então achei melhor não excluir.
# 3. Na tabela product_similar, o ASIN similar não é uma chave estrangeira.
#    Tem muitos ASINs no arquivo que não estão na nossa lista de produtos.
# 4. Fiz a inserção em duas etapas: primeiro o que não tem dependências, depois o resto.

import psycopg2
import re
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

#configuracoes do bd
db_config = {
    'dbname': 'main',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

#essa funcao cria o banco de dados caso ele ainda nao exista
def create_database(dbname, user, password, host, port):
    conn = psycopg2.connect(dbname='postgres', user=user, password=password, host=host, port=port)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{dbname}'")
    exists = cursor.fetchone()
    if not exists:
        cursor.execute(f"CREATE DATABASE {dbname}")
    cursor.close()
    conn.close()

#chamada da funcao de criar banco (chamo fora da main para garantir que vai ser criado)
create_database(db_config['dbname'], db_config['user'], db_config['password'], db_config['host'], db_config['port'])

#criar o esquema do banco de dados
def create_tables():
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    create_tables_sql = [
        """
        CREATE TABLE product_group (
          group_id SERIAL PRIMARY KEY,
          group_name VARCHAR UNIQUE
        )
        """,
        """
        CREATE TABLE customer (
          customer_id VARCHAR PRIMARY KEY
        )
        """,
        """
        CREATE TABLE product (
          product_id INTEGER PRIMARY KEY,
          ASIN VARCHAR UNIQUE,
          group_id INTEGER,
          salesrank INTEGER,
          title VARCHAR,
          similar_value INTEGER,
          FOREIGN KEY (group_id) REFERENCES product_group (group_id)
        )
        """,
        """
        CREATE TABLE product_categories (
          category_id INTEGER PRIMARY KEY,
          category_name VARCHAR,
          parent_category_id INTEGER,
          FOREIGN KEY (parent_category_id) REFERENCES product_categories (category_id)
        )
        """,
        """
        CREATE TABLE product_reviews (
          review_id SERIAL PRIMARY KEY,
          created_at DATE,
          votes INTEGER,
          rating INTEGER,
          helpful INTEGER,
          product_id INTEGER,
          customer_id VARCHAR,
          FOREIGN KEY (product_id) REFERENCES product (product_id),
          FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
        )
        """,
        """
       CREATE TABLE product_similar (
            similar_id SERIAL PRIMARY KEY,
            product_asin VARCHAR REFERENCES product (ASIN),
            similar_product_asin VARCHAR
        )
        """,
        """
        CREATE TABLE product_category_link (
            id SERIAL PRIMARY KEY,
            product_id INTEGER,
            category_id INTEGER,
            FOREIGN KEY (product_id) REFERENCES product (product_id),
            FOREIGN KEY (category_id) REFERENCES product_categories (category_id)
        )
        """
    ]

    for command in create_tables_sql:
        cursor.execute(command)

    conn.commit()
    cursor.close()
    conn.close()

#inserir na tabela customer
def insert_customers(customer_ids):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    for customer_id in customer_ids:
        cursor.execute("INSERT INTO customer (customer_id) VALUES (%s) ON CONFLICT (customer_id) DO NOTHING;", (customer_id,))
    conn.commit()
    cursor.close()
    conn.close()

#inserir os grupos dos produtos
def insert_groups(group_names):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    for group_name in group_names:
        cursor.execute("INSERT INTO product_group (group_name) VALUES (%s) ON CONFLICT (group_name) DO NOTHING;", (group_name,))
    conn.commit()
    cursor.close()
    conn.close()

#essa funcao serve como 'parser' para os dados de produtos e clientes
#o script tem 2 parser pq aproveito para inserir o que n tem chave  de uma vez so
def process_file(filename):
    customer_ids = set()
    group_names = set()

    with open(filename, 'r') as file:
        for line in file:
            if 'cutomer:' in line:
                match = re.search(r'cutomer:\s+(\w+)', line)
                if match:
                    customer_ids.add(match.group(1))
            elif 'group:' in line and 'title:' not in line:
                # Capturando todo o texto após 'group:' e normalizando
                group_name = ' '.join(line.split('group:', 1)[1].strip().split()).upper()
                if group_name:
                    group_names.add(group_name)

    return customer_ids, group_names

#parser dos outros dados
def extract_product_data(filename):
    products = []
    current_product = None

    with open(filename, 'r') as file:
        for line in file:
            if line.startswith("Id:"):
                if current_product:
                    products.append(current_product)
                product_id = int(line.split()[1].strip())
                current_product = {
                    'product_id': product_id,
                    'ASIN': '',
                    'discontinued': False,
                    'categories': [],
                    'similar_products': [],
                    'similar_value': 0
                }
            elif line.startswith("ASIN:"):
                current_product['ASIN'] = line.split()[1].strip()
            elif 'discontinued product' in line:
                current_product['discontinued'] = True
            elif 'title:' in line and current_product and not current_product.get('discontinued', False):
                current_product['title'] = line.split('title:', 1)[1].strip()
            elif 'group:' in line and current_product and not current_product.get('discontinued', False):
                current_product['group'] = line.split('group:', 1)[1].strip()
            elif 'salesrank:' in line and current_product and not current_product.get('discontinued', False):
                current_product['salesrank'] = int(line.split('salesrank:', 1)[1].strip())
            elif 'similar:' in line and current_product and not current_product.get('discontinued', False):
                parts = line.split()
                similar_count = int(parts[1])
                current_product['similar_value'] = similar_count
                current_product['similar_products'] = parts[2:2 + similar_count] if similar_count > 0 else []
            elif 'categories:' in line and current_product and not current_product.get('discontinued', False):
                current_product['categories'] = []
            elif '|' in line and current_product and not current_product.get('discontinued', False):
                current_product['categories'].append(line.strip())

        if current_product:
            products.append(current_product)

    return products

#inserir produtos
#inserir os  produtos descontinuados pois sao usados em alguns casos
def insert_products(products):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    for product in products:
        if product.get('discontinued'):
            # Inserir produtos descontinuados sem detalhes de grupo, título e rank de vendas
            cursor.execute("""
                INSERT INTO product (product_id, ASIN, title, group_id, salesrank, similar_value)
                VALUES (%s, %s, NULL, NULL, NULL, NULL) ON CONFLICT (product_id) DO NOTHING;
                """, (product['product_id'], product['ASIN']))
        else:
            # Normalizar o nome do grupo para correspondência
            group_id = None
            if product['group']:
                group_name = product['group'].upper()  # Normalizando para maiúsculas
                cursor.execute("SELECT group_id FROM product_group WHERE group_name = %s", (group_name,))
                group_result = cursor.fetchone()
                group_id = group_result[0] if group_result else None

                # Se o group_id não for encontrado, inserir o novo grupo e obter o novo group_id
                if group_id is None:
                    cursor.execute("INSERT INTO product_group (group_name) VALUES (%s) RETURNING group_id;", (group_name,))
                    group_id = cursor.fetchone()[0]

            # Inserir detalhes do produto
            cursor.execute("""
                INSERT INTO product (product_id, ASIN, title, group_id, salesrank, similar_value)
                VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (product_id) DO NOTHING;
                """, (product['product_id'], product['ASIN'], product['title'], group_id, product['salesrank'], product['similar_value']))

    conn.commit()
    cursor.close()
    conn.close()


#inserir na tabela de similares
def insert_similar_products(products):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    for product in products:
        if not product.get('discontinued', False):
            product_asin = product['ASIN']
            for similar_asin in product.get('similar_products', []):
                cursor.execute("""
                    INSERT INTO product_similar (product_asin, similar_product_asin)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING;
                    """, (product_asin, similar_asin))

    conn.commit()
    cursor.close()
    conn.close()

#inserir na tabela categorias
def insert_categories(products):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    for product in products:
        if not product.get('discontinued', False):
            for category_path in product['categories']:
                parent_id = None
                category_parts = category_path.split('|')[1:]

                for category in category_parts:
                    category_match = re.match(r"(.*)\[(\d+)\]", category)
                    if category_match:
                        category_name = category_match.group(1).strip()
                        category_id = int(category_match.group(2))

                        cursor.execute("""
                            INSERT INTO product_categories (category_id, category_name, parent_category_id)
                            VALUES (%s, %s, %s) ON CONFLICT (category_id) DO NOTHING;
                            """, (category_id, category_name, parent_id))
                        parent_id = category_id

    conn.commit()
    cursor.close()
    conn.close()

#inserir na tabela de reviews
def extract_and_insert_reviews(filename):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    with open(filename, 'r') as file:
        product_id = None
        for line in file:
            if line.startswith("Id:"):
                product_id = int(line.split()[1].strip())

            elif 'cutomer:' in line and product_id is not None:
                review_data = line.strip().split()
                created_at = review_data[0]
                customer_id = review_data[2]
                rating = int(review_data[4])
                votes = int(review_data[6])
                helpful = int(review_data[8])

                cursor.execute("""
                    INSERT INTO product_reviews (created_at, customer_id, rating, votes, helpful, product_id)
                    VALUES (TO_DATE(%s, 'YYYY-MM-DD'), %s, %s, %s, %s, %s);
                    """, (created_at, customer_id, rating, votes, helpful, product_id))

    conn.commit()
    cursor.close()
    conn.close()

# Função para inserir dados na tabela product_category_link
def insert_product_category_links(products):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    for product in products:
        if not product.get('discontinued', False):
            product_id = product['product_id']
            for category_path in product['categories']:
                category_parts = category_path.split('|')[1:]
                for category in category_parts:
                    category_id = int(re.search(r"\[(\d+)\]", category).group(1))
                    cursor.execute("""
                        INSERT INTO product_category_link (product_id, category_id)
                        VALUES (%s, %s) ON CONFLICT DO NOTHING;
                        """, (product_id, category_id))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_tables()
    # inserir grupos e clientes primeiro pq nao tem fk
    customer_ids, group_names = process_file('amazon-meta.txt')
    insert_customers(customer_ids)
    insert_groups(group_names)

    # releitura do arquivo do início
    # para evitar problemas com as chaves estrangeiras (FKs)
    products = extract_product_data('amazon-meta.txt')
    insert_products(products)
    insert_categories(products)
    insert_similar_products(products)
    insert_product_category_links(products)
    extract_and_insert_reviews('amazon-meta.txt')
