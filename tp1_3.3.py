import psycopg2
import sys
from psycopg2 import sql

# Configurações do banco de dados
db_config = {
    'dbname': 'main',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

# executar uma consulta SQL e escrever os resultados em um arquivo
def execute_query(query, params, filename):
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
        with open(filename, 'a') as file:
            file.write(f"\n--- Resultados da consulta ---\n")
            for row in results:
                file.write(str(row) + '\n')
        print(f"Resultados salvos em {filename}")
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao executar a consulta: {error}")
    finally:
        if conn:
            cursor.close()
            conn.close()

# mostrar o menu e capturar a escolha do usuário
def show_menu():
    print("\nDashboard de Consultas SQL")
    print("1. Comentários mais úteis e com maior/menor avaliação")
    print("2. Produtos similares com maiores vendas")
    print("3. Evolução diária das médias de avaliação")
    print("4. 10 produtos líderes de venda em cada grupo")
    print("5. 10 produtos com a maior média de avaliações úteis positivas")
    print("6. 5 categorias de produto com a maior média de avaliações úteis positivas")
    print("7. 10 clientes que mais fizeram comentários por grupo de produto")
    print("8. Sair")
    return input("Escolha uma opção: ")

def main():
    filename = "query_results.txt"  # Arquivo onde os resultados serão salvos

    while True:
        choice = show_menu()

        if choice == '1':
            product_id = input("Digite o ID do produto: ")
            query = sql.SQL("""
                (SELECT pr.review_id, pr.created_at, pr.rating, pr.helpful, pr.votes, p.title
                FROM product_reviews pr
                INNER JOIN product p ON pr.product_id = p.product_id
                WHERE p.product_id = %s
                ORDER BY pr.helpful DESC, pr.rating DESC
                LIMIT 5)
                UNION ALL
                (SELECT pr.review_id, pr.created_at, pr.rating, pr.helpful, pr.votes, p.title
                FROM product_reviews pr
                INNER JOIN product p ON pr.product_id = p.product_id
                WHERE p.product_id = %s
                ORDER BY pr.helpful DESC, pr.rating ASC
                LIMIT 5);
            """)
            execute_query(query, (product_id, product_id), filename)

        elif choice == '2':
            product_asin = input("Digite o ASIN do produto: ")
            query = sql.SQL("""
                SELECT ps.similar_id, ps.product_asin, ps.similar_product_asin, p1.salesrank as original_rank, p2.salesrank as similar_rank
                FROM product_similar ps
                JOIN product p1 ON ps.product_asin = p1.asin
                JOIN product p2 ON ps.similar_product_asin = p2.asin
                WHERE ps.product_asin = %s AND p2.salesrank < p1.salesrank
                ORDER BY p2.salesrank ASC;
            """)
            execute_query(query, (product_asin,), filename)

        elif choice == '3':
            product_id = input("Digite o ID do produto: ")
            query = sql.SQL("""
                SELECT DATE(created_at) as date, AVG(rating) as avg_rating
                FROM product_reviews
                WHERE product_id = %s
                GROUP BY DATE(created_at)
                ORDER BY DATE(created_at);
            """)
            execute_query(query, (product_id,), filename)

        elif choice == '4':
            query = sql.SQL("""
                WITH ranked_products AS (
                    SELECT p.*, pg.group_name,
                           ROW_NUMBER() OVER (PARTITION BY p.group_id ORDER BY p.salesrank ASC) AS rank_in_group
                    FROM product p
                    JOIN product_group pg ON p.group_id = pg.group_id
                )
                SELECT product_id, asin, title, salesrank, group_name
                FROM ranked_products
                WHERE rank_in_group <= 10
                ORDER BY group_name, salesrank;
            """)
            execute_query(query, (), filename)

        elif choice == '5':
            query = sql.SQL("""
                SELECT p.product_id, p.asin, p.title, AVG(pr.helpful) as avg_helpful_positive
                FROM product p
                JOIN product_reviews pr ON p.product_id = pr.product_id
                WHERE pr.helpful > 0
                GROUP BY p.product_id, p.asin, p.title
                ORDER BY avg_helpful_positive DESC
                LIMIT 10;
            """)
            execute_query(query, (), filename)

        elif choice == '6':
            query = sql.SQL("""
                SELECT pc.category_id, pc.category_name, AVG(pr.helpful) as avg_helpful_positive
                FROM product_categories pc
                JOIN product_category_link pcl ON pc.category_id = pcl.category_id
                JOIN product p ON pcl.product_id = p.product_id
                JOIN product_reviews pr ON p.product_id = pr.product_id
                WHERE pr.helpful > 0
                GROUP BY pc.category_id, pc.category_name
                ORDER BY avg_helpful_positive DESC
                LIMIT 5;
            """)
            execute_query(query, (), filename)

        elif choice == '7':
            query = sql.SQL("""
                SELECT pg.group_name, pr.customer_id, COUNT(*) as review_count
                FROM product_reviews pr
                JOIN product p ON pr.product_id = p.product_id
                JOIN product_group pg ON p.group_id = pg.group_id
                GROUP BY pg.group_name, pr.customer_id
                ORDER BY pg.group_name, review_count DESC
                LIMIT 10;
            """)
            execute_query(query, (), filename)

        elif choice == '8':
            print("Saindo....")
            sys.exit()
        else:
            print("Opção inválida. Tente novamente.")

if __name__ == "__main__":
    main()
