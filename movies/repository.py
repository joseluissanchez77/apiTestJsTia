from django.db import connection
from .common import TableNames

class MoviesRepository:

    TABLE_NAME = TableNames.TB_REVIEWS

    @staticmethod
    def get_timeline(sql_params,sort_parameter,order_direction):
        # Usa la conexión predeterminada definida en settings.py
        with connection.cursor() as cursor:
            # Construir la consulta SQL con LIMIT, OFFSET y ORDER BY dinámicos usando parámetros
            query = f"""SELECT 
                TO_CHAR(review_time, 'DD/MM/YYYY') as review_time,
                AVG(score) AS avg_score,
                AVG(CAST(SPLIT_PART(helpfulness, '/', 1) AS FLOAT) / CAST(SPLIT_PART(helpfulness, '/', 2) AS FLOAT)) AS avg_helpfulness,
                CONCAT(AVG(CAST(SPLIT_PART(helpfulness, '/', 1) AS FLOAT)), '/', AVG(CAST(SPLIT_PART(helpfulness, '/', 2) AS FLOAT))) AS avg_helpfulness_format
                FROM 
                    {MoviesRepository.TABLE_NAME}
                WHERE
                   (COALESCE(%s, review_time) <= review_time) AND
                   (COALESCE(%s, review_time) >= review_time)
                    AND (%s IS NULL OR product_id LIKE %s OR product_id LIKE %s OR product_id LIKE %s)
                    AND (
                        (%s IS NULL OR user_id LIKE %s OR user_id LIKE %s OR user_id LIKE %s)
                        OR (%s IS NULL OR profile_name LIKE %s OR profile_name LIKE %s OR profile_name LIKE %s)
                        )
                GROUP BY
                    review_time,product_id
                ORDER BY {sort_parameter} {order_direction}
                LIMIT %s OFFSET %s
            """
             # Ejecutar la consulta SQL con parámetros
            cursor.execute(query,sql_params)


            # Obtener los resultados de la consulta
            #return cursor.fetchall()
            
            # Obtener los resultados de la consulta (se pude retornar)
            results = cursor.fetchall()


            # Obtener los nombres de las columnas desde el cursor.description
            column_names = [col[0] for col in cursor.description]

        # Formatear los resultados de manera automapeable usando los nombres de las columnas
        data = [dict(zip(column_names, row)) for row in results]

        return data


    @staticmethod
    def get_data_review(sql_params,sort_parameter,order_direction):
        # Usa la conexión predeterminada definida en settings.py
        with connection.cursor() as cursor:
            # Construir la consulta SQL con LIMIT, OFFSET y ORDER BY dinámicos usando parámetros
            query = f"""SELECT 
                product_id,
                categories,
                user_id,
                profile_name,
                helpfulness,
                score,
                TO_CHAR(review_time, 'DD/MM/YYYY') as review_time,
                summary,
                review_text 
                FROM 
                    {MoviesRepository.TABLE_NAME}
                WHERE
                   (COALESCE(%s, review_time) <= review_time) AND
                   (COALESCE(%s, review_time) >= review_time)
                    AND (%s IS NULL OR product_id LIKE %s OR product_id LIKE %s OR product_id LIKE %s)
                    AND (
                        (%s IS NULL OR user_id LIKE %s OR user_id LIKE %s OR user_id LIKE %s)
                        OR (%s IS NULL OR profile_name LIKE %s OR profile_name LIKE %s OR profile_name LIKE %s)
                        )
                ORDER BY {sort_parameter} {order_direction}
                LIMIT %s OFFSET %s
            """
             # Ejecutar la consulta SQL con parámetros
            cursor.execute(query,sql_params)


            # Obtener los resultados de la consulta
            results = cursor.fetchall()

            # Obtener los nombres de las columnas desde el cursor.description
            column_names = [col[0] for col in cursor.description]

        # Formatear los resultados de manera automapeable usando los nombres de las columnas
        data = [dict(zip(column_names, row)) for row in results]

        return data

    @staticmethod
    def get_data_total_register(sql_params):
        # Usa la conexión predeterminada definida en settings.py
        with connection.cursor() as cursor:
            # Obtener el número total de registros
            count_query = f"""
                SELECT COUNT(*) 
                FROM 
                {MoviesRepository.TABLE_NAME}
                WHERE
                   (COALESCE(%s, review_time) <= review_time) AND
                   (COALESCE(%s, review_time) >= review_time)
                    AND (%s IS NULL OR product_id LIKE %s OR product_id LIKE %s OR product_id LIKE %s)
                    AND (
                        (%s IS NULL OR user_id LIKE %s OR user_id LIKE %s OR user_id LIKE %s)
                        OR (%s IS NULL OR profile_name LIKE %s OR profile_name LIKE %s OR profile_name LIKE %s)
                        )
                """
            cursor.execute(count_query, sql_params[:14])  # Solo se necesitan los primeros 6 parámetros
            count_query = cursor.fetchone()[0]

        return count_query