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
