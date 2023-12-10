from django.shortcuts import render

# Create your views here.

from django.http import JsonResponse
from django.db import connection
from django.db.utils import DatabaseError
from .repository import MoviesRepository
from .params_helper import RequestParamsHelper
from .decorators_errors import handle_errors


import json




def data_movies(request):
    try:

        # Nombre de la tabla
        table_name = 'reviewsdos'  # Puedes ajustar esto según tus necesidades

        # Obtén el valor del parámetro 'size' de la consulta, utilizando 10 como valor predeterminado si no se proporciona
        size = int(request.GET.get('size', 10))#cantidad de registros
        page = int(request.GET.get('page', 1))#pagina
        sort_parameter = request.GET.get('sort', 'product_id')  # Parámetro para ordenar, por defecto 'product_id'
        start_date = request.GET.get('start_date', None)  # Parámetro para la fecha de inicio del rango
        end_date = request.GET.get('end_date', None)  # Parámetro para la fecha de fin del rango
        product_id = request.GET.get('product_id', None)  # Parámetro para la búsqueda por product_id
        search_user = request.GET.get('search_user', None)  # Parámetro para la búsqueda de usuarios

        # Calcula el offset para la consulta SQL
        offset = (page - 1) * size

        # Verifica si se proporciona un orden ascendente o descendente (por defecto, ascendente)
        order_direction = 'ASC' if request.GET.get('order', 'asc').lower() == 'asc' else 'DESC'

        # Parámetros para las consultas SQL
        sql_params = [start_date, end_date, 
                    product_id, f"{product_id}%", f"%{product_id}", f"%{product_id}%" ,
                    search_user,f"%{search_user}%", f"%{search_user}", f"{search_user}%",
                    search_user,f"%{search_user}%", f"%{search_user}", f"{search_user}%",
                    size, offset]



        # Imprimir el valor del parámetro para depurar
        #print(f'Tamaño recibido: {size}')

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
                    {table_name}
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


            # Obtener el número total de registros
            count_query = f"""
                SELECT COUNT(*) 
                FROM {table_name} 
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
    
        # Formatear los resultados de manera automapeable usando los nombres de las columnas
        data = [dict(zip(column_names, row)) for row in results]

            

        # Calcular el número total de páginas
        total_pages = (count_query + size - 1) // size
            
        # Formatear los resultados como desees (aquí, simplemente se devuelven como JSON)
        #data = [{'product_id': row[0], 'categories': row[1], 'user_id': row[2]} for row in results]

        # Construir la respuesta estructurada
        response_data = {
            "current_page": page,
            "data": data,
            "total_register": count_query,
            "total_pages": total_pages
        }


        # Devolver los datos como una respuesta JSON
        return JsonResponse(response_data, safe=False)
    except DatabaseError as e:
        # Maneja cualquier error de conexión aquí
        return JsonResponse({'error': str(e)}, status=500)
        

def data_additional(request):
    try:

        # Nombre de la tabla
        table_name = 'reviewsdos'  # Puedes ajustar esto según tus necesidades

        # Obtén el valor del parámetro 'size' de la consulta, utilizando 10 como valor predeterminado si no se proporciona
        size = int(request.GET.get('size', 10))#cantidad de registros
        sort_parameter = request.GET.get('sort', 'product_id')  # Parámetro para ordenar, por defecto 'product_id'
        start_date = request.GET.get('start_date', None)  # Parámetro para la fecha de inicio del rango
        end_date = request.GET.get('end_date', None)  # Parámetro para la fecha de fin del rango
        product_id = request.GET.get('product_id', None)  # Parámetro para la búsqueda por product_id
        search_user = request.GET.get('search_user', None)  # Parámetro para la búsqueda de usuarios

        # mejores score
        order_direction_best_score = 'DESC'
        # peores score
        order_direction_worst_score = 'ASC'

        # Parámetros para las consultas SQL
        sql_params = [start_date, end_date, 
                    product_id, f"{product_id}%", f"%{product_id}", f"%{product_id}%" ,
                    search_user,f"%{search_user}%", f"%{search_user}", f"{search_user}%",
                    search_user,f"%{search_user}%", f"%{search_user}", f"{search_user}%",
                    size]



        # Imprimir el valor del parámetro para depurar
        #print(f'Tamaño recibido: {size}')

        # Usa la conexión predeterminada definida en settings.py
        with connection.cursor() as cursor:
            ##MEJORES
            # Construir la consulta SQL con LIMIT, OFFSET y ORDER BY dinámicos usando parámetros
            query_best = f"""SELECT 
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
                    {table_name}
                WHERE
                   (COALESCE(%s, review_time) <= review_time) AND
                   (COALESCE(%s, review_time) >= review_time)
                    AND (%s IS NULL OR product_id LIKE %s OR product_id LIKE %s OR product_id LIKE %s)
                    AND (
                        (%s IS NULL OR user_id LIKE %s OR user_id LIKE %s OR user_id LIKE %s)
                        OR (%s IS NULL OR profile_name LIKE %s OR profile_name LIKE %s OR profile_name LIKE %s)
                        )
                ORDER BY {sort_parameter} {order_direction_best_score}
                LIMIT %s 
            """
            # Ejecutar la consulta SQL con parámetros
            cursor.execute(query_best,sql_params)
            # Obtener los resultados de la consulta
            results_best = cursor.fetchall()
            # Obtener los nombres de las columnas desde el cursor.description
            column_names_best = [col[0] for col in cursor.description]

            #PEORES
            # Construir la consulta SQL con LIMIT, OFFSET y ORDER BY dinámicos usando parámetros
            query_worst = f"""SELECT 
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
                    {table_name}
                WHERE
                   (COALESCE(%s, review_time) <= review_time) AND
                   (COALESCE(%s, review_time) >= review_time)
                    AND (%s IS NULL OR product_id LIKE %s OR product_id LIKE %s OR product_id LIKE %s)
                    AND (
                        (%s IS NULL OR user_id LIKE %s OR user_id LIKE %s OR user_id LIKE %s)
                        OR (%s IS NULL OR profile_name LIKE %s OR profile_name LIKE %s OR profile_name LIKE %s)
                        )
                ORDER BY {sort_parameter} {order_direction_worst_score}
                LIMIT %s 
            """

            # Ejecutar la consulta SQL con parámetros
            cursor.execute(query_worst,sql_params)
            # Obtener los resultados de la consulta
            results_worst = cursor.fetchall()
            # Obtener los nombres de las columnas desde el cursor.description
            column_names_worst = [col[0] for col in cursor.description]


            # Obtener el número total de registros
            statistics_query = f"""
                SELECT 
                COUNT(DISTINCT user_id) AS amount_user,
                MAX(score) AS max_score,
                MIN(score) AS min_score,
                AVG(score) AS avg_score
                FROM {table_name} 
                WHERE
                   (COALESCE(%s, review_time) <= review_time) AND
                   (COALESCE(%s, review_time) >= review_time)
                    AND (%s IS NULL OR product_id LIKE %s OR product_id LIKE %s OR product_id LIKE %s)
                    AND (
                        (%s IS NULL OR user_id LIKE %s OR user_id LIKE %s OR user_id LIKE %s)
                        OR (%s IS NULL OR profile_name LIKE %s OR profile_name LIKE %s OR profile_name LIKE %s)
                        )
                """

        
            cursor.execute(statistics_query, sql_params[:14])  # Solo se necesitan los primeros 6 parámetros
            results_statistics = cursor.fetchall()
            # Obtener los nombres de las columnas desde el cursor.description
            column_names_statistics = [col[0] for col in cursor.description]
            
    
        # Formatear los resultados de manera automapeable usando los nombres de las columnas
        data_best_scores = [dict(zip(column_names_best, row)) for row in results_best]
        data_worst_scores = [dict(zip(column_names_worst, row)) for row in results_worst]
        data_statistics = [dict(zip(column_names_statistics, row)) for row in results_statistics]
        
            
            
        # Formatear los resultados como desees (aquí, simplemente se devuelven como JSON)
        #data = [{'product_id': row[0], 'categories': row[1], 'user_id': row[2]} for row in results]

        # Construir la respuesta estructurada
        response_data = {
            "data_best_scores": data_best_scores,#mejores
            "data_worst_scores": data_worst_scores,#peores
            "statistics":data_statistics[0]
        }


        # Devolver los datos como una respuesta JSON
        return JsonResponse(response_data, safe=False)
    except DatabaseError as e:
        # Maneja cualquier error de conexión aquí
        return JsonResponse({'error': str(e)}, status=500)
        
@handle_errors
def data_timeline(request):


    size, page, sort_parameter = RequestParamsHelper.get_pagination_params(request)
    start_date, end_date, product_id, search_user = RequestParamsHelper.get_filter_params(request)


    # Calcula el offset para la consulta SQL
    offset = (page - 1) * size

    # Verifica si se proporciona un orden ascendente o descendente (por defecto, ascendente)
    order_direction = RequestParamsHelper.get_order_direction(request)

    # Parámetros para las consultas SQL
    sql_params = [start_date, end_date, 
                product_id, f"{product_id}%", f"%{product_id}", f"%{product_id}%" ,
                search_user,f"%{search_user}%", f"%{search_user}", f"{search_user}%",
                search_user,f"%{search_user}%", f"%{search_user}", f"{search_user}%",
                size, offset]

    # Usa el método de MoviesRepository para obtener la línea de tiempo
    data = MoviesRepository.get_timeline(sql_params,sort_parameter,order_direction)

    # Construir la respuesta estructurada
    response_data = {
        "current_page": page,
        "data": data,
    }

    # Devolver los datos como una respuesta JSON
    return JsonResponse(response_data, safe=False)
