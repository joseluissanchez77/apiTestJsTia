from django.shortcuts import render

# Create your views here.

from django.http import JsonResponse
from django.db import connection
from django.db.utils import DatabaseError
from .repository import MoviesRepository
from .params_helper import RequestParamsHelper
from .decorators_errors import handle_errors


import json



@handle_errors
def data_movies(request):

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

    data =  MoviesRepository.get_data_review(sql_params,sort_parameter,order_direction)
    count_query =  MoviesRepository.get_data_total_register(sql_params)

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

        
@handle_errors
def data_additional(request):

    size, page, sort_parameter = RequestParamsHelper.get_pagination_params(request)
    start_date, end_date, product_id, search_user = RequestParamsHelper.get_filter_params(request)

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


    # Formatear los resultados de manera automapeable usando los nombres de las columnas
    # MEJORES
    data_best_scores = MoviesRepository.get_data_classification(sql_params,sort_parameter,order_direction_best_score)
    # PEORES
    data_worst_scores = MoviesRepository.get_data_classification(sql_params,sort_parameter,order_direction_worst_score)
    #estadisticas
    data_statistics = MoviesRepository.get_data_statistics(sql_params)
    
        
    # Construir la respuesta estructurada
    response_data = {
        "data_best_scores": data_best_scores,#mejores
        "data_worst_scores": data_worst_scores,#peores
        "statistics":data_statistics[0]
    }

    # Devolver los datos como una respuesta JSON
    return JsonResponse(response_data, safe=False)

        
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
