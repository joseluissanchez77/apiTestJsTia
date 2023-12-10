class RequestParamsHelper:
    @staticmethod
    def get_pagination_params(request):
        size = int(request.GET.get('size', 10))#cantidad de registros
        page = int(request.GET.get('page', 1))#pagina
        sort_parameter = request.GET.get('sort', 'product_id')# Parámetro para ordenar, por defecto 'product_id'
        return size, page, sort_parameter

    @staticmethod
    def get_filter_params(request):
        start_date = request.GET.get('start_date', None) # Parámetro para la fecha de inicio del rango
        end_date = request.GET.get('end_date', None)# Parámetro para la fecha de fin del rango
        product_id = request.GET.get('product_id', None) # Parámetro para la búsqueda por product_id
        search_user = request.GET.get('search_user', None)# Parámetro para la búsqueda de usuarios
        return start_date, end_date, product_id, search_user

    @staticmethod
    def get_order_direction(request):
        # Verifica si se proporciona un orden ascendente o descendente (por defecto, ascendente)
        return 'ASC' if request.GET.get('order', 'asc').lower() == 'asc' else 'DESC'
