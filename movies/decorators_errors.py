from django.http import JsonResponse
from django.db.utils import DatabaseError
from django.http import JsonResponse
from django.core.exceptions import ObjectDoesNotExist

def handle_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except DatabaseError as db_error:
            return JsonResponse({'error': f'DatabaseError: {db_error}'}, status=500)
        except ObjectDoesNotExist as other_exception:
            return JsonResponse({'error': f'ObjectDoesNotExist: {other_exception}'}, status=500)
        except Exception as generic_error:
            return JsonResponse({'error': f'Unexpected Error: {generic_error}'}, status=500)
    return wrapper