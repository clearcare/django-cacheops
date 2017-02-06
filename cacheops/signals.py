import django.dispatch

cache_read = django.dispatch.Signal(providing_args=["func", "hit", "age", "cache_key"])
cache_invalidation = django.dispatch.Signal(providing_args=["model_name", "obj_dict", "deleted", "duration"])
