# utils/django_setup.py
import os

import django


def initialize_django():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "historia.historia.settings")
    django.setup()

    from django.conf import settings
    print(f"Current database host: {settings.DATABASES['default']['HOST']}")
