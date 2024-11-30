# utils/django_setup.py
import os

import django
from django.conf import settings


def initialize_django():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "historia.historia.settings")
    django.setup()
