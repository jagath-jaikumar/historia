"""
URL configuration for historia project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from datetime import datetime

from django.contrib import admin
from django.http import JsonResponse
from django.urls import include, path


def server_info(request):
    return JsonResponse(
        {"datetime": datetime.now().isoformat(), "name": "Historia AI Server"}
    )


urlpatterns = [
    path("admin/", admin.site.urls),
    path(r"health/", include("health_check.urls")),
    path("api/", include("historia.generation.urls")),
    path("", server_info, name="server_info"),
]
