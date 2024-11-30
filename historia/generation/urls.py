from django.urls import include, path
from rest_framework.routers import DefaultRouter

from . import views

router = DefaultRouter()
router.register(r"generations", views.GenerationViewSet)
router.register(r"questions", views.QuestionViewSet)

urlpatterns = [
    path("generation/", include(router.urls)),
]
