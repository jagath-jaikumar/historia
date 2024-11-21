from rest_framework import viewsets, serializers
from .models import Generation, Question


class GenerationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Generation
        fields = ["id", "question", "config_name", "output_text", "metadata"]


class QuestionSerializer(serializers.ModelSerializer):
    generations = GenerationSerializer(many=True, read_only=True)

    class Meta:
        model = Question
        fields = ["id", "query", "generations"]


class GenerationViewSet(viewsets.ModelViewSet):
    queryset = Generation.objects.all()
    serializer_class = GenerationSerializer


class QuestionViewSet(viewsets.ModelViewSet):
    queryset = Question.objects.all()
    serializer_class = QuestionSerializer
