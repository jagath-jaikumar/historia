from django.db import models
from pgvector.django import VectorField


class Document(models.Model):
    title = models.CharField(max_length=255)
    content = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)


class WikipediaDocument(Document):
    url = models.URLField(unique=True)
    metadata = models.JSONField(default=dict)


class Index(models.Model):
    name = models.CharField(max_length=255, unique=True)
    dimensions = models.IntegerField(default=768)  # Default to BERT base dimensions


class Embedding(models.Model):
    embedding = VectorField(dimensions=None)  # Allow variable dimensions
    dimensions = models.IntegerField()  # Store the actual dimensions used


class IndexedDocumentSnippet(models.Model):
    index = models.ForeignKey(Index, on_delete=models.CASCADE)
    document = models.ForeignKey(Document, on_delete=models.CASCADE)
    snippet = models.TextField()
    embedding = models.ForeignKey(Embedding, on_delete=models.CASCADE, null=True)

    class Meta:
        unique_together = ("index", "document", "snippet")
