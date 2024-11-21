from django.db import models


class Question(models.Model):
    query = models.TextField()

    def __str__(self):
        return self.query


class Generation(models.Model):
    question = models.ForeignKey(
        Question, on_delete=models.CASCADE, related_name="generations"
    )
    config_name = models.CharField(max_length=255)
    output_text = models.TextField()
    metadata = models.JSONField(default=dict, blank=True)

    def __str__(self):
        return f"Generation for '{self.question}' using {self.config_name}"
