from django.db import models

class Integration(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pendente'),
        ('processing', 'Em progresso'),
        ('done', 'Completo'),
        ('error', 'Erro'),
    ]

    name = models.CharField(max_length=255)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    record_count = models.IntegerField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ['-created_at']


class DataSource(models.Model):
    TYPE_CHOICES = [
        ('csv', 'CSV'),
        ('json', 'JSON'),
        ('api', 'API REST'),
        ('sql', 'Banco SQL'),
    ]

    ORIGIN_CHOICES = [
        ('upload', 'Upload de arquivo'),
        ('url', 'URL remota'),
        ('endpoint', 'URL do endpoint'),
        ('database', 'Banco de dados'),
    ]

    LABEL_CHOICES = [
        ('A', 'Fonte A'),
        ('B', 'Fonte B'),
    ]

    integration = models.ForeignKey(
        Integration,
        on_delete=models.CASCADE,
        related_name='sources'
    )
    label = models.CharField(max_length=1, choices=LABEL_CHOICES)
    data_type = models.CharField(max_length=10, choices=TYPE_CHOICES)
    origin = models.CharField(max_length=20, choices=ORIGIN_CHOICES)

    # para upload de arquivo
    file = models.FileField(upload_to='uploads/', null=True, blank=True)

    # para URL/API/banco
    connection_string = models.TextField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.integration.name} — Fonte {self.label}"

    class Meta:
        unique_together = ('integration', 'label')