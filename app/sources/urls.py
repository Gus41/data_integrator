from django.urls import path
from . import views

urlpatterns = [
    path('', views.IntegrationListView.as_view(), name='home'),
    path('integrations/new/', views.IntegrationCreateView.as_view(), name='integration_create'),
    path('integrations/<int:pk>/', views.IntegrationDetailView.as_view(), name='integration_detail'),
    path('integrations/<int:pk>/delete/', views.IntegrationDeleteView.as_view(), name='integration_delete'),
    path('integrations/<int:pk>/sources/', views.IntegrationSourcesUpdateView.as_view(), name='integration_sources_update'),
    path('integrations/<int:pk>/configure/', views.IntegrationConfigureView.as_view(), name='integration_configure'),
]