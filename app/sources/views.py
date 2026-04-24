from django.shortcuts import render, get_object_or_404, redirect
from django.views import View
from .models import Integration, DataSource
from .forms import IntegrationForm
from django.contrib import messages

class IntegrationListView(View):
    def get(self, request):
        integrations = Integration.objects.all()
        recent = integrations[:4]
        all_integrations = integrations[4:]
        form = IntegrationForm()
        return render(request, 'index.html', {
            'recent': recent,
            'all_integrations': all_integrations,
            'form': form,
        })


class IntegrationCreateView(View):
    def post(self, request):
        form = IntegrationForm(request.POST)
        if form.is_valid():
            integration = form.save()
   
            DataSource.objects.create(
                integration=integration,
                label='A',
                data_type=request.POST.get('source_a_type'),
                origin=request.POST.get('source_a_origin'),
            )
   
            DataSource.objects.create(
                integration=integration,
                label='B',
                data_type=request.POST.get('source_b_type'),
                origin=request.POST.get('source_b_origin'),
            )
            return redirect('integration_detail', pk=integration.pk)
        return redirect('home')


class IntegrationDetailView(View):
    def get(self, request, pk):
        integration = get_object_or_404(Integration, pk=pk)
        sources = integration.sources.all()
        return render(request, 'detail.html', {
            'integration': integration,
            'sources': sources,
        })
class IntegrationConfigureView(View):
    def get(self, request, pk):
        integration = get_object_or_404(Integration, pk=pk)
        return render(request, 'configure.html', {
            'integration': integration,
        })

class IntegrationSourcesUpdateView(View):
    def post(self, request, pk):
        integration = get_object_or_404(Integration, pk=pk)

        for source in integration.sources.all():
            label = source.label

            # UPLOAD DE ARQUIVO
            file_key = f'file_{label}'
            if file_key in request.FILES:
                source.file = request.FILES[file_key]

            # URL / STRING DE CONEXÃO
            url_key = f'url_{label}'
            if request.POST.get(url_key):
                source.connection_string = request.POST[url_key]

            # HEADERS (para API endpoints)
            headers_key = f'headers_{label}'
            if request.POST.get(headers_key):
                source.headers = request.POST[headers_key]

            source.save()

        messages.success(request, 'Fontes salvas com sucesso.')
        return redirect('integration_detail', pk=integration.pk)


class IntegrationDeleteView(View):
    def post(self, request, pk):
        integration = get_object_or_404(Integration, pk=pk)
        integration.delete()
        return redirect('home')