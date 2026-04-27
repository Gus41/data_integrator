from django.shortcuts import render, get_object_or_404, redirect
from django.views import View
from .models import Integration, DataSource, JoinConfig
from .forms import IntegrationForm
from django.contrib import messages
from .parser import extract_columns, execute_pipeline, make_preview, build_chart_data
import json

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
        chart_data = integration.chart_data or {}
        pipeline_columns = integration.result_columns or []
        preview = integration.result_preview or []
        pipeline_rows = [[row.get(col) for col in pipeline_columns] for row in preview]
        return render(request, 'detail.html', {
            'integration': integration,
            'sources': sources,
            'pipeline_preview': pipeline_rows,
            'pipeline_columns': pipeline_columns,
            'chart_data_json': json.dumps(chart_data),
        })


class IntegrationConfigureView(View):
    def get(self, request, pk):
        integration = get_object_or_404(Integration, pk=pk)
 
        sources_data = []
        for source in integration.sources.all():
            try:
                cols = extract_columns(source)
                error = None
            except Exception as e:
                cols = []
                error = str(e)
            sources_data.append({
                'source': source,
                'columns': cols,
                'error': error,
            })
 
        existing = getattr(integration, 'join_config', None)
 
        return render(request, 'configure.html', {
            'integration': integration,
            'sources_data': sources_data,
            'columns_a': next((s['columns'] for s in sources_data if s['source'].label == 'A'), []),
            'columns_b': next((s['columns'] for s in sources_data if s['source'].label == 'B'), []),
            'existing': existing,
            'join_types': JoinConfig.JOIN_CHOICES,
        })
 
    def post(self, request, pk):
        integration = get_object_or_404(Integration, pk=pk)
 
        key_a     = request.POST.get('key_source_a')
        key_b     = request.POST.get('key_source_b')
        join_type = request.POST.get('join_type', 'inner')
        cols      = request.POST.getlist('columns_to_keep')
 
        JoinConfig.objects.update_or_create(
            integration=integration,
            defaults={
                'key_source_a':     key_a,
                'key_source_b':     key_b,
                'join_type':        join_type,
                'columns_to_keep':  cols,
            }
        )
 
        print(f"[DEBUG] configure post integration={integration.pk} name={integration.name}")
        print(f"[DEBUG] selected keys A={key_a} B={key_b} join_type={join_type}")
        print(f"[DEBUG] selected columns_to_keep={cols}")
        for source in integration.sources.all():
            print(f"[DEBUG] source={source.label} origin={source.origin} type={source.data_type} conn={source.connection_string} file={source.file}")

        try:
            result = execute_pipeline(integration)
            integration.status = 'done'
            integration.record_count = len(result)
            integration.result_columns = list(result.columns)
            integration.result_preview = make_preview(result)
            integration.chart_data = build_chart_data(result)
            integration.save()
            messages.success(request, 'Pipeline executado com sucesso. Veja o resultado abaixo.')
            return redirect('integration_detail', pk=integration.pk)
        except Exception as e:
            print(f"[DEBUG] execute_pipeline failed integration={integration.pk} error={e}")
            integration.status = 'error'
            integration.save()
            messages.error(request, f'Erro ao executar pipeline: {e}')
            return redirect('integration_configure', pk=pk)
 



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