# analytics/admin.py

from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse, path
from django.shortcuts import render, redirect
from django.contrib import messages
from django.core.management import call_command
from django.http import HttpResponseRedirect
from django.utils import timezone
from datetime import timedelta
import threading

from .models import DataProfilingReport, DataQualityMetric, ReportGenerationLog


class DataQualityMetricInline(admin.TabularInline):
    model = DataQualityMetric
    extra = 0
    readonly_fields = ['metric_name', 'metric_category', 'metric_value', 'metric_unit', 'is_within_threshold']
    fields = ['metric_name', 'metric_category', 'metric_value', 'metric_unit', 'table_name', 'column_name',
              'is_within_threshold']

    def has_add_permission(self, request, obj=None):
        return False


class ReportGenerationLogInline(admin.TabularInline):
    model = ReportGenerationLog
    extra = 0
    readonly_fields = ['requested_at', 'status', 'duration_display', 'error_message']
    fields = ['requested_at', 'status', 'duration_display', 'error_message']

    def has_add_permission(self, request, obj=None):
        return False


@admin.register(DataProfilingReport)
class DataProfilingReportAdmin(admin.ModelAdmin):
    list_display = [
        'id',
        'countries_display_short',
        'analysis_period',
        'record_count_formatted',
        'report_type',
        'status_display',
        'generated_at',
        'view_report_link'
    ]

    list_filter = [
        'status',
        'report_type',
        ('generated_at', admin.DateFieldListFilter),
        'countries'
    ]

    search_fields = ['countries']
    date_hierarchy = 'generated_at'
    ordering = ['-generated_at']

    readonly_fields = [
        'report_url',
        'record_count',
        'generated_at',
        'analysis_period_days',
        'file_size_mb',
        'view_report_link'
    ]

    fieldsets = (
        ('Report Information', {
            'fields': ('status', 'report_type', 'view_report_link')
        }),
        ('Analysis Scope', {
            'fields': ('countries', 'start_date', 'end_date', 'analysis_period_days')
        }),
        ('Report Details', {
            'fields': ('record_count', 'file_size_mb', 'report_url')
        }),
        ('Metadata', {
            'fields': ('generated_at', 'generated_by'),
            'classes': ('collapse',)
        })
    )

    inlines = [DataQualityMetricInline, ReportGenerationLogInline]

    actions = ['generate_new_report']

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('generate-report/', self.admin_site.admin_view(self.generate_report_view),
                 name='analytics_generate_report'),
        ]
        return custom_urls + urls

    def countries_display_short(self, obj):
        if not obj.countries:
            return "All Countries"
        if len(obj.countries) <= 3:
            return ', '.join(obj.countries)
        return f"{', '.join(obj.countries[:2])} +{len(obj.countries) - 2} more"

    countries_display_short.short_description = "Countries"

    def analysis_period(self, obj):
        days = obj.analysis_period_days
        return f"{obj.start_date.strftime('%Y-%m-%d')} to {obj.end_date.strftime('%Y-%m-%d')} ({days} days)"

    analysis_period.short_description = "Analysis Period"

    def record_count_formatted(self, obj):
        return f"{obj.record_count:,}"

    record_count_formatted.short_description = "Records"
    record_count_formatted.admin_order_field = 'record_count'

    def status_display(self, obj):
        color_map = {
            'completed': 'green',
            'generating': 'orange',
            'failed': 'red'
        }
        color = color_map.get(obj.status, 'gray')
        return format_html(
            '<span style="color: {};">{}</span>',
            color, obj.get_status_display()
        )

    status_display.short_description = "Status"

    def view_report_link(self, obj):
        if obj.report_url and obj.status == 'completed':
            return format_html(
                '<a href="{}" target="_blank" class="button">View Report</a>',
                obj.report_url
            )
        return "-"

    view_report_link.short_description = "Report"

    def generate_new_report(self, request, queryset):
        """Admin action to generate new reports"""
        return HttpResponseRedirect(reverse('admin:analytics_generate_report'))

    generate_new_report.short_description = "Generate New Profiling Report"

    def generate_report_view(self, request):
        """Custom view for generating reports"""
        if request.method == 'POST':
            # Extract form data
            countries = request.POST.get('countries', '').strip()
            start_date = request.POST.get('start_date')
            end_date = request.POST.get('end_date')
            report_type = request.POST.get('report_type', 'minimal')
            sample_size = request.POST.get('sample_size', '50000')

            # Build command arguments
            cmd_args = [
                '--report-type', report_type,
                '--sample-size', sample_size
            ]

            if countries:
                cmd_args.extend(['--countries', countries])
            if start_date:
                cmd_args.extend(['--start-date', start_date])
            if end_date:
                cmd_args.extend(['--end-date', end_date])

            # Create generation log
            log = ReportGenerationLog.objects.create(
                requested_by=request.user,
                countries_requested=countries.split(',') if countries else [],
                start_date_requested=timezone.now() - timedelta(days=30),
                end_date_requested=timezone.now(),
                report_type_requested=report_type,
                started_at=timezone.now()
            )

            # Start report generation in background
            def generate_report_async():
                try:
                    call_command('generate_data_profile', *cmd_args)
                    log.status = ReportGenerationLog.SUCCESS
                    log.completed_at = timezone.now()
                    log.save()
                except Exception as e:
                    log.status = ReportGenerationLog.FAILED
                    log.error_message = str(e)
                    log.completed_at = timezone.now()
                    log.save()

            # Start background thread
            thread = threading.Thread(target=generate_report_async)
            thread.daemon = True
            thread.start()

            messages.success(
                request,
                'Report generation started! Check back in a few minutes to view the results.'
            )
            return redirect('admin:analytics_dataprofilingreport_changelist')

        # GET request - show form
        context = {
            'title': 'Generate Data Profiling Report',
            'opts': self.model._meta,
            'has_view_permission': True,
        }
        return render(request, 'admin/analytics/generate_report.html', context)


@admin.register(DataQualityMetric)
class DataQualityMetricAdmin(admin.ModelAdmin):
    list_display = [
        'report_link',
        'metric_name',
        'metric_category',
        'metric_value_display',
        'table_name',
        'column_name',
        'threshold_status'
    ]

    list_filter = [
        'metric_category',
        'is_within_threshold',
        'table_name',
        ('report__generated_at', admin.DateFieldListFilter)
    ]

    search_fields = ['metric_name', 'table_name', 'column_name']
    ordering = ['-report__generated_at', 'metric_category', 'metric_name']

    def report_link(self, obj):
        url = reverse('admin:analytics_dataprofilingreport_change', args=[obj.report.pk])
        return format_html('<a href="{}">{}</a>', url, obj.report.id)

    report_link.short_description = "Report"

    def metric_value_display(self, obj):
        return f"{obj.metric_value}{obj.metric_unit}"

    metric_value_display.short_description = "Value"

    def threshold_status(self, obj):
        if obj.threshold_value is None:
            return "-"

        if obj.is_within_threshold:
            return format_html('<span style="color: green;">✓ Within Threshold</span>')
        else:
            return format_html('<span style="color: red;">✗ Outside Threshold</span>')

    threshold_status.short_description = "Threshold Status"


@admin.register(ReportGenerationLog)
class ReportGenerationLogAdmin(admin.ModelAdmin):
    list_display = [
        'id',
        'requested_by',
        'requested_at',
        'countries_requested_display',
        'report_type_requested',
        'status_display',
        'duration_display',
        'report_link'
    ]

    list_filter = [
        'status',
        'report_type_requested',
        ('requested_at', admin.DateFieldListFilter)
    ]

    search_fields = ['requested_by__username', 'countries_requested']
    date_hierarchy = 'requested_at'
    ordering = ['-requested_at']

    readonly_fields = [
        'requested_at', 'started_at', 'completed_at',
        'data_extraction_seconds', 'report_generation_seconds',
        'upload_seconds', 'total_seconds'
    ]

    def countries_requested_display(self, obj):
        if not obj.countries_requested:
            return "All Countries"
        return ', '.join(obj.countries_requested)

    countries_requested_display.short_description = "Countries"

    def status_display(self, obj):
        color_map = {
            'success': 'green',
            'in_progress': 'orange',
            'failed': 'red'
        }
        color = color_map.get(obj.status, 'gray')
        return format_html(
            '<span style="color: {};">{}</span>',
            color, obj.get_status_display()
        )

    status_display.short_description = "Status"

    def report_link(self, obj):
        if obj.report:
            url = reverse('admin:analytics_dataprofilingreport_change', args=[obj.report.pk])
            return format_html('<a href="{}">View Report #{}</a>', url, obj.report.id)
        return "-"

    report_link.short_description = "Generated Report"


# Customize admin site header
admin.site.site_header = "Energy Forecasting Analytics"
admin.site.site_title = "Energy Analytics Admin"
admin.site.index_title = "Energy Data Management"
