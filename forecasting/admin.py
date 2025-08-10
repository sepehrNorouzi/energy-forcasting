from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe
from .models import ForecastModel, EnergyForecast, ModelPerformanceMetric


class EnergyForecastInline(admin.TabularInline):
    model = EnergyForecast
    extra = 0
    readonly_fields = ['forecast_error', 'forecast_accuracy_percent']
    fields = [
        'target_timestamp',
        'predicted_value',
        'actual_value',
        'forecast_error',
        'forecast_accuracy_percent'
    ]

    def get_queryset(self, request):
        return super().get_queryset(request).order_by('-target_timestamp')[:10]


class ModelPerformanceMetricInline(admin.TabularInline):
    model = ModelPerformanceMetric
    extra = 0
    readonly_fields = ['evaluation_date', 'mae', 'rmse', 'mape', 'forecast_count']

    def get_queryset(self, request):
        return super().get_queryset(request).order_by('-evaluation_date')[:5]


@admin.register(ForecastModel)
class ForecastModelAdmin(admin.ModelAdmin):
    list_display = [
        'name',
        'model_type',
        'country_code',
        'target_variable',
        'version',
        'is_active_status',
        'performance_summary',
        'training_period'
    ]
    list_filter = [
        'model_type',
        'country_code',
        'target_variable',
        'is_active',
        ('created_at', admin.DateFieldListFilter),
    ]
    search_fields = ['name', 'country_code', 'target_variable']
    ordering = ['-created_at', 'name']
    readonly_fields = [
        'created_at',
        'updated_at',
        'performance_summary',
        'training_period',
        'forecast_count'
    ]
    inlines = [ModelPerformanceMetricInline, EnergyForecastInline]

    fieldsets = (
        ('Model Information', {
            'fields': ('name', 'model_type', 'version', 'is_active')
        }),
        ('Target & Features', {
            'fields': ('country_code', 'target_variable', 'feature_columns')
        }),
        ('Training Configuration', {
            'fields': ('parameters', 'training_start_date', 'training_end_date', 'training_samples')
        }),
        ('Performance Metrics', {
            'fields': ('mae', 'rmse', 'mape', 'r2_score', 'performance_summary')
        }),
        ('Model Storage', {
            'fields': ('model_file_path',)
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at', 'training_period', 'forecast_count'),
            'classes': ('collapse',)
        })
    )

    def is_active_status(self, obj):
        if obj.is_active:
            return format_html('<span style="color: green;">✓ Active</span>')
        else:
            return format_html('<span style="color: red;">✗ Inactive</span>')

    is_active_status.short_description = "Status"

    def performance_summary(self, obj):
        if obj.mae and obj.rmse:
            return f"MAE: {obj.mae:.2f}, RMSE: {obj.rmse:.2f}"
        return "-"

    performance_summary.short_description = "Performance"

    def training_period(self, obj):
        if obj.training_start_date and obj.training_end_date:
            days = (obj.training_end_date - obj.training_start_date).days
            return f"{obj.training_start_date.strftime('%Y-%m-%d')} to {obj.training_end_date.strftime('%Y-%m-%d')} ({days} days)"
        return "-"

    training_period.short_description = "Training Period"

    def forecast_count(self, obj):
        return obj.forecasts.count()

    forecast_count.short_description = "Total Forecasts"

    actions = ['activate_models', 'deactivate_models']

    def activate_models(self, request, queryset):
        updated = queryset.update(is_active=True)
        self.message_user(request, f"Activated {updated} models")

    activate_models.short_description = "Activate selected models"

    def deactivate_models(self, request, queryset):
        updated = queryset.update(is_active=False)
        self.message_user(request, f"Deactivated {updated} models")

    deactivate_models.short_description = "Deactivate selected models"


@admin.register(EnergyForecast)
class EnergyForecastAdmin(admin.ModelAdmin):
    list_display = [
        'model_link',
        'country_code',
        'target_timestamp',
        'horizon_hours',
        'predicted_value_formatted',
        'actual_value_formatted',
        'forecast_accuracy_display',
        'forecast_age'
    ]
    list_filter = [
        'country_code',
        'horizon_hours',
        'model__model_type',
        ('target_timestamp', admin.DateFieldListFilter),
    ]
    search_fields = ['model__name', 'country_code']
    date_hierarchy = 'target_timestamp'
    ordering = ['-target_timestamp', 'country_code']
    readonly_fields = [
        'created_at',
        'forecast_error',
        'forecast_accuracy_percent',
        'forecast_age'
    ]

    fieldsets = (
        ('Forecast Information', {
            'fields': ('model', 'country_code', 'horizon_hours')
        }),
        ('Timing', {
            'fields': ('forecast_timestamp', 'target_timestamp', 'forecast_age')
        }),
        ('Predictions', {
            'fields': ('predicted_value', 'confidence_lower', 'confidence_upper')
        }),
        ('Actual & Evaluation', {
            'fields': ('actual_value', 'forecast_error', 'forecast_accuracy_percent')
        }),
        ('Metadata', {
            'fields': ('created_at',),
            'classes': ('collapse',)
        })
    )

    def model_link(self, obj):
        url = reverse('admin:forecasting_forecastmodel_change', args=[obj.model.pk])
        return format_html('<a href="{}">{}</a>', url, obj.model.name)

    model_link.short_description = "Model"
    model_link.admin_order_field = 'model__name'

    def predicted_value_formatted(self, obj):
        return f"{obj.predicted_value:,.0f}"

    predicted_value_formatted.short_description = "Predicted"
    predicted_value_formatted.admin_order_field = 'predicted_value'

    def actual_value_formatted(self, obj):
        if obj.actual_value:
            return f"{obj.actual_value:,.0f}"
        return "-"

    actual_value_formatted.short_description = "Actual"
    actual_value_formatted.admin_order_field = 'actual_value'

    def forecast_accuracy_display(self, obj):
        accuracy = obj.forecast_accuracy_percent
        if accuracy is not None:
            color = "green" if accuracy > 95 else "orange" if accuracy > 90 else "red"
            return format_html(
                '<span style="color: {};">{:.1f}%</span>',
                color, accuracy
            )
        return "-"

    forecast_accuracy_display.short_description = "Accuracy"

    def forecast_age(self, obj):
        from django.utils import timezone
        age = timezone.now() - obj.forecast_timestamp
        hours = age.total_seconds() / 3600
        if hours < 24:
            return f"{hours:.1f}h"
        else:
            return f"{hours / 24:.1f}d"

    forecast_age.short_description = "Age"


@admin.register(ModelPerformanceMetric)
class ModelPerformanceMetricAdmin(admin.ModelAdmin):
    list_display = [
        'model_link',
        'evaluation_date',
        'evaluation_period_days',
        'mae',
        'rmse',
        'mape_formatted',
        'forecast_count'
    ]
    list_filter = [
        'model__country_code',
        'model__model_type',
        ('evaluation_date', admin.DateFieldListFilter),
        'evaluation_period_days'
    ]
    search_fields = ['model__name']
    date_hierarchy = 'evaluation_date'
    ordering = ['-evaluation_date', 'model__name']
    readonly_fields = ['created_at']

    def model_link(self, obj):
        url = reverse('admin:forecasting_forecastmodel_change', args=[obj.model.pk])
        return format_html('<a href="{}">{}</a>', url, obj.model.name)

    model_link.short_description = "Model"
    model_link.admin_order_field = 'model__name'

    def mape_formatted(self, obj):
        color = "green" if obj.mape < 5 else "orange" if obj.mape < 10 else "red"
        return format_html(
            '<span style="color: {};">{:.2f}%</span>',
            color, obj.mape
        )

    mape_formatted.short_description = "MAPE"
    mape_formatted.admin_order_field = 'mape'
