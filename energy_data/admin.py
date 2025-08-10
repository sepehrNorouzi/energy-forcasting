from django.contrib import admin
from django.utils.html import format_html
from django.db.models import Count, Avg
from .models import LoadData, RenewableGeneration, EnergyPrice, DataImportLog


@admin.register(LoadData)
class LoadDataAdmin(admin.ModelAdmin):
    list_display = [
        'country_code',
        'utc_timestamp',
        'actual_load_mw_formatted',
        'forecast_load_mw_formatted',
        'forecast_accuracy'
    ]
    list_filter = [
        'country_code',
        ('utc_timestamp', admin.DateFieldListFilter),
        'created_at'
    ]
    search_fields = ['country_code']
    date_hierarchy = 'utc_timestamp'
    ordering = ['-utc_timestamp', 'country_code']
    readonly_fields = ['created_at', 'updated_at', 'forecast_accuracy']

    fieldsets = (
        ('Time Information', {
            'fields': ('utc_timestamp', 'cet_cest_timestamp')
        }),
        ('Location', {
            'fields': ('country_code',)
        }),
        ('Load Data', {
            'fields': ('actual_load_mw', 'forecast_load_mw', 'forecast_accuracy')
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )

    def actual_load_mw_formatted(self, obj):
        if obj.actual_load_mw:
            return f"{obj.actual_load_mw:,.0f} MW"
        return "-"

    actual_load_mw_formatted.short_description = "Actual Load"
    actual_load_mw_formatted.admin_order_field = 'actual_load_mw'

    def forecast_load_mw_formatted(self, obj):
        if obj.forecast_load_mw:
            return f"{obj.forecast_load_mw:,.0f} MW"
        return "-"

    forecast_load_mw_formatted.short_description = "Forecast Load"
    forecast_load_mw_formatted.admin_order_field = 'forecast_load_mw'

    def forecast_accuracy(self, obj):
        if obj.actual_load_mw and obj.forecast_load_mw:
            error_pct = abs(obj.actual_load_mw - obj.forecast_load_mw) / obj.actual_load_mw * 100
            accuracy = max(0, 100 - error_pct)
            color = "green" if accuracy > 95 else "orange" if accuracy > 90 else "red"
            return format_html(
                '<span style="color: {};">{:.1f}%</span>',
                color, accuracy
            )
        return "-"

    forecast_accuracy.short_description = "Forecast Accuracy"

    actions = ['export_selected_data']

    def export_selected_data(self, request, queryset):
        # Custom action to export data (could implement CSV export)
        self.message_user(request, f"Export functionality would process {queryset.count()} records")

    export_selected_data.short_description = "Export selected load data"


@admin.register(RenewableGeneration)
class RenewableGenerationAdmin(admin.ModelAdmin):
    list_display = [
        'country_code',
        'generation_type',
        'utc_timestamp',
        'actual_generation_mw_formatted',
        'capacity_mw_formatted',
        'capacity_factor_formatted'
    ]
    list_filter = [
        'country_code',
        'generation_type',
        ('utc_timestamp', admin.DateFieldListFilter),
    ]
    search_fields = ['country_code', 'generation_type']
    date_hierarchy = 'utc_timestamp'
    ordering = ['-utc_timestamp', 'country_code', 'generation_type']
    readonly_fields = ['created_at', 'updated_at']

    fieldsets = (
        ('Time Information', {
            'fields': ('utc_timestamp', 'cet_cest_timestamp')
        }),
        ('Location & Type', {
            'fields': ('country_code', 'generation_type')
        }),
        ('Generation Data', {
            'fields': ('actual_generation_mw', 'capacity_mw', 'capacity_factor')
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )

    def actual_generation_mw_formatted(self, obj):
        if obj.actual_generation_mw:
            return f"{obj.actual_generation_mw:,.0f} MW"
        return "-"

    actual_generation_mw_formatted.short_description = "Generation"
    actual_generation_mw_formatted.admin_order_field = 'actual_generation_mw'

    def capacity_mw_formatted(self, obj):
        if obj.capacity_mw:
            return f"{obj.capacity_mw:,.0f} MW"
        return "-"

    capacity_mw_formatted.short_description = "Capacity"
    capacity_mw_formatted.admin_order_field = 'capacity_mw'

    def capacity_factor_formatted(self, obj):
        if obj.capacity_factor:
            pct = obj.capacity_factor * 100
            color = "green" if pct > 50 else "orange" if pct > 25 else "red"
            return format_html(
                '<span style="color: {};">{:.1f}%</span>',
                color, pct
            )
        return "-"

    capacity_factor_formatted.short_description = "Capacity Factor"
    capacity_factor_formatted.admin_order_field = 'capacity_factor'


@admin.register(EnergyPrice)
class EnergyPriceAdmin(admin.ModelAdmin):
    list_display = [
        'country_code',
        'utc_timestamp',
        'day_ahead_price_formatted',
        'currency',
        'bidding_zone'
    ]
    list_filter = [
        'country_code',
        'currency',
        ('utc_timestamp', admin.DateFieldListFilter),
    ]
    search_fields = ['country_code', 'bidding_zone']
    date_hierarchy = 'utc_timestamp'
    ordering = ['-utc_timestamp', 'country_code']
    readonly_fields = ['created_at', 'updated_at']

    def day_ahead_price_formatted(self, obj):
        if obj.day_ahead_price:
            color = "red" if obj.day_ahead_price > 100 else "orange" if obj.day_ahead_price > 50 else "green"
            return format_html(
                '<span style="color: {};">{:.2f} {}</span>',
                color, obj.day_ahead_price, obj.currency
            )
        return "-"

    day_ahead_price_formatted.short_description = "Day-Ahead Price"
    day_ahead_price_formatted.admin_order_field = 'day_ahead_price'


@admin.register(DataImportLog)
class DataImportLogAdmin(admin.ModelAdmin):
    list_display = [
        'source',
        'import_timestamp',
        'data_period',
        'records_imported_formatted',
        'success_status'
    ]
    list_filter = [
        'source',
        'success',
        ('import_timestamp', admin.DateFieldListFilter),
    ]
    search_fields = ['file_name', 'source']
    date_hierarchy = 'import_timestamp'
    ordering = ['-import_timestamp']
    readonly_fields = ['import_timestamp']

    fieldsets = (
        ('Import Information', {
            'fields': ('source', 'import_timestamp', 'file_name')
        }),
        ('Data Period', {
            'fields': ('data_start_date', 'data_end_date')
        }),
        ('Import Results', {
            'fields': ('records_imported', 'records_updated', 'records_failed', 'success')
        }),
        ('Details', {
            'fields': ('import_parameters', 'error_log'),
            'classes': ('collapse',)
        })
    )

    def data_period(self, obj):
        return f"{obj.data_start_date.strftime('%Y-%m-%d')} to {obj.data_end_date.strftime('%Y-%m-%d')}"

    data_period.short_description = "Data Period"

    def records_imported_formatted(self, obj):
        total = obj.records_imported + obj.records_updated
        if obj.records_failed > 0:
            return format_html(
                '<span style="color: orange;">{:,} imported, {} failed</span>',
                total, obj.records_failed
            )
        return f"{total:,} records"

    records_imported_formatted.short_description = "Records"

    def success_status(self, obj):
        if obj.success:
            return format_html('<span style="color: green;">✓ Success</span>')
        else:
            return format_html('<span style="color: red;">✗ Failed</span>')

    success_status.short_description = "Status"
