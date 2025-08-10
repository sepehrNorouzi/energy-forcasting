from django.contrib import admin
from django.utils.html import format_html
from .models import WeatherData, WeatherForecast

@admin.register(WeatherData)
class WeatherDataAdmin(admin.ModelAdmin):
    list_display = [
        'location',
        'country_code',
        'timestamp',
        'temperature_celsius',
        'wind_speed_ms',
        'humidity_percent',
        'weather_summary'
    ]
    list_filter = [
        'country_code',
        'location',
        ('timestamp', admin.DateFieldListFilter),
    ]
    search_fields = ['location', 'country_code']
    date_hierarchy = 'timestamp'
    ordering = ['-timestamp', 'country_code', 'location']
    readonly_fields = ['created_at', 'weather_summary']

    fieldsets = (
        ('Location & Time', {
            'fields': ('timestamp', 'location', 'country_code')
        }),
        ('Temperature', {
            'fields': ('temperature_celsius', 'temperature_min_celsius', 'temperature_max_celsius')
        }),
        ('Wind & Pressure', {
            'fields': ('wind_speed_ms', 'wind_direction_degrees', 'pressure_hpa')
        }),
        ('Humidity & Precipitation', {
            'fields': ('humidity_percent', 'precipitation_mm', 'cloud_cover_percent')
        }),
        ('Solar', {
            'fields': ('solar_irradiance_wm2',)
        }),
        ('Metadata', {
            'fields': ('created_at', 'weather_summary'),
            'classes': ('collapse',)
        })
    )

    def weather_summary(self, obj):
        summary = []
        if obj.temperature_celsius:
            summary.append(f"{obj.temperature_celsius}°C")
        if obj.wind_speed_ms:
            summary.append(f"{obj.wind_speed_ms}m/s wind")
        if obj.humidity_percent:
            summary.append(f"{obj.humidity_percent}% humidity")
        return ", ".join(summary) if summary else "-"

    weather_summary.short_description = "Weather Summary"


@admin.register(WeatherForecast)
class WeatherForecastAdmin(admin.ModelAdmin):
    list_display = [
        'location',
        'country_code',
        'target_timestamp',
        'forecast_horizon_hours',
        'temperature_celsius',
        'wind_speed_ms',
        'forecast_age'
    ]
    list_filter = [
        'country_code',
        'location',
        'forecast_horizon_hours',
        ('target_timestamp', admin.DateFieldListFilter),
    ]
    search_fields = ['location', 'country_code']
    date_hierarchy = 'target_timestamp'
    ordering = ['-target_timestamp', 'forecast_horizon_hours']

    def forecast_age(self, obj):
        from django.utils import timezone
        age = timezone.now() - obj.forecast_timestamp
        hours = age.total_seconds() / 3600
        if hours < 24:
            return f"{hours:.1f}h old"
        else:
            return f"{hours / 24:.1f}d old"

    forecast_age.short_description = "Forecast Age"
