from django.db import models

# Create your models here.


class WeatherData(models.Model):
    """Weather data for forecasting"""
    timestamp = models.DateTimeField(db_index=True)
    location = models.CharField(max_length=100)
    country_code = models.CharField(max_length=10, db_index=True)

    # Core weather variables
    temperature_celsius = models.FloatField(null=True, blank=True)
    temperature_min_celsius = models.FloatField(null=True, blank=True)
    temperature_max_celsius = models.FloatField(null=True, blank=True)
    humidity_percent = models.FloatField(null=True, blank=True)
    wind_speed_ms = models.FloatField(null=True, blank=True)
    wind_direction_degrees = models.FloatField(null=True, blank=True)
    cloud_cover_percent = models.FloatField(null=True, blank=True)
    precipitation_mm = models.FloatField(null=True, blank=True)
    solar_irradiance_wm2 = models.FloatField(null=True, blank=True)
    pressure_hpa = models.FloatField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ['country_code', 'location', 'timestamp']
        indexes = [
            models.Index(fields=['country_code', 'timestamp']),
        ]


class WeatherForecast(models.Model):
    """Weather forecast data"""
    forecast_timestamp = models.DateTimeField()  # When forecast was made
    target_timestamp = models.DateTimeField()  # What time forecast is for
    location = models.CharField(max_length=100)
    country_code = models.CharField(max_length=10)

    # Same fields as WeatherData
    temperature_celsius = models.FloatField(null=True, blank=True)
    wind_speed_ms = models.FloatField(null=True, blank=True)
    cloud_cover_percent = models.FloatField(null=True, blank=True)
    solar_irradiance_wm2 = models.FloatField(null=True, blank=True)

    forecast_horizon_hours = models.PositiveIntegerField()  # 1, 6, 12, 24, 48 etc.

    class Meta:
        unique_together = ['country_code', 'location', 'target_timestamp', 'forecast_horizon_hours']

