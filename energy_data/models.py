from django.db import models

# Create your models here.
from django.db import models
from django.core.validators import MinValueValidator


class BaseEnergyData(models.Model):
    """Abstract base model for all energy data"""
    utc_timestamp = models.DateTimeField(db_index=True)
    cet_cest_timestamp = models.DateTimeField(db_index=True)
    country_code = models.CharField(max_length=10, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True
        indexes = [
            models.Index(fields=['country_code', 'utc_timestamp']),
        ]


class LoadData(BaseEnergyData):
    """Electricity load data (actual and forecast)"""
    actual_load_mw = models.FloatField(
        null=True, blank=True,
        validators=[MinValueValidator(0)],
        help_text="Total actual load in MW"
    )
    forecast_load_mw = models.FloatField(
        null=True, blank=True,
        validators=[MinValueValidator(0)],
        help_text="Day-ahead load forecast in MW"
    )

    class Meta:
        unique_together = ['country_code', 'utc_timestamp']
        verbose_name = "Load Data"
        verbose_name_plural = "Load Data"

    def __str__(self):
        return f"{self.country_code} - {self.utc_timestamp} - {self.actual_load_mw}MW"


class RenewableGeneration(BaseEnergyData):
    """Renewable energy generation data"""
    GENERATION_TYPE_CHOICES = [
        ('solar', 'Solar'),
        ('wind_onshore', 'Wind Onshore'),
        ('wind_offshore', 'Wind Offshore'),
        ('wind_total', 'Wind Total'),
    ]

    generation_type = models.CharField(max_length=20, choices=GENERATION_TYPE_CHOICES)
    actual_generation_mw = models.FloatField(
        null=True, blank=True,
        validators=[MinValueValidator(0)]
    )
    capacity_mw = models.FloatField(
        null=True, blank=True,
        validators=[MinValueValidator(0)]
    )
    capacity_factor = models.FloatField(
        null=True, blank=True,
        validators=[MinValueValidator(0)]
    )

    class Meta:
        unique_together = ['country_code', 'utc_timestamp', 'generation_type']


class EnergyPrice(BaseEnergyData):
    """Energy pricing data"""
    day_ahead_price = models.FloatField(null=True, blank=True)
    currency = models.CharField(max_length=3, default='EUR')
    bidding_zone = models.CharField(max_length=20, null=True, blank=True)

    class Meta:
        unique_together = ['country_code', 'utc_timestamp']


class DataImportLog(models.Model):
    """Log of data imports"""
    SOURCE_CHOICES = [
        ('opsd', 'Open Power System Data'),
        ('entsoe', 'ENTSO-E Transparency'),
        ('manual', 'Manual Upload'),
    ]

    source = models.CharField(max_length=20, choices=SOURCE_CHOICES)
    import_timestamp = models.DateTimeField(auto_now_add=True)
    data_start_date = models.DateTimeField()
    data_end_date = models.DateTimeField()
    records_imported = models.PositiveIntegerField(default=0)
    file_name = models.CharField(max_length=255, null=True, blank=True)
    success = models.BooleanField(default=True)
    error_log = models.TextField(blank=True)
