from django.db import models

class ForecastModel(models.Model):
    """ML model metadata and performance tracking"""
    MODEL_TYPE_CHOICES = [
        ('linear_regression', 'Linear Regression'),
        ('random_forest', 'Random Forest'),
        ('neural_network', 'Neural Network'),
        ('arima', 'ARIMA'),
        ('prophet', 'Prophet'),
        ('ensemble', 'Ensemble'),
    ]

    name = models.CharField(max_length=100, unique=True)
    model_type = models.CharField(max_length=20, choices=MODEL_TYPE_CHOICES)
    country_code = models.CharField(max_length=10)
    target_variable = models.CharField(max_length=50)  # 'load', 'solar_generation'

    # Model configuration
    parameters = models.JSONField(default=dict, blank=True)
    feature_columns = models.JSONField(default=list, blank=True)

    # Training info
    training_start_date = models.DateTimeField()
    training_end_date = models.DateTimeField()
    training_samples = models.PositiveIntegerField(null=True, blank=True)

    # Performance metrics
    mae = models.FloatField(null=True, blank=True)
    rmse = models.FloatField(null=True, blank=True)
    mape = models.FloatField(null=True, blank=True)
    r2_score = models.FloatField(null=True, blank=True)

    # Model lifecycle
    version = models.CharField(max_length=20, default="1.0")
    is_active = models.BooleanField(default=True)
    model_file_path = models.CharField(max_length=255, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.name} v{self.version} ({self.country_code})"


class EnergyForecast(models.Model):
    """Energy demand/generation predictions"""
    model = models.ForeignKey(ForecastModel, on_delete=models.CASCADE, related_name='forecasts')
    country_code = models.CharField(max_length=10, db_index=True)
    forecast_timestamp = models.DateTimeField(db_index=True)  # When prediction was made
    target_timestamp = models.DateTimeField(db_index=True)  # What time prediction is for

    # Prediction results
    predicted_value = models.FloatField()
    confidence_lower = models.FloatField(null=True, blank=True)
    confidence_upper = models.FloatField(null=True, blank=True)

    # For evaluation (filled later)
    actual_value = models.FloatField(null=True, blank=True)

    # Forecast horizon
    horizon_hours = models.PositiveIntegerField()  # 1, 6, 12, 24, 48 hours ahead

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ['model', 'target_timestamp']
        indexes = [
            models.Index(fields=['country_code', 'target_timestamp']),
            models.Index(fields=['forecast_timestamp']),
        ]

    @property
    def forecast_error(self):
        if self.actual_value is not None:
            return abs(self.predicted_value - self.actual_value)
        return None

    @property
    def forecast_accuracy_percent(self):
        if self.actual_value is not None and self.actual_value != 0:
            error_percent = abs(self.predicted_value - self.actual_value) / self.actual_value * 100
            return max(0, 100 - error_percent)
        return None


class ModelPerformanceMetric(models.Model):
    """Track model performance over time"""
    model = models.ForeignKey(ForecastModel, on_delete=models.CASCADE, related_name='performance_metrics')
    evaluation_date = models.DateField()
    evaluation_period_days = models.PositiveIntegerField(default=30)

    # Performance metrics for the period
    mae = models.FloatField()
    rmse = models.FloatField()
    mape = models.FloatField()
    forecast_count = models.PositiveIntegerField()

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ['model', 'evaluation_date']
        ordering = ['-evaluation_date']
