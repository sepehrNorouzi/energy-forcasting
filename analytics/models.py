# analytics/models.py

from django.db import models
from django.utils import timezone
from django.contrib.postgres.fields import ArrayField


class DataProfilingReport(models.Model):
    """Store metadata for generated data profiling reports"""

    REPORT_STATUS_CHOICES = [
        ('generating', 'Generating'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]

    # Report identification
    report_url = models.URLField(
        max_length=500,
        help_text="S3 URL of the generated HTML report"
    )

    # Report parameters
    countries = ArrayField(
        models.CharField(max_length=10),
        default=list,
        blank=True,
        help_text="List of country codes included in the report"
    )
    start_date = models.DateTimeField(
        help_text="Start date of data analysis period"
    )
    end_date = models.DateTimeField(
        help_text="End date of data analysis period"
    )

    # Report statistics
    record_count = models.PositiveIntegerField(
        help_text="Number of records analyzed in the report"
    )
    report_type = models.CharField(
        max_length=20,
        default='minimal',
        help_text="Type of profiling report (minimal, full, explorative)"
    )

    # Metadata
    generated_at = models.DateTimeField(default=timezone.now)
    generated_by = models.ForeignKey(
        'auth.User',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        help_text="User who generated the report"
    )

    status = models.CharField(
        max_length=20,
        choices=REPORT_STATUS_CHOICES,
        default='completed'
    )

    # File info
    file_size_mb = models.FloatField(
        null=True,
        blank=True,
        help_text="Report file size in MB"
    )

    class Meta:
        ordering = ['-generated_at']
        verbose_name = "Data Profiling Report"
        verbose_name_plural = "Data Profiling Reports"

    def __str__(self):
        countries_str = ', '.join(self.countries) if self.countries else 'All Countries'
        return f"Profile Report - {countries_str} ({self.generated_at.strftime('%Y-%m-%d %H:%M')})"

    @property
    def analysis_period_days(self):
        """Calculate the umber of days covered in the analysis"""
        if self.end_date and self.start_date:
            return (self.end_date - self.start_date).days
        return None

    @property
    def countries_display(self):
        """Human-readable country list"""
        if not self.countries:
            return "All Countries"
        return ', '.join(self.countries)


class DataQualityMetric(models.Model):
    """Store specific data quality metrics from profiling reports"""

    report = models.ForeignKey(
        DataProfilingReport,
        on_delete=models.CASCADE,
        related_name='quality_metrics'
    )

    # Metric identification
    metric_name = models.CharField(
        max_length=100,
        help_text="Name of the data quality metric"
    )
    metric_category = models.CharField(
        max_length=50,
        help_text="Category of metric (completeness, consistency, validity, etc.)"
    )

    # Metric values
    metric_value = models.FloatField(
        help_text="Numeric value of the metric"
    )
    metric_unit = models.CharField(
        max_length=20,
        blank=True,
        help_text="Unit of measurement (%, count, ratio, etc.)"
    )

    # Context
    table_name = models.CharField(
        max_length=100,
        blank=True,
        help_text="Database table or dataset name"
    )
    column_name = models.CharField(
        max_length=100,
        blank=True,
        help_text="Specific column name if applicable"
    )

    # Threshold and status
    threshold_value = models.FloatField(
        null=True,
        blank=True,
        help_text="Acceptable threshold for this metric"
    )
    is_within_threshold = models.BooleanField(
        default=True,
        help_text="Whether the metric value is within acceptable range"
    )

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ['report', 'metric_name', 'table_name', 'column_name']
        ordering = ['metric_category', 'metric_name']

    def __str__(self):
        return f"{self.metric_name}: {self.metric_value}{self.metric_unit}"


class ReportGenerationLog(models.Model):
    requested_by = models.ForeignKey(
        'auth.User',
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )
    requested_at = models.DateTimeField(auto_now_add=True)

    countries_requested = ArrayField(
        models.CharField(max_length=10),
        default=list,
        blank=True
    )
    start_date_requested = models.DateTimeField()
    end_date_requested = models.DateTimeField()
    report_type_requested = models.CharField(max_length=20)

    # Generation results
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    SUCCESS = 'success'
    FAILED = 'failed'
    IN_PROGRESS = 'in_progress'

    STATUS_CHOICES = [
        (SUCCESS, 'Success'),
        (FAILED, 'Failed'),
        (IN_PROGRESS, 'In Progress'),
    ]

    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default=IN_PROGRESS
    )

    # Results
    report = models.ForeignKey(
        DataProfilingReport,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='generation_logs'
    )

    error_message = models.TextField(
        blank=True,
        help_text="Error message if generation failed"
    )

    # Performance metrics
    data_extraction_seconds = models.FloatField(null=True, blank=True)
    report_generation_seconds = models.FloatField(null=True, blank=True)
    upload_seconds = models.FloatField(null=True, blank=True)
    total_seconds = models.FloatField(null=True, blank=True)

    class Meta:
        ordering = ['-requested_at']

    def __str__(self):
        return f"Report Generation - {self.status} ({self.requested_at.strftime('%Y-%m-%d %H:%M')})"

    @property
    def duration_display(self):
        """Human-readable duration"""
        if self.total_seconds:
            if self.total_seconds < 60:
                return f"{self.total_seconds:.1f} seconds"
            else:
                minutes = self.total_seconds / 60
                return f"{minutes:.1f} minutes"
        return "Unknown"
