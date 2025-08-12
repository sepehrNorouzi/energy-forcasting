# analytics/management/commands/generate_data_profile.py

import pandas as pd
from django.contrib.auth.models import User
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.utils import timezone
from ydata_profiling import ProfileReport
import boto3
from botocore.exceptions import ClientError
import os
import tempfile
from datetime import datetime, timedelta
from typing import Optional, List

from energy_data.models import LoadData, RenewableGeneration, EnergyPrice
from weather.models import WeatherData
from analytics.models import DataProfilingReport


class Command(BaseCommand):
    help = 'Generate comprehensive data profiling reports using ydata-profiling'

    def add_arguments(self, parser):
        parser.add_argument(
            '--countries',
            type=str,
            help='Comma-separated list of country codes (e.g., DE,FR,GB). Default: all countries'
        )
        parser.add_argument(
            '--start-date',
            type=str,
            help='Start date for analysis (YYYY-MM-DD). Default: last 30 days'
        )
        parser.add_argument(
            '--end-date',
            type=str,
            help='End date for analysis (YYYY-MM-DD). Default: latest data'
        )
        parser.add_argument(
            '--report-type',
            type=str,
            choices=['full', 'minimal', 'explorative'],
            default='minimal',
            help='Type of profiling report to generate'
        )
        parser.add_argument(
            '--sample-size',
            type=int,
            default=50000,
            help='Maximum number of records to sample for profiling (default: 50000)'
        )

    def handle(self, *args, **options):
        self.stdout.write('Starting data profiling report generation...')

        try:
            # Parse options
            countries = self.parse_countries(options.get('countries'))
            start_date, end_date = self.parse_date_range(options)

            self.stdout.write(f'Generating report for:')
            self.stdout.write(f'  Countries: {countries or "all"}')
            self.stdout.write(f'  Date range: {start_date} to {end_date}')
            self.stdout.write(f'  Sample size: {options["sample_size"]:,}')

            # Extract and combine data
            combined_df = self.extract_combined_data(
                countries, start_date, end_date, options['sample_size']
            )

            if combined_df.empty:
                raise CommandError('No data found for the specified criteria')

            self.stdout.write(f'Extracted {len(combined_df):,} records for profiling')

            # Generate profiling report
            report_path = self.generate_profile_report(
                combined_df, options['report_type']
            )

            # Upload to S3
            s3_url = self.upload_to_s3(report_path)

            # Save report metadata to database
            self.save_report_metadata(
                s3_url, countries, start_date, end_date, len(combined_df)
            )

            self.stdout.write(
                self.style.SUCCESS(f'Profiling report generated successfully: {s3_url}')
            )

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Profiling failed: {str(e)}')
            )
            raise CommandError(f'Profiling failed: {str(e)}')

    def parse_countries(self, countries_str: Optional[str]) -> Optional[List[str]]:
        """Parse countries parameter"""
        if countries_str:
            return [c.strip().upper() for c in countries_str.split(',')]
        return None

    def parse_date_range(self, options: dict) -> tuple:
        """Parse and validate date range"""
        end_date = options.get('end_date')
        if end_date:
            end_date = pd.to_datetime(end_date, utc=True)
        else:
            # Default to latest data
            latest_load = LoadData.objects.order_by('-utc_timestamp').first()
            end_date = latest_load.utc_timestamp if latest_load else timezone.now()

        start_date = options.get('start_date')
        if start_date:
            start_date = pd.to_datetime(start_date, utc=True)
        else:
            # Default to 30 days before end_date
            start_date = end_date - timedelta(days=30)

        return start_date, end_date

    def extract_combined_data(self, countries: Optional[List[str]],
                              start_date: datetime, end_date: datetime,
                              sample_size: int) -> pd.DataFrame:
        """Extract and combine energy and weather data"""

        self.stdout.write('Extracting load data...')

        # Base query filters
        load_filters = {
            'utc_timestamp__gte': start_date,
            'utc_timestamp__lte': end_date
        }
        if countries:
            load_filters['country_code__in'] = countries

        # Extract load data
        load_queryset = LoadData.objects.filter(**load_filters).order_by('?')[:sample_size]
        load_data = []

        for load in load_queryset:
            load_data.append({
                'timestamp': load.utc_timestamp,
                'country_code': load.country_code,
                'actual_load_mw': load.actual_load_mw,
                'forecast_load_mw': load.forecast_load_mw,
            })

        if not load_data:
            return pd.DataFrame()

        load_df = pd.DataFrame(load_data)
        self.stdout.write(f'Extracted {len(load_df)} load records')

        # Extract renewable generation data
        self.stdout.write('Extracting renewable generation data...')
        gen_filters = load_filters.copy()
        gen_queryset = RenewableGeneration.objects.filter(**gen_filters)

        gen_data = []
        for gen in gen_queryset:
            gen_data.append({
                'timestamp': gen.utc_timestamp,
                'country_code': gen.country_code,
                'generation_type': gen.generation_type,
                'actual_generation_mw': gen.actual_generation_mw,
                'capacity_mw': gen.capacity_mw,
                'capacity_factor': gen.capacity_factor,
            })

        # Pivot generation data
        if gen_data:
            gen_df = pd.DataFrame(gen_data)
            gen_pivot = gen_df.pivot_table(
                index=['timestamp', 'country_code'],
                columns='generation_type',
                values=['actual_generation_mw', 'capacity_mw', 'capacity_factor'],
                aggfunc='first'
            )
            gen_pivot.columns = [f'{col[1]}_{col[0]}' for col in gen_pivot.columns]
            gen_pivot = gen_pivot.reset_index()
        else:
            gen_pivot = pd.DataFrame(columns=['timestamp', 'country_code'])

        # Extract weather data
        self.stdout.write('Extracting weather data...')
        weather_filters = {
            'timestamp__gte': start_date,
            'timestamp__lte': end_date
        }
        if countries:
            weather_filters['country_code__in'] = countries

        weather_queryset = WeatherData.objects.filter(**weather_filters)
        weather_data = []

        for weather in weather_queryset:
            weather_data.append({
                'timestamp': weather.timestamp,
                'country_code': weather.country_code,
                'temperature_celsius': weather.temperature_celsius,
                'solar_irradiance_wm2': weather.solar_irradiance_wm2,
                'humidity_percent': weather.humidity_percent,
                'wind_speed_ms': weather.wind_speed_ms,
                'pressure_hpa': weather.pressure_hpa,
            })

        weather_df = pd.DataFrame(weather_data) if weather_data else pd.DataFrame(
            columns=['timestamp', 'country_code']
        )

        # Extract price data
        self.stdout.write('Extracting price data...')
        price_queryset = EnergyPrice.objects.filter(**load_filters)
        price_data = []

        for price in price_queryset:
            price_data.append({
                'timestamp': price.utc_timestamp,
                'country_code': price.country_code,
                'day_ahead_price': price.day_ahead_price,
                'currency': price.currency,
            })

        price_df = pd.DataFrame(price_data) if price_data else pd.DataFrame(
            columns=['timestamp', 'country_code']
        )

        # Combine all datasets
        self.stdout.write('Combining datasets...')
        combined_df = load_df.copy()

        # Merge generation data
        if not gen_pivot.empty:
            combined_df = combined_df.merge(
                gen_pivot, on=['timestamp', 'country_code'], how='left'
            )

        # Merge weather data
        if not weather_df.empty:
            combined_df = combined_df.merge(
                weather_df, on=['timestamp', 'country_code'], how='left'
            )

        # Merge price data
        if not price_df.empty:
            combined_df = combined_df.merge(
                price_df, on=['timestamp', 'country_code'], how='left'
            )

        # Add derived features
        combined_df = self.add_derived_features(combined_df)

        return combined_df

    def add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived features for better analysis"""
        if df.empty:
            return df

        # Time-based features
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['month'] = df['timestamp'].dt.month
        df['is_weekend'] = df['day_of_week'].isin([5, 6])

        # Load features
        if 'actual_load_mw' in df.columns and 'forecast_load_mw' in df.columns:
            df['forecast_error_mw'] = df['actual_load_mw'] - df['forecast_load_mw']
            df['forecast_error_pct'] = (
                    df['forecast_error_mw'] / df['actual_load_mw'] * 100
            ).fillna(0)

        # Renewable features
        renewable_cols = [col for col in df.columns if 'actual_generation_mw' in col]
        if renewable_cols:
            df['total_renewable_mw'] = df[renewable_cols].sum(axis=1)
            if 'actual_load_mw' in df.columns:
                df['renewable_penetration_pct'] = (
                        df['total_renewable_mw'] / df['actual_load_mw'] * 100
                ).fillna(0)

        return df

    def generate_profile_report(self, df: pd.DataFrame, report_type: str) -> str:
        """Generate ydata-profiling report"""
        self.stdout.write('Generating profiling report...')

        # Configure profiling based on report type
        config_map = {
            'minimal': {
                'title': 'Energy Data Profile - Minimal',
                'explorative': False,
                'interactions': {'targets': []},
                'correlations': {'auto': {'calculate': True, 'warn_high_cardinality': False}},
                'missing_diagrams': {'matrix': False, 'bar': True, 'heatmap': False},
                'duplicates': {'head': 0},
                'samples': {'head': 5, 'tail': 5}
            },
            'full': {
                'title': 'Energy Data Profile - Complete Analysis',
                'explorative': True,
            },
            'explorative': {
                'title': 'Energy Data Profile - Explorative',
                'explorative': True,
                'interactions': {'targets': ['actual_load_mw', 'temperature_celsius']},
            }
        }

        config = config_map.get(report_type, config_map['minimal'])

        # Generate report
        profile = ProfileReport(
            df,
            **config,
            progress_bar=False
        )

        # Save to temporary file
        timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
        temp_file = f'/tmp/energy_profile_{timestamp}.html'
        profile.to_file(temp_file)

        return temp_file

    def upload_to_s3(self, file_path: str) -> str:
        """Upload report to S3 and return URL"""
        self.stdout.write('Uploading report to S3...')

        # Validate AWS settings
        required_settings = {
            'AWS_ACCESS_KEY_ID': getattr(settings, 'AWS_ACCESS_KEY_ID', None),
            'AWS_SECRET_ACCESS_KEY': getattr(settings, 'AWS_SECRET_ACCESS_KEY', None),
            'AWS_STORAGE_BUCKET_NAME': getattr(settings, 'AWS_STORAGE_BUCKET_NAME', None),
        }

        missing_settings = [key for key, value in required_settings.items() if not value]
        if missing_settings:
            # Save report locally instead
            local_path = self.save_report_locally(file_path)
            self.stdout.write(
                self.style.WARNING(
                    f'Missing AWS settings: {", ".join(missing_settings)}. '
                    f'Report saved locally at: {local_path}'
                )
            )
            return local_path

        try:
            # Initialize S3 client
            s3_client = boto3.client(
                's3',
                endpoint_url=settings.AWS_S3_ENDPOINT_URL,
                aws_access_key_id=required_settings['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=required_settings['AWS_SECRET_ACCESS_KEY'],
                region_name=getattr(settings, 'AWS_S3_REGION_NAME', 'us-east-1')
            )

            # Generate S3 key
            timestamp = timezone.now().strftime('%Y/%m/%d')
            filename = os.path.basename(file_path)
            s3_key = f'analytics/data-profiles/{timestamp}/{filename}'
            endpoint_url = settings.AWS_S3_ENDPOINT_URL

            # Upload file
            s3_client.upload_file(
                file_path,
                required_settings['AWS_STORAGE_BUCKET_NAME'],
                s3_key,
                ExtraArgs={'ContentType': 'text/html'}
            )

            expiration_seconds = getattr(settings, 'AWS_S3_URL_EXPIRATION_SECONDS', 604800)

            try:
                presigned_url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': required_settings['AWS_STORAGE_BUCKET_NAME'],
                        'Key': s3_key
                    },
                    ExpiresIn=expiration_seconds
                )

                self.stdout.write(f'Generated pre-signed URL (expires in {expiration_seconds / 86400:.1f} days)')

            except Exception as presign_error:
                self.stdout.write(
                    self.style.WARNING(f'Failed to generate pre-signed URL: {presign_error}')
                )
                # Fallback to direct URL (might not work if bucket is private)
                bucket_name = required_settings['AWS_STORAGE_BUCKET_NAME']
                if endpoint_url:
                    endpoint_clean = endpoint_url.replace('https://', '').replace('http://', '')
                    protocol = 'https' if use_ssl else 'http'
                    presigned_url = f'{protocol}://{bucket_name}.{endpoint_clean}/{s3_key}'
                else:
                    presigned_url = f'https://{bucket_name}.s3.amazonaws.com/{s3_key}'

            # Clean up temp file
            os.remove(file_path)
            self.stdout.write(f'Report uploaded successfully. URL: {presigned_url[:100]}...')
            return presigned_url

        except ClientError as e:
            # Fallback to local storage
            local_path = self.save_report_locally(file_path)
            self.stdout.write(
                self.style.WARNING(
                    f'S3 upload failed: {str(e)}. Report saved locally at: {local_path}'
                )
            )
            return local_path
        except Exception as e:
            # Fallback to local storage
            local_path = self.save_report_locally(file_path)
            self.stdout.write(
                self.style.WARNING(
                    f'Upload error: {str(e)}. Report saved locally at: {local_path}'
                )
            )
            return local_path

    def save_report_locally(self, temp_file_path: str) -> str:
        """Save report to local media directory as fallback"""
        try:
            reports_dir = os.path.join(settings.MEDIA_ROOT, 'analytics', 'data-profiles')
            os.makedirs(reports_dir, exist_ok=True)

            timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
            filename = f'energy_profile_{timestamp}.html'
            local_path = os.path.join(reports_dir, filename)

            import shutil
            shutil.move(temp_file_path, local_path)

            # Return relative URL
            relative_path = os.path.join('analytics', 'data-profiles', filename)
            return f'{settings.MEDIA_URL}{relative_path}' if hasattr(settings, 'MEDIA_URL') else local_path

        except Exception as e:
            # Last resort - return temp file path
            self.stdout.write(self.style.ERROR(f'Failed to save locally: {str(e)}'))
            return temp_file_path

    def save_report_metadata(self, s3_url: str, countries: Optional[List[str]],
                             start_date: datetime, end_date: datetime,
                             record_count: int):
        """Save report metadata to database"""
        DataProfilingReport.objects.create(
            report_url=s3_url,
            countries=countries or [],
            start_date=start_date,
            end_date=end_date,
            record_count=record_count,
            generated_at=timezone.now(),
            generated_by=User.objects.filter(is_superuser=True).first(),
        )
