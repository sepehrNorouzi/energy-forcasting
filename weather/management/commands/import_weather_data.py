# weather/management/commands/import_weather_data.py

import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone
from datetime import datetime
import pytz
from typing import Dict, List, Tuple
import logging

from weather.models import WeatherData


class Command(BaseCommand):
    help = 'Import Open Power System Weather Data from CSV file'

    def add_arguments(self, parser):
        parser.add_argument(
            'csv_file',
            type=str,
            help='Path to the OPSD weather CSV file (e.g., weather_data.csv)'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Number of records to process in each batch (default: 1000)'
        )
        parser.add_argument(
            '--start-date',
            type=str,
            help='Start date for import (YYYY-MM-DD). If not provided, imports all data.'
        )
        parser.add_argument(
            '--end-date',
            type=str,
            help='End date for import (YYYY-MM-DD). If not provided, imports all data.'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Test the import without saving to database'
        )
        parser.add_argument(
            '--countries',
            type=str,
            help='Comma-separated list of country codes to import (e.g., DE,FR,GB). If not provided, imports all countries.'
        )

    def handle(self, *args, **options):
        csv_file = options['csv_file']
        batch_size = options['batch_size']
        dry_run = options['dry_run']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE - No data will be saved'))

        try:
            # Read and process CSV
            self.stdout.write('Reading weather CSV file...')
            df = self.read_csv_file(csv_file, options)

            # Validate data structure
            self.validate_csv_structure(df)

            # Parse columns to understand data structure
            column_mapping = self.parse_weather_columns(df, options.get('countries'))

            # Import weather data
            import_stats = self.import_weather_data(df, column_mapping, batch_size, dry_run)

            # Log import results (if not dry run)
            if not dry_run:
                self.log_import_results(csv_file, df, import_stats)

            self.stdout.write(
                self.style.SUCCESS(f'Weather import completed: {import_stats}')
            )

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Weather import failed: {str(e)}')
            )
            raise CommandError(f'Weather import failed: {str(e)}')

    def read_csv_file(self, csv_file: str, options: dict) -> pd.DataFrame:
        """Read and filter CSV file"""
        try:
            # Read CSV with pandas - handle timezone properly
            df = pd.read_csv(csv_file)

            # Parse timestamp column manually to handle timezone issues
            df['utc_timestamp'] = pd.to_datetime(df['utc_timestamp'], utc=True)

            # Filter by date range if provided
            if options.get('start_date'):
                start_date = pd.to_datetime(options['start_date'], utc=True)
                df = df[df['utc_timestamp'] >= start_date]

            if options.get('end_date'):
                end_date = pd.to_datetime(options['end_date'], utc=True)
                df = df[df['utc_timestamp'] <= end_date]

            self.stdout.write(f'Loaded {len(df)} rows from weather CSV')
            self.stdout.write(f'Date range: {df["utc_timestamp"].min()} to {df["utc_timestamp"].max()}')
            return df

        except FileNotFoundError:
            raise CommandError(f'Weather CSV file not found: {csv_file}')
        except Exception as e:
            raise CommandError(f'Error reading weather CSV: {str(e)}')

    def validate_csv_structure(self, df: pd.DataFrame):
        """Validate CSV has expected structure"""
        required_columns = ['utc_timestamp']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise CommandError(f'Missing required columns: {missing_columns}')

        # Check if we have any temperature or radiation columns
        temp_columns = [col for col in df.columns if '_temperature' in col]
        radiation_columns = [col for col in df.columns if '_radiation_' in col]

        if not temp_columns and not radiation_columns:
            raise CommandError('No temperature or radiation columns found in CSV')

        self.stdout.write(f'Weather CSV validation passed - {len(df.columns)} columns found')
        self.stdout.write(f'Temperature columns: {len(temp_columns)}, Radiation columns: {len(radiation_columns)}')

    def parse_weather_columns(self, df: pd.DataFrame, countries_filter: str = None) -> Dict[str, Dict[str, str]]:
        """Parse column names to map weather variables by country"""

        # Parse countries filter if provided
        target_countries = None
        if countries_filter:
            target_countries = [c.strip().upper() for c in countries_filter.split(',')]
            self.stdout.write(f'Filtering for countries: {target_countries}')

        country_mapping = {}

        for column in df.columns:
            if column == 'utc_timestamp':
                continue

            # Parse column pattern: COUNTRY_variable
            parts = column.split('_', 1)
            if len(parts) < 2:
                continue

            country_code = parts[0].upper()
            variable = parts[1]

            # Filter by target countries if specified
            if target_countries and country_code not in target_countries:
                continue

            # Initialize country mapping if not exists
            if country_code not in country_mapping:
                country_mapping[country_code] = {
                    'temperature': None,
                    'radiation_direct': None,
                    'radiation_diffuse': None
                }

            # Map variables
            if variable == 'temperature':
                country_mapping[country_code]['temperature'] = column
            elif variable == 'radiation_direct_horizontal':
                country_mapping[country_code]['radiation_direct'] = column
            elif variable == 'radiation_diffuse_horizontal':
                country_mapping[country_code]['radiation_diffuse'] = column

        # Log what we found
        self.stdout.write(f'Found weather data for {len(country_mapping)} countries:')
        for country, variables in country_mapping.items():
            available_vars = [var for var, col in variables.items() if col is not None]
            self.stdout.write(f'  {country}: {", ".join(available_vars)}')

        return country_mapping

    def import_weather_data(self, df: pd.DataFrame, country_mapping: Dict[str, Dict[str, str]],
                            batch_size: int, dry_run: bool) -> Dict[str, int]:
        """Import weather data into Django models"""

        stats = {
            'weather_records': 0,
            'countries_processed': 0,
            'errors': 0
        }

        total_rows = len(df)

        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]

            self.stdout.write(f'Processing weather batch {start_idx}-{end_idx} of {total_rows}')

            if not dry_run:
                with transaction.atomic():
                    batch_stats = self.process_weather_batch(batch_df, country_mapping)
                    stats['weather_records'] += batch_stats['records']
                    stats['errors'] += batch_stats['errors']
            else:
                # Dry run - count what would be imported
                batch_stats = self.count_weather_records(batch_df, country_mapping)
                stats['weather_records'] += batch_stats['records']

        stats['countries_processed'] = len(country_mapping)
        return stats

    def process_weather_batch(self, df: pd.DataFrame, country_mapping: Dict[str, Dict[str, str]]) -> Dict[str, int]:
        """Process a batch of weather data"""
        weather_records = []
        errors = 0

        for _, row in df.iterrows():
            timestamp = row['utc_timestamp']

            for country_code, variables in country_mapping.items():
                try:
                    # Extract weather variables for this country
                    temperature = None
                    solar_irradiance = None

                    # Get temperature
                    if variables['temperature']:
                        temp_val = row.get(variables['temperature'])
                        if not pd.isna(temp_val):
                            temperature = float(temp_val)

                    # Calculate total solar irradiance (direct + diffuse)
                    direct_rad = None
                    diffuse_rad = None

                    if variables['radiation_direct']:
                        direct_val = row.get(variables['radiation_direct'])
                        if not pd.isna(direct_val):
                            direct_rad = float(direct_val)

                    if variables['radiation_diffuse']:
                        diffuse_val = row.get(variables['radiation_diffuse'])
                        if not pd.isna(diffuse_val):
                            diffuse_rad = float(diffuse_val)

                    # Calculate total horizontal irradiance
                    if direct_rad is not None and diffuse_rad is not None:
                        solar_irradiance = direct_rad + diffuse_rad
                    elif direct_rad is not None:
                        solar_irradiance = direct_rad
                    elif diffuse_rad is not None:
                        solar_irradiance = diffuse_rad

                    # Skip if no data available
                    if temperature is None and solar_irradiance is None:
                        continue

                    # Create weather record
                    weather_record = WeatherData(
                        timestamp=timestamp,
                        location=f"{country_code} Average",  # Country-level average
                        country_code=country_code,
                        temperature_celsius=temperature,
                        solar_irradiance_wm2=solar_irradiance,
                        # Store components in additional fields if your model supports them
                        # You might want to add these to your WeatherData model:
                        # radiation_direct_wm2=direct_rad,
                        # radiation_diffuse_wm2=diffuse_rad,
                    )
                    weather_records.append(weather_record)

                except Exception as e:
                    errors += 1
                    if errors <= 5:  # Log first few errors
                        self.stdout.write(f'Error processing {country_code} at {timestamp}: {e}')

        # Bulk create weather records
        if weather_records:
            try:
                WeatherData.objects.bulk_create(
                    weather_records,
                    ignore_conflicts=True,
                    batch_size=500
                )
            except Exception as e:
                self.stdout.write(f'Bulk create error: {e}')
                errors += len(weather_records)
                return {'records': 0, 'errors': errors}

        return {'records': len(weather_records), 'errors': errors}

    def count_weather_records(self, df: pd.DataFrame, country_mapping: Dict[str, Dict[str, str]]) -> Dict[str, int]:
        """Count weather records for dry run"""
        record_count = 0

        for _, row in df.iterrows():
            for country_code, variables in country_mapping.items():
                # Count if any data is available for this country/timestamp
                has_data = False

                if variables['temperature']:
                    temp_val = row.get(variables['temperature'])
                    if not pd.isna(temp_val):
                        has_data = True

                if variables['radiation_direct']:
                    rad_val = row.get(variables['radiation_direct'])
                    if not pd.isna(rad_val):
                        has_data = True

                if variables['radiation_diffuse']:
                    rad_val = row.get(variables['radiation_diffuse'])
                    if not pd.isna(rad_val):
                        has_data = True

                if has_data:
                    record_count += 1

        return {'records': record_count}

    def log_import_results(self, csv_file: str, df: pd.DataFrame, stats: Dict[str, int]):
        """Log import results"""
        # You could create a WeatherImportLog model similar to DataImportLog
        # For now, just log to console
        self.stdout.write(f'Weather import completed:')
        self.stdout.write(f'  File: {csv_file}')
        self.stdout.write(f'  Date range: {df["utc_timestamp"].min()} to {df["utc_timestamp"].max()}')
        self.stdout.write(f'  Countries processed: {stats["countries_processed"]}')
        self.stdout.write(f'  Records imported: {stats["weather_records"]}')
        self.stdout.write(f'  Errors: {stats["errors"]}')

