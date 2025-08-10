# energy_data/management/commands/import_opsd_data.py

import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone
from datetime import datetime
import pytz
from typing import Dict, List, Tuple
import logging

from energy_data.models import LoadData, RenewableGeneration, EnergyPrice, DataImportLog


class Command(BaseCommand):
    help = 'Import Open Power System Data from CSV file'

    def add_arguments(self, parser):
        parser.add_argument(
            'csv_file',
            type=str,
            help='Path to the OPSD CSV file (e.g., time_series_60min_singleindex.csv)'
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

    def handle(self, *args, **options):
        csv_file = options['csv_file']
        batch_size = options['batch_size']
        dry_run = options['dry_run']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE - No data will be saved'))

        try:
            # Read and process CSV
            self.stdout.write('Reading CSV file...')
            df = self.read_csv_file(csv_file, options)

            # Validate data structure
            self.validate_csv_structure(df)

            # Parse columns to understand data types
            column_mapping = self.parse_column_structure(df)

            # Import data
            import_stats = self.import_data(df, column_mapping, batch_size, dry_run)

            # Log import results
            if not dry_run:
                self.log_import_results(csv_file, df, import_stats)

            self.stdout.write(
                self.style.SUCCESS(f'Import completed successfully: {import_stats}')
            )

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Import failed: {str(e)}')
            )
            raise CommandError(f'Import failed: {str(e)}')

    def read_csv_file(self, csv_file: str, options: dict) -> pd.DataFrame:
        """Read and filter CSV file"""
        try:
            # Read CSV with pandas
            df = pd.read_csv(csv_file, parse_dates=['utc_timestamp', 'cet_cest_timestamp'])

            # Filter by date range if provided
            if options.get('start_date'):
                start_date = pd.to_datetime(options['start_date'])
                df = df[df['utc_timestamp'] >= start_date]

            if options.get('end_date'):
                end_date = pd.to_datetime(options['end_date'])
                df = df[df['utc_timestamp'] <= end_date]

            self.stdout.write(f'Loaded {len(df)} rows from CSV')
            return df

        except FileNotFoundError:
            raise CommandError(f'CSV file not found: {csv_file}')
        except Exception as e:
            raise CommandError(f'Error reading CSV: {str(e)}')

    def validate_csv_structure(self, df: pd.DataFrame):
        """Validate CSV has expected structure"""
        required_columns = ['utc_timestamp', 'cet_cest_timestamp']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise CommandError(f'Missing required columns: {missing_columns}')

        self.stdout.write(f'CSV validation passed - {len(df.columns)} columns found')

    def parse_column_structure(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Parse column names to categorize data types"""
        column_mapping = {
            'load_data': [],
            'renewable_generation': [],
            'energy_prices': [],
            'capacity_data': [],
            'ignored': []
        }

        for column in df.columns:
            if column in ['utc_timestamp', 'cet_cest_timestamp']:
                continue

            # Parse column pattern: COUNTRY_TYPE_DETAILS
            parts = column.split('_')
            if len(parts) < 2:
                column_mapping['ignored'].append(column)
                continue

            country_code = parts[0]
            data_type = '_'.join(parts[1:])

            # Categorize by data type
            if 'load_actual' in data_type or 'load_forecast' in data_type:
                column_mapping['load_data'].append(column)
            elif 'generation_actual' in data_type:
                column_mapping['renewable_generation'].append(column)
            elif 'capacity' in data_type:
                column_mapping['capacity_data'].append(column)
            elif 'price_day_ahead' in data_type:
                column_mapping['energy_prices'].append(column)
            else:
                column_mapping['ignored'].append(column)

        # Log what we found
        for category, columns in column_mapping.items():
            if columns:
                self.stdout.write(f'{category}: {len(columns)} columns')

        return column_mapping

    def import_data(self, df: pd.DataFrame, column_mapping: Dict[str, List[str]],
                    batch_size: int, dry_run: bool) -> Dict[str, int]:
        """Import data into Django models"""

        stats = {
            'load_records': 0,
            'generation_records': 0,
            'price_records': 0,
            'errors': 0
        }

        total_rows = len(df)

        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]

            self.stdout.write(f'Processing batch {start_idx}-{end_idx} of {total_rows}')

            if not dry_run:
                with transaction.atomic():
                    # Import load data
                    load_count = self.import_load_data(batch_df, column_mapping['load_data'])
                    stats['load_records'] += load_count

                    # Import renewable generation data
                    gen_count = self.import_generation_data(
                        batch_df,
                        column_mapping['renewable_generation'],
                        column_mapping['capacity_data']
                    )
                    stats['generation_records'] += gen_count

                    # Import price data
                    price_count = self.import_price_data(batch_df, column_mapping['energy_prices'])
                    stats['price_records'] += price_count
            else:
                # Dry run - just count what would be imported
                stats['load_records'] += self.count_load_records(batch_df, column_mapping['load_data'])
                stats['generation_records'] += self.count_generation_records(batch_df,
                                                                             column_mapping['renewable_generation'])
                stats['price_records'] += self.count_price_records(batch_df, column_mapping['energy_prices'])

        return stats

    def import_load_data(self, df: pd.DataFrame, load_columns: List[str]) -> int:
        """Import load data (actual and forecast)"""
        load_records = []

        # Group load columns by country
        country_loads = {}
        for col in load_columns:
            country_code = col.split('_')[0]
            if country_code not in country_loads:
                country_loads[country_code] = {}

            if 'load_actual' in col:
                country_loads[country_code]['actual'] = col
            elif 'load_forecast' in col:
                country_loads[country_code]['forecast'] = col

        # Create load records
        for _, row in df.iterrows():
            for country_code, columns in country_loads.items():
                actual_col = columns.get('actual')
                forecast_col = columns.get('forecast')

                # Skip if both actual and forecast are null
                actual_val = row.get(actual_col) if actual_col else None
                forecast_val = row.get(forecast_col) if forecast_col else None

                if pd.isna(actual_val) and pd.isna(forecast_val):
                    continue

                load_record = LoadData(
                    utc_timestamp=row['utc_timestamp'],
                    cet_cest_timestamp=row['cet_cest_timestamp'],
                    country_code=country_code,
                    actual_load_mw=actual_val if not pd.isna(actual_val) else None,
                    forecast_load_mw=forecast_val if not pd.isna(forecast_val) else None
                )
                load_records.append(load_record)

        # Bulk create
        if load_records:
            LoadData.objects.bulk_create(
                load_records,
                ignore_conflicts=True,
                batch_size=500
            )

        return len(load_records)

    def import_generation_data(self, df: pd.DataFrame, generation_columns: List[str],
                               capacity_columns: List[str]) -> int:
        """Import renewable generation data"""
        generation_records = []

        # Map generation types
        generation_type_map = {
            'solar_generation_actual': 'solar',
            'wind_onshore_generation_actual': 'wind_onshore',
            'wind_offshore_generation_actual': 'wind_offshore',
            'wind_generation_actual': 'wind_total'
        }

        # Group by country and generation type
        for _, row in df.iterrows():
            for col in generation_columns:
                parts = col.split('_')
                country_code = parts[0]

                # Determine generation type
                generation_type = None
                for key, value in generation_type_map.items():
                    if key in col:
                        generation_type = value
                        break

                if not generation_type:
                    continue

                generation_val = row.get(col)
                if pd.isna(generation_val):
                    continue

                # Look for corresponding capacity data
                capacity_val = None
                capacity_col = col.replace('generation_actual', 'capacity')
                if capacity_col in capacity_columns:
                    capacity_val = row.get(capacity_col)
                    if pd.isna(capacity_val):
                        capacity_val = None

                # Calculate capacity factor if both generation and capacity available
                capacity_factor = None
                if capacity_val and capacity_val > 0:
                    capacity_factor = generation_val / capacity_val

                generation_record = RenewableGeneration(
                    utc_timestamp=row['utc_timestamp'],
                    cet_cest_timestamp=row['cet_cest_timestamp'],
                    country_code=country_code,
                    generation_type=generation_type,
                    actual_generation_mw=generation_val,
                    capacity_mw=capacity_val,
                    capacity_factor=capacity_factor
                )
                generation_records.append(generation_record)

        # Bulk create
        if generation_records:
            RenewableGeneration.objects.bulk_create(
                generation_records,
                ignore_conflicts=True,
                batch_size=500
            )

        return len(generation_records)

    def import_price_data(self, df: pd.DataFrame, price_columns: List[str]) -> int:
        """Import energy price data"""
        price_records = []

        for _, row in df.iterrows():
            for col in price_columns:
                country_code = col.split('_')[0]
                price_val = row.get(col)

                if pd.isna(price_val):
                    continue

                # Determine currency (EUR for most, GBP for GB regions)
                currency = 'GBP' if country_code.startswith('GB') else 'EUR'

                price_record = EnergyPrice(
                    utc_timestamp=row['utc_timestamp'],
                    cet_cest_timestamp=row['cet_cest_timestamp'],
                    country_code=country_code,
                    day_ahead_price=price_val,
                    currency=currency
                )
                price_records.append(price_record)

        # Bulk create
        if price_records:
            EnergyPrice.objects.bulk_create(
                price_records,
                ignore_conflicts=True,
                batch_size=500
            )

        return len(price_records)

    def count_load_records(self, df: pd.DataFrame, load_columns: List[str]) -> int:
        """Count load records for dry run"""
        count = 0
        for col in load_columns:
            count += df[col].notna().sum()
        return count

    def count_generation_records(self, df: pd.DataFrame, generation_columns: List[str]) -> int:
        """Count generation records for dry run"""
        count = 0
        for col in generation_columns:
            count += df[col].notna().sum()
        return count

    def count_price_records(self, df: pd.DataFrame, price_columns: List[str]) -> int:
        """Count price records for dry run"""
        count = 0
        for col in price_columns:
            count += df[col].notna().sum()
        return count

    def log_import_results(self, csv_file: str, df: pd.DataFrame, stats: Dict[str, int]):
        """Log import results to DataImportLog"""
        DataImportLog.objects.create(
            source='opsd',
            data_start_date=df['utc_timestamp'].min(),
            data_end_date=df['utc_timestamp'].max(),
            records_imported=sum([
                stats['load_records'],
                stats['generation_records'],
                stats['price_records']
            ]),
            records_failed=stats['errors'],
            file_name=csv_file,
            success=stats['errors'] == 0
        )