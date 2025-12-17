"""
Enhanced Test Data Generator for ShopZada Data Warehouse - Edge Cases & Schema Drift
Generates datasets with intentional quality issues and schema variations for comprehensive testing

Usage:
    python generate_test_data_edge_cases.py --scenario nulls --output data/test_cases/TC-Q-01/
    python generate_test_data_edge_cases.py --scenario invalid_types --output data/test_cases/TC-Q-02/
    python generate_test_data_edge_cases.py --scenario duplicates --output data/test_cases/TC-Q-03/
    python generate_test_data_edge_cases.py --scenario boundary_values --output data/test_cases/TC-Q-04/
    python generate_test_data_edge_cases.py --scenario schema_drift --output data/test_cases/TC-Q-05/
    python generate_test_data_edge_cases.py --scenario all --output data/test_cases/comprehensive/
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import argparse
from pathlib import Path
import json


class EdgeCaseTestDataGenerator:
    """Generate test data with intentional quality issues and edge cases"""
    
    def __init__(self, output_dir='data/test_cases/edge_cases/'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report = []
        
    def log_issue(self, category, description, count, severity="MEDIUM"):
        """Log information about intentional data quality issues"""
        self.report.append({
            'category': category,
            'description': description,
            'count': count,
            'severity': severity
        })
        print(f"  [{severity}] {description}: {count} cases")
        
    # ==================== SCENARIO 1: NULL/BLANK VALUES ====================
    
    def generate_null_values_scenario(self):
        """Generate data with various null/blank scenarios"""
        print("\n📊 SCENARIO 1: NULL/BLANK Values Testing")
        print("=" * 60)
        
        # Customer data with nulls in various critical fields
        customers = []
        num_customers = 100
        
        for i in range(num_customers):
            customer = {
                'user_id': f'USER{i:06d}',
                'name': f'Customer {i}',
                'email': f'user{i}@example.com',
                'phone': f'+1-555-{random.randint(1000, 9999)}',
                'age': random.randint(18, 80),
                'gender': random.choice(['Male', 'Female', 'Other']),
                'city': random.choice(['New York', 'Los Angeles', 'Chicago']),
                'state': random.choice(['NY', 'CA', 'IL']),
                'country': 'USA',
                'registration_date': (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))).strftime('%Y-%m-%d')
            }
            
            # Inject NULL values strategically
            if i % 10 == 0:  # 10% null user_id (primary key violation)
                customer['user_id'] = None
            if i % 8 == 0:   # 12.5% null email
                customer['email'] = None
            if i % 7 == 0:   # 14% blank name (empty string)
                customer['name'] = ''
            if i % 15 == 0:  # 6.7% null age
                customer['age'] = None
            if i % 20 == 0:  # 5% null/invalid dates
                customer['registration_date'] = None
                
            customers.append(customer)
        
        df = pd.DataFrame(customers)
        
        # Also test different representations of "missing"
        # Add some rows with string "NULL", "null", "N/A", empty strings
        for i in range(5):
            df.loc[len(df)] = {
                'user_id': f'USER{num_customers + i:06d}',
                'name': random.choice(['NULL', 'null', 'N/A', 'None', '']),
                'email': 'null',
                'phone': 'N/A',
                'age': None,
                'gender': '',
                'city': 'NULL',
                'state': None,
                'country': 'USA',
                'registration_date': 'N/A'
            }
        
        df.to_json(self.output_dir / 'user_data_with_nulls.json', orient='records', indent=2)
        df.to_csv(self.output_dir / 'user_data_with_nulls.csv', index=False)
        
        self.log_issue("NULL_VALUES", "NULL user_id (primary key)", df['user_id'].isna().sum(), "CRITICAL")
        self.log_issue("NULL_VALUES", "NULL email addresses", df['email'].isna().sum(), "HIGH")
        self.log_issue("NULL_VALUES", "Blank/empty names", (df['name'] == '').sum(), "MEDIUM")
        self.log_issue("NULL_VALUES", "NULL ages", df['age'].isna().sum(), "LOW")
        self.log_issue("NULL_VALUES", "NULL dates", df['registration_date'].isna().sum(), "HIGH")
        
        # Credit cards with null foreign keys
        num_cards = 80  # Intentionally less than customers to create orphans later
        credit_cards = pd.DataFrame({
            'user_id': [f'USER{i:06d}' if i % 5 != 0 else None for i in range(num_cards)],
            'card_number': [f'****-****-****-{random.randint(1000, 9999)}' if i % 6 != 0 else None for i in range(num_cards)],
            'card_type': [random.choice(['Visa', 'MasterCard', 'Amex']) if i % 12 != 0 else None for i in range(num_cards)],
            'issuing_bank': [random.choice(['Chase', 'Bank of America', 'Citi']) if i % 10 != 0 else '' for i in range(num_cards)]
        })
        
        credit_cards.to_pickle(self.output_dir / 'user_credit_card_with_nulls.pkl')
        
        self.log_issue("NULL_VALUES", "Credit card with NULL user_id (orphan)", credit_cards['user_id'].isna().sum(), "CRITICAL")
        self.log_issue("NULL_VALUES", "Credit card with NULL card_number", credit_cards['card_number'].isna().sum(), "CRITICAL")
        
        print(f"\n✓ Generated null/blank test data in {self.output_dir}")
        
    # ==================== SCENARIO 2: INVALID TYPES ====================
    
    def generate_invalid_types_scenario(self):
        """Generate data with type mismatches"""
        print("\n📊 SCENARIO 2: INVALID TYPES Testing")
        print("=" * 60)
        
        # Orders with type errors
        orders = []
        num_orders = 100
        
        for i in range(num_orders):
            order = {
                'order_id': f'ORD{i:08d}',
                'user_id': f'USER{random.randint(0, 49):06d}',
                'transaction_date': (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d %H:%M:%S'),
                'status': random.choice(['Completed', 'Pending', 'Shipped']),
                'total_amount': round(random.uniform(50, 500), 2)
            }
            
            # Inject type errors
            if i % 10 == 0:  # String in numeric field
                order['total_amount'] = "NOT_A_NUMBER"
            if i % 8 == 0:   # Invalid date format
                order['transaction_date'] = "2024-13-45"  # Invalid month/day
            if i % 12 == 0:  # Number as string where expected
                order['order_id'] = 12345  # Should be string
            if i % 15 == 0:  # Boolean where string expected
                order['status'] = True
                
            orders.append(order)
        
        df_orders = pd.DataFrame(orders)
        df_orders.to_csv(self.output_dir / 'orders_invalid_types.csv', index=False)
        df_orders.to_json(self.output_dir / 'orders_invalid_types.json', orient='records', indent=2)
        
        # Count type issues
        type_errors = sum(1 for x in df_orders['total_amount'] if isinstance(x, str) and x != "NOT_A_NUMBER")
        self.log_issue("TYPE_MISMATCH", "String in numeric field (total_amount)", 10, "CRITICAL")
        self.log_issue("TYPE_MISMATCH", "Invalid date formats", 12, "HIGH")
        self.log_issue("TYPE_MISMATCH", "Number in string field (order_id)", 8, "MEDIUM")
        
        # Line items with quantity/price issues
        line_items = []
        for i in range(100):
            item = {
                'order_id': f'ORD{random.randint(0, num_orders-1):08d}',
                'product_id': f'PROD{random.randint(0, 19):06d}',
                'quantity': random.randint(1, 10),
                'price': round(random.uniform(10, 100), 2),
            }
            
            # Inject issues
            if i % 7 == 0:
                item['quantity'] = "five"  # String instead of int
            if i % 9 == 0:
                item['price'] = "19.99USD"  # Currency symbol in numeric
            if i % 11 == 0:
                item['quantity'] = 3.14159  # Float where int expected
                
            line_items.append(item)
        
        df_items = pd.DataFrame(line_items)
        df_items.to_parquet(self.output_dir / 'line_items_invalid_types.parquet', index=False)
        
        self.log_issue("TYPE_MISMATCH", "String in quantity field", 14, "CRITICAL")
        self.log_issue("TYPE_MISMATCH", "String with currency in price field", 11, "HIGH")
        
        print(f"\n✓ Generated invalid type test data in {self.output_dir}")
        
    # ==================== SCENARIO 3: DUPLICATES ====================
    
    def generate_duplicates_scenario(self):
        """Generate data with various duplicate scenarios"""
        print("\n📊 SCENARIO 3: DUPLICATES Testing")
        print("=" * 60)
        
        # Generate base product data
        products = []
        num_products = 50
        
        for i in range(num_products):
            products.append({
                'product_id': f'PROD{i:06d}',
                'product_name': f'Product {i}',
                'category': random.choice(['Electronics', 'Clothing', 'Books']),
                'price': round(random.uniform(10, 500), 2),
                'brand': f'Brand {random.randint(1, 10)}'
            })
        
        df_products = pd.DataFrame(products)
        
        # Create duplicates
        # 1. Exact duplicates (same product_id and all fields)
        exact_duplicates = df_products.sample(n=10).copy()
        self.log_issue("DUPLICATES", "Exact duplicate rows (all fields match)", len(exact_duplicates), "HIGH")
        
        # 2. Duplicate primary keys with different data
        pk_duplicates = df_products.sample(n=5).copy()
        pk_duplicates['product_name'] = pk_duplicates['product_name'] + ' (Updated)'
        pk_duplicates['price'] = pk_duplicates['price'] * 1.1
        self.log_issue("DUPLICATES", "Duplicate primary keys (different data)", len(pk_duplicates), "CRITICAL")
        
        # 3. Near-duplicates (same name, different ID)
        near_duplicates = df_products.sample(n=8).copy()
        near_duplicates['product_id'] = [f'PROD{100+i:06d}' for i in range(len(near_duplicates))]
        self.log_issue("DUPLICATES", "Near-duplicates (same name, different ID)", len(near_duplicates), "MEDIUM")
        
        # Combine all
        df_all = pd.concat([df_products, exact_duplicates, pk_duplicates, near_duplicates], ignore_index=True)
        
        df_all.to_excel(self.output_dir / 'products_with_duplicates.xlsx', index=False, engine='openpyxl')
        df_all.to_csv(self.output_dir / 'products_with_duplicates.csv', index=False)
        
        # Also create duplicate orders (same order_id appearing multiple times)
        orders = []
        for i in range(60):
            orders.append({
                'order_id': f'ORD{i:08d}',
                'user_id': f'USER{random.randint(0, 20):06d}',
                'order_date': (datetime.now() - timedelta(days=random.randint(0, 90))).strftime('%Y-%m-%d'),
                'total': round(random.uniform(50, 500), 2)
            })
        
        # Add 15 duplicate order_ids
        duplicate_orders = random.sample(orders, 15)
        orders.extend(duplicate_orders)
        
        df_orders = pd.DataFrame(orders)
        df_orders.to_json(self.output_dir / 'orders_with_duplicates.json', orient='records', indent=2)
        
        self.log_issue("DUPLICATES", "Duplicate order IDs in transaction log", 15, "CRITICAL")
        
        print(f"\n✓ Generated duplicate test data in {self.output_dir}")
        
    # ==================== SCENARIO 4: BOUNDARY VALUES ====================
    
    def generate_boundary_values_scenario(self):
        """Generate data with edge case boundary values"""
        print("\n📊 SCENARIO 4: BOUNDARY VALUES Testing")
        print("=" * 60)
        
        # Products with boundary price values
        products = []
        
        # Normal products
        for i in range(40):
            products.append({
                'product_id': f'PROD{i:06d}',
                'product_name': f'Product {i}',
                'price': round(random.uniform(10, 500), 2),
                'cost': round(random.uniform(5, 250), 2),
                'stock': random.randint(1, 100)
            })
        
        # Boundary cases
        boundary_cases = [
            {'product_id': 'PROD000040', 'product_name': 'Zero Price Product', 'price': 0.00, 'cost': 10.0, 'stock': 50},
            {'product_id': 'PROD000041', 'product_name': 'Negative Price Product', 'price': -10.50, 'cost': 5.0, 'stock': 20},
            {'product_id': 'PROD000042', 'product_name': 'Extremely High Price', 'price': 999999.99, 'cost': 100.0, 'stock': 1},
            {'product_id': 'PROD000043', 'product_name': 'Cost > Price Product', 'price': 10.00, 'cost': 50.0, 'stock': 10},
            {'product_id': 'PROD000044', 'product_name': 'Zero Stock Product', 'price': 50.00, 'cost': 25.0, 'stock': 0},
            {'product_id': 'PROD000045', 'product_name': 'Negative Stock', 'price': 30.00, 'cost': 15.0, 'stock': -5},
            {'product_id': 'PROD000046', 'product_name': 'Penny Product', 'price': 0.01, 'cost': 0.01, 'stock': 1000},
            {'product_id': 'PROD000047', 'product_name': 'Max Integer Stock', 'price': 100.00, 'cost': 50.0, 'stock': 2147483647},
        ]
        
        products.extend(boundary_cases)
        df_products = pd.DataFrame(products)
        df_products.to_csv(self.output_dir / 'products_boundary_values.csv', index=False)
        
        self.log_issue("BOUNDARY_VALUE", "Zero price products", 1, "HIGH")
        self.log_issue("BOUNDARY_VALUE", "Negative price products", 1, "CRITICAL")
        self.log_issue("BOUNDARY_VALUE", "Extremely high prices (>999k)", 1, "MEDIUM")
        self.log_issue("BOUNDARY_VALUE", "Cost exceeds price (negative margin)", 1, "HIGH")
        self.log_issue("BOUNDARY_VALUE", "Zero or negative stock", 2, "MEDIUM")
        
        # Orders with boundary quantities
        orders = []
        for i in range(50):
            order = {
                'order_id': f'ORD{i:08d}',
                'product_id': f'PROD{random.randint(0, 39):06d}',
                'quantity': random.randint(1, 10),
                'price': round(random.uniform(10, 100), 2),
                'discount': round(random.uniform(0, 20), 2)
            }
            
            # Add boundary cases
            if i == 0:
                order['quantity'] = 0  # Zero quantity order
            elif i == 1:
                order['quantity'] = -1  # Negative quantity (return?)
            elif i == 2:
                order['discount'] = 100.0  # 100% discount (free)
            elif i == 3:
                order['discount'] = 150.0  # Discount > 100% (INVALID)
            elif i == 4:
                order['quantity'] = 999999  # Unrealistically large quantity
                
            orders.append(order)
        
        df_orders = pd.DataFrame(orders)
        df_orders.to_parquet(self.output_dir / 'orders_boundary_values.parquet', index=False)
        
        self.log_issue("BOUNDARY_VALUE", "Zero quantity orders", 1, "HIGH")
        self.log_issue("BOUNDARY_VALUE", "Negative quantity orders", 1, "CRITICAL")
        self.log_issue("BOUNDARY_VALUE", "100% discount orders", 1, "MEDIUM")
        self.log_issue("BOUNDARY_VALUE", "Discount > 100%", 1, "CRITICAL")
        self.log_issue("BOUNDARY_VALUE", "Unrealistically large quantities", 1, "MEDIUM")
        
        # Date boundary cases
        customers = []
        for i in range(30):
            customers.append({
                'user_id': f'USER{i:06d}',
                'name': f'Customer {i}',
                'registration_date': (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1000))).strftime('%Y-%m-%d'),
                'last_order_date': (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 400))).strftime('%Y-%m-%d'),
                'age': random.randint(18, 70)
            })
        
        # Add date boundary cases
        date_boundary_cases = [
            {'user_id': 'USER000030', 'name': 'Future Registration', 'registration_date': '2099-12-31', 'last_order_date': '2024-01-01', 'age': 25},
            {'user_id': 'USER000031', 'name': 'Ancient Registration', 'registration_date': '1900-01-01', 'last_order_date': '2024-01-01', 'age': 30},
            {'user_id': 'USER000032', 'name': 'Underage Customer', 'registration_date': '2020-01-01', 'last_order_date': '2024-01-01', 'age': 5},
            {'user_id': 'USER000033', 'name': 'Age 0 Customer', 'registration_date': '2023-01-01', 'last_order_date': '2024-01-01', 'age': 0},
            {'user_id': 'USER000034', 'name': 'Super Senior', 'registration_date': '2010-01-01', 'last_order_date': '2024-01-01', 'age': 150},
            {'user_id': 'USER000035', 'name': 'Order Before Registration', 'registration_date': '2024-06-01', 'last_order_date': '2024-01-01', 'age': 40},
        ]
        
        customers.extend(date_boundary_cases)
        df_customers = pd.DataFrame(customers)
        df_customers.to_json(self.output_dir / 'customers_boundary_dates.json', orient='records', indent=2)
        
        self.log_issue("BOUNDARY_VALUE", "Future registration dates", 1, "CRITICAL")
        self.log_issue("BOUNDARY_VALUE", "Registration dates before 1950", 1, "HIGH")
        self.log_issue("BOUNDARY_VALUE", "Underage customers (age < 13)", 2, "HIGH")
        self.log_issue("BOUNDARY_VALUE", "Unrealistic ages (> 120)", 1, "MEDIUM")
        self.log_issue("BOUNDARY_VALUE", "Orders before registration date", 1, "CRITICAL")
        
        print(f"\n✓ Generated boundary value test data in {self.output_dir}")
        
    # ==================== SCENARIO 5: SCHEMA DRIFT ====================
    
    def generate_schema_drift_scenario(self):
        """Generate data with intentional schema changes"""
        print("\n📊 SCENARIO 5: SCHEMA DRIFT Testing")
        print("=" * 60)
        
        # Version 1: Original schema
        print("\n  Creating ORIGINAL schema version...")
        customers_v1 = []
        for i in range(50):
            customers_v1.append({
                'user_id': f'USER{i:06d}',
                'name': f'Customer {i}',
                'email': f'user{i}@example.com',
                'age': random.randint(18, 70),
                'city': random.choice(['New York', 'Los Angeles', 'Chicago'])
            })
        
        df_v1 = pd.DataFrame(customers_v1)
        df_v1.to_csv(self.output_dir / 'customers_schema_v1.csv', index=False)
        
        # Version 2: Column renamed
        print("  Creating schema with RENAMED COLUMN...")
        customers_v2 = []
        for i in range(50, 100):
            customers_v2.append({
                'user_id': f'USER{i:06d}',
                'full_name': f'Customer {i}',  # RENAMED: name -> full_name
                'email': f'user{i}@example.com',
                'age': random.randint(18, 70),
                'city': random.choice(['New York', 'Los Angeles', 'Chicago'])
            })
        
        df_v2 = pd.DataFrame(customers_v2)
        df_v2.to_csv(self.output_dir / 'customers_schema_v2_renamed_column.csv', index=False)
        self.log_issue("SCHEMA_DRIFT", "Column renamed: 'name' → 'full_name'", 50, "HIGH")
        
        # Version 3: Column dropped
        print("  Creating schema with DROPPED COLUMN...")
        customers_v3 = []
        for i in range(100, 150):
            customers_v3.append({
                'user_id': f'USER{i:06d}',
                'name': f'Customer {i}',
                'email': f'user{i}@example.com',
                'city': random.choice(['New York', 'Los Angeles', 'Chicago'])
                # DROPPED: age column removed
            })
        
        df_v3 = pd.DataFrame(customers_v3)
        df_v3.to_csv(self.output_dir / 'customers_schema_v3_dropped_column.csv', index=False)
        self.log_issue("SCHEMA_DRIFT", "Column dropped: 'age' no longer present", 50, "MEDIUM")
        
        # Version 4: New column added
        print("  Creating schema with NEW COLUMN...")
        customers_v4 = []
        for i in range(150, 200):
            customers_v4.append({
                'user_id': f'USER{i:06d}',
                'name': f'Customer {i}',
                'email': f'user{i}@example.com',
                'age': random.randint(18, 70),
                'city': random.choice(['New York', 'Los Angeles', 'Chicago']),
                'country': 'USA',  # NEW COLUMN
                'loyalty_tier': random.choice(['Bronze', 'Silver', 'Gold'])  # NEW COLUMN
            })
        
        df_v4 = pd.DataFrame(customers_v4)
        df_v4.to_json(self.output_dir / 'customers_schema_v4_new_columns.json', orient='records', indent=2)
        self.log_issue("SCHEMA_DRIFT", "New columns added: 'country', 'loyalty_tier'", 50, "LOW")
        
        # Version 5: Data type changed
        print("  Creating schema with CHANGED DATA TYPES...")
        customers_v5 = []
        for i in range(200, 250):
            customers_v5.append({
                'user_id': i,  # CHANGED: string -> integer
                'name': f'Customer {i}',
                'email': f'user{i}@example.com',
                'age': str(random.randint(18, 70)),  # CHANGED: integer -> string
                'city': random.choice(['New York', 'Los Angeles', 'Chicago'])
            })
        
        df_v5 = pd.DataFrame(customers_v5)
        df_v5.to_excel(self.output_dir / 'customers_schema_v5_type_changes.xlsx', index=False, engine='openpyxl')
        self.log_issue("SCHEMA_DRIFT", "Data type changed: user_id (str→int), age (int→str)", 50, "CRITICAL")
        
        # Version 6: Column order changed
        print("  Creating schema with REORDERED COLUMNS...")
        customers_v6 = []
        for i in range(250, 300):
            customers_v6.append({
                'email': f'user{i}@example.com',  # Reordered
                'user_id': f'USER{i:06d}',
                'city': random.choice(['New York', 'Los Angeles', 'Chicago']),
                'age': random.randint(18, 70),
                'name': f'Customer {i}'
            })
        
        df_v6 = pd.DataFrame(customers_v6)
        df_v6.to_csv(self.output_dir / 'customers_schema_v6_reordered.csv', index=False)
        self.log_issue("SCHEMA_DRIFT", "Column order changed (should handle gracefully)", 50, "LOW")
        
        # Version 7: Completely different schema
        print("  Creating COMPLETELY DIFFERENT schema...")
        customers_v7 = []
        for i in range(300, 350):
            customers_v7.append({
                'customer_identifier': f'CUST{i:06d}',  # Different naming convention
                'personal_info': f'Customer {i}',
                'contact_email': f'user{i}@example.com',
                'location': random.choice(['NYC', 'LA', 'CHI']),
                'member_since': (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1000))).strftime('%Y-%m-%d'),
                'status': random.choice(['Active', 'Inactive', 'Suspended'])
            })
        
        df_v7 = pd.DataFrame(customers_v7)
        df_v7.to_parquet(self.output_dir / 'customers_schema_v7_completely_different.parquet', index=False)
        self.log_issue("SCHEMA_DRIFT", "Completely different schema (breaking change)", 50, "CRITICAL")
        
        # Create a schema evolution manifest
        schema_manifest = {
            'schema_versions': [
                {
                    'version': 'v1',
                    'file': 'customers_schema_v1.csv',
                    'columns': ['user_id', 'name', 'email', 'age', 'city'],
                    'description': 'Original schema'
                },
                {
                    'version': 'v2',
                    'file': 'customers_schema_v2_renamed_column.csv',
                    'columns': ['user_id', 'full_name', 'email', 'age', 'city'],
                    'description': 'Renamed column: name → full_name'
                },
                {
                    'version': 'v3',
                    'file': 'customers_schema_v3_dropped_column.csv',
                    'columns': ['user_id', 'name', 'email', 'city'],
                    'description': 'Dropped column: age'
                },
                {
                    'version': 'v4',
                    'file': 'customers_schema_v4_new_columns.json',
                    'columns': ['user_id', 'name', 'email', 'age', 'city', 'country', 'loyalty_tier'],
                    'description': 'Added columns: country, loyalty_tier'
                },
                {
                    'version': 'v5',
                    'file': 'customers_schema_v5_type_changes.xlsx',
                    'columns': ['user_id', 'name', 'email', 'age', 'city'],
                    'description': 'Type changes: user_id (str→int), age (int→str)'
                },
                {
                    'version': 'v6',
                    'file': 'customers_schema_v6_reordered.csv',
                    'columns': ['email', 'user_id', 'city', 'age', 'name'],
                    'description': 'Reordered columns (same data)'
                },
                {
                    'version': 'v7',
                    'file': 'customers_schema_v7_completely_different.parquet',
                    'columns': ['customer_identifier', 'personal_info', 'contact_email', 'location', 'member_since', 'status'],
                    'description': 'Completely different schema (breaking change)'
                }
            ]
        }
        
        with open(self.output_dir / 'schema_evolution_manifest.json', 'w') as f:
            json.dump(schema_manifest, f, indent=2)
        
        print(f"\n✓ Generated schema drift test data with 7 versions in {self.output_dir}")
        
    # ==================== COMPREHENSIVE SCENARIO ====================
    
    def generate_all_scenarios(self):
        """Generate all edge case scenarios"""
        print("\n" + "="*60)
        print("GENERATING ALL EDGE CASE SCENARIOS")
        print("="*60)
        
        self.generate_null_values_scenario()
        self.generate_invalid_types_scenario()
        self.generate_duplicates_scenario()
        self.generate_boundary_values_scenario()
        self.generate_schema_drift_scenario()
        
        self._generate_summary_report()
        
    def _generate_summary_report(self):
        """Generate a summary report of all injected issues"""
        print("\n" + "="*60)
        print("DATA QUALITY ISSUES SUMMARY REPORT")
        print("="*60)
        
        df_report = pd.DataFrame(self.report)
        
        # Group by category and severity
        print("\n📋 Issues by Category:")
        category_summary = df_report.groupby('category')['count'].sum().sort_values(ascending=False)
        for cat, count in category_summary.items():
            print(f"  • {cat}: {count} total issues")
        
        print("\n⚠️  Issues by Severity:")
        severity_summary = df_report.groupby('severity')['count'].sum().sort_values(ascending=False)
        for sev, count in severity_summary.items():
            print(f"  • {sev}: {count} cases")
        
        # Save detailed report
        df_report.to_csv(self.output_dir / 'QUALITY_ISSUES_REPORT.csv', index=False)
        
        # Create README
        readme_content = f"""# Edge Case Test Data - Quality Issues Report

Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Overview

This test dataset contains **intentional data quality issues** designed to validate your data warehouse's ability to detect, handle, and report data quality problems.

## Issue Categories

{self._format_category_table(df_report)}

## Critical Issues Injected

{self._format_critical_issues(df_report)}

## Testing Instructions

1. **Run your data ingestion pipeline** with this test dataset
2. **Monitor your data quality checks** - they should detect these issues
3. **Verify handling logic** - how does your pipeline handle each type of issue?
4. **Check reporting** - are all issues logged and reported correctly?

## Expected Pipeline Behavior

- **CRITICAL issues**: Pipeline should FAIL or QUARANTINE affected records
- **HIGH issues**: Pipeline should LOG WARNING and possibly skip records
- **MEDIUM issues**: Pipeline should LOG and attempt to clean/fix
- **LOW issues**: Pipeline should handle gracefully (may log for info)

## Files Generated

{self._list_generated_files()}

## Success Criteria

✅ All {len(self.report)} issue types are detected by your quality checks  
✅ Issues are categorized by severity correctly  
✅ Appropriate actions taken based on severity  
✅ Comprehensive quality report generated  
✅ Clean records are processed successfully  

"""
        
        with open(self.output_dir / 'README.md', 'w') as f:
            f.write(readme_content)
        
        print(f"\n📄 Detailed report saved to: {self.output_dir / 'QUALITY_ISSUES_REPORT.csv'}")
        print(f"📄 README saved to: {self.output_dir / 'README.md'}")
        print(f"\n✅ All edge case scenarios generated successfully!")
        
    def _format_category_table(self, df):
        """Format category summary as markdown table"""
        category_summary = df.groupby('category').agg({
            'count': 'sum',
            'severity': lambda x: ', '.join(sorted(set(x)))
        }).reset_index()
        
        lines = [
            "| Category | Total Issues | Severities |",
            "|----------|--------------|------------|"
        ]
        
        for _, row in category_summary.iterrows():
            lines.append(f"| {row['category']} | {row['count']} | {row['severity']} |")
        
        return '\n'.join(lines)
    
    def _format_critical_issues(self, df):
        """Format critical issues list"""
        critical = df[df['severity'] == 'CRITICAL']
        lines = []
        for _, issue in critical.iterrows():
            lines.append(f"- **{issue['description']}**: {issue['count']} cases")
        return '\n'.join(lines) if lines else "None"
    
    def _list_generated_files(self):
        """List all generated files"""
        files = [f for f in os.listdir(self.output_dir) if os.path.isfile(self.output_dir / f)]
        return '\n'.join([f"- `{f}`" for f in sorted(files)])


def main():
    parser = argparse.ArgumentParser(
        description='Generate test data with edge cases and data quality issues',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate_test_data_edge_cases.py --scenario nulls
  python generate_test_data_edge_cases.py --scenario duplicates --output data/test_cases/TC-Q-03/
  python generate_test_data_edge_cases.py --scenario all --output data/test_cases/comprehensive/
        """
    )
    
    parser.add_argument(
        '--scenario',
        choices=['nulls', 'invalid_types', 'duplicates', 'boundary_values', 'schema_drift', 'all'],
        default='all',
        help='Type of edge case scenario to generate'
    )
    
    parser.add_argument(
        '--output',
        default='data/test_cases/edge_cases/',
        help='Output directory for test data files'
    )
    
    args = parser.parse_args()
    
    generator = EdgeCaseTestDataGenerator(output_dir=args.output)
    
    if args.scenario == 'nulls':
        generator.generate_null_values_scenario()
    elif args.scenario == 'invalid_types':
        generator.generate_invalid_types_scenario()
    elif args.scenario == 'duplicates':
        generator.generate_duplicates_scenario()
    elif args.scenario == 'boundary_values':
        generator.generate_boundary_values_scenario()
    elif args.scenario == 'schema_drift':
        generator.generate_schema_drift_scenario()
    elif args.scenario == 'all':
        generator.generate_all_scenarios()
    
    generator._generate_summary_report()


if __name__ == '__main__':
    main()
