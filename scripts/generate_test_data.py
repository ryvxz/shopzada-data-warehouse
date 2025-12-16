"""
Test Data Generator for ShopZada Data Warehouse Test Cases
Generates synthetic datasets for comprehensive testing

Usage:
    python generate_test_data.py --size minimal --output data/test_cases/TC-E-03/
    python generate_test_data.py --size standard --output data/test_cases/standard_dataset/
    python generate_test_data.py --size large --output data/test_cases/performance_test/
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import argparse
from pathlib import Path


class TestDataGenerator:
    """Generate synthetic test data for ShopZada DWH testing"""
    
    SIZES = {
        'minimal': {
            'orders': 100,
            'customers': 50,
            'products': 20,
            'merchants': 10,
            'staff': 15,
            'campaigns': 5,
            'days': 30
        },
        'standard': {
            'orders': 10000,
            'customers': 5000,
            'products': 500,
            'merchants': 100,
            'staff': 150,
            'campaigns': 20,
            'days': 365
        },
        'large': {
            'orders': 1000000,
            'customers': 50000,
            'products': 5000,
            'merchants': 1000,
            'staff': 1500,
            'campaigns': 100,
            'days': 1095  # 3 years
        }
    }
    
    def __init__(self, size='minimal', output_dir='data/test_cases/'):
        self.config = self.SIZES.get(size, self.SIZES['minimal'])
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def generate_customers(self, num_customers):
        """Generate customer data in multiple formats"""
        
        # Main customer data (JSON format)
        customers = []
        for i in range(num_customers):
            customer = {
                'user_id': f'USER{i:06d}',
                'name': f'Customer {i}',
                'email': f'user{i}@example.com',
                'phone': f'+1-555-{random.randint(1000, 9999)}',
                'age': random.randint(18, 80),
                'gender': random.choice(['Male', 'Female', 'Other']),
                'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia']),
                'state': random.choice(['NY', 'CA', 'IL', 'TX', 'AZ', 'PA']),
                'country': 'USA',
                'customer_type': random.choice(['Regular', 'Premium', 'VIP']),
                'registration_date': (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))).strftime('%Y-%m-%d')
            }
            customers.append(customer)
        
        df = pd.DataFrame(customers)
        df.to_json(self.output_dir / 'user_data.json', orient='records', indent=2)
        
        # Customer credit cards (Pickle format)
        credit_cards = pd.DataFrame({
            'user_id': [f'USER{i:06d}' for i in range(num_customers)],
            'card_number': [f'****-****-****-{random.randint(1000, 9999)}' for _ in range(num_customers)],
            'card_type': [random.choice(['Visa', 'MasterCard', 'Amex']) for _ in range(num_customers)],
            'issuing_bank': [random.choice(['Chase', 'Bank of America', 'Citi', 'Wells Fargo']) for _ in range(num_customers)]
        })
        credit_cards.to_pickle(self.output_dir / 'user_credit_card.pkl')
        
        # Customer jobs (CSV format)
        jobs = pd.DataFrame({
            'user_id': [f'USER{i:06d}' for i in range(num_customers)],
            'job_title': [random.choice(['Engineer', 'Manager', 'Analyst', 'Designer', 'Developer']) for _ in range(num_customers)],
            'job_level': [random.choice(['Junior', 'Mid', 'Senior', 'Lead']) for _ in range(num_customers)],
            'company': [f'Company {random.randint(1, 100)}' for _ in range(num_customers)]
        })
        jobs.to_csv(self.output_dir / 'user_job.csv', index=False)
        
        print(f"✓ Generated {num_customers} customers (JSON, Pickle, CSV)")
        
    def generate_products(self, num_products):
        """Generate product catalog (Excel format)"""
        
        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Beauty']
        
        products = pd.DataFrame({
            'product_id': [f'PROD{i:06d}' for i in range(num_products)],
            'product_name': [f'Product {i}' for i in range(num_products)],
            'product_type': [random.choice(categories) for _ in range(num_products)],
            'price': np.random.uniform(10, 500, num_products).round(2),
            'cost': np.random.uniform(5, 250, num_products).round(2),
            'brand': [f'Brand {random.randint(1, 50)}' for _ in range(num_products)],
            'description': [f'Description for product {i}' for i in range(num_products)]
        })
        
        products.to_excel(self.output_dir / 'product_list.xlsx', index=False, engine='openpyxl')
        print(f"✓ Generated {num_products} products (Excel)")
        
    def generate_merchants(self, num_merchants):
        """Generate merchant data (HTML table format)"""
        
        merchants = pd.DataFrame({
            'merchant_id': [f'MERCH{i:05d}' for i in range(num_merchants)],
            'merchant_name': [f'Merchant {i}' for i in range(num_merchants)],
            'city': [random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston']) for _ in range(num_merchants)],
            'state': [random.choice(['NY', 'CA', 'IL', 'TX']) for _ in range(num_merchants)],
            'rating': np.random.uniform(3.0, 5.0, num_merchants).round(1),
            'years_in_business': [random.randint(1, 20) for _ in range(num_merchants)]
        })
        
        # Save as HTML table
        html_content = f"""
        <html>
        <head><title>Merchant Data</title></head>
        <body>
            <h1>ShopZada Merchant Directory</h1>
            {merchants.to_html(index=False)}
        </body>
        </html>
        """
        
        with open(self.output_dir / 'merchant_data.html', 'w') as f:
            f.write(html_content)
            
        print(f"✓ Generated {num_merchants} merchants (HTML)")
        
    def generate_staff(self, num_staff):
        """Generate staff data (HTML format)"""
        
        staff = pd.DataFrame({
            'staff_id': [f'STAFF{i:05d}' for i in range(num_staff)],
            'staff_name': [f'Staff Member {i}' for i in range(num_staff)],
            'job_level': [random.choice(['Entry', 'Mid', 'Senior', 'Manager']) for _ in range(num_staff)],
            'department': [random.choice(['Warehouse', 'Delivery', 'Customer Service', 'Management']) for _ in range(num_staff)],
            'hire_date': [(datetime(2015, 1, 1) + timedelta(days=random.randint(0, 3650))).strftime('%Y-%m-%d') for _ in range(num_staff)]
        })
        
        html_content = f"""
        <html>
        <head><title>Staff Data</title></head>
        <body>
            <h1>ShopZada Staff Directory</h1>
            {staff.to_html(index=False)}
        </body>
        </html>
        """
        
        with open(self.output_dir / 'staff_data.html', 'w') as f:
            f.write(html_content)
            
        print(f"✓ Generated {num_staff} staff members (HTML)")
        
    def generate_campaigns(self, num_campaigns):
        """Generate marketing campaigns (CSV format)"""
        
        campaigns = pd.DataFrame({
            'campaign_id': [f'CAMP{i:04d}' for i in range(num_campaigns)],
            'campaign_name': [f'Campaign {i}' for i in range(num_campaigns)],
            'discount_percentage': np.random.uniform(5, 30, num_campaigns).round(1),
            'start_date': [(datetime(2024, 1, 1) + timedelta(days=random.randint(0, 300))).strftime('%Y-%m-%d') for _ in range(num_campaigns)],
            'end_date': [(datetime(2024, 1, 1) + timedelta(days=random.randint(301, 365))).strftime('%Y-%m-%d') for _ in range(num_campaigns)],
            'budget': [random.randint(5000, 100000) for _ in range(num_campaigns)]
        })
        
        campaigns.to_csv(self.output_dir / 'campaign_data.csv', index=False)
        print(f"✓ Generated {num_campaigns} campaigns (CSV)")
        
    def generate_orders(self, num_orders, num_customers, num_products, num_merchants, num_staff, days):
        """Generate order data in multiple formats"""
        
        start_date = datetime.now() - timedelta(days=days)
        
        # Generate base order data
        orders = []
        line_items = []
        order_merchant_mappings = []
        
        for i in range(num_orders):
            order_id = f'ORD{i:08d}'
            user_id = f'USER{random.randint(0, num_customers - 1):06d}'
            transaction_date = (start_date + timedelta(days=random.randint(0, days))).strftime('%Y-%m-%d %H:%M:%S')
            
            # Number of line items per order
            num_line_items = random.randint(1, 5)
            
            for j in range(num_line_items):
                product_id = f'PROD{random.randint(0, num_products - 1):06d}'
                quantity = random.randint(1, 10)
                price = round(random.uniform(10, 500), 2)
                subtotal = round(quantity * price, 2)
                
                line_items.append({
                    'order_id': order_id,
                    'product_id': product_id,
                    'quantity': quantity,
                    'price': price,
                    'subtotal': subtotal
                })
            
            # Order metadata
            merchant_id = f'MERCH{random.randint(0, num_merchants - 1):05d}'
            staff_id = f'STAFF{random.randint(0, num_staff - 1):05d}'
            
            orders.append({
                'order_id': order_id,
                'user_id': user_id,
                'transaction_date': transaction_date,
                'status': random.choice(['Completed', 'Pending', 'Shipped', 'Delivered'])
            })
            
            order_merchant_mappings.append({
                'order_id': order_id,
                'merchant_id': merchant_id,
                'staff_id': staff_id
            })
        
        # Save in multiple formats (CSV, JSON, Parquet, Excel, HTML)
        df_orders = pd.DataFrame(orders)
        df_line_items = pd.DataFrame(line_items)
        df_mappings = pd.DataFrame(order_merchant_mappings)
        
        # Split orders across different formats
        split_point_1 = len(df_orders) // 5
        split_point_2 = 2 * len(df_orders) // 5
        split_point_3 = 3 * len(df_orders) // 5
        split_point_4 = 4 * len(df_orders) // 5
        
        # CSV
        df_orders.iloc[:split_point_1].to_csv(self.output_dir / 'order_data_2020.csv', index=False)
        # JSON
        df_orders.iloc[split_point_1:split_point_2].to_json(self.output_dir / 'order_data_2021.json', orient='records', indent=2)
        # Excel
        df_orders.iloc[split_point_2:split_point_3].to_excel(self.output_dir / 'order_data_2022.xlsx', index=False, engine='openpyxl')
        # Parquet
        df_orders.iloc[split_point_3:split_point_4].to_parquet(self.output_dir / 'order_data_2023.parquet', index=False)
        # HTML
        html_content = f"""
        <html>
        <head><title>Order Data 2024</title></head>
        <body>
            <h1>Order Data 2024</h1>
            {df_orders.iloc[split_point_4:].to_html(index=False)}
        </body>
        </html>
        """
        with open(self.output_dir / 'order_data_2024.html', 'w') as f:
            f.write(html_content)
        
        # Line items (split between CSV and Parquet)
        split_line = len(df_line_items) // 2
        df_line_items.iloc[:split_line].to_csv(self.output_dir / 'line_item_prices.csv', index=False)
        df_line_items.iloc[split_line:].to_parquet(self.output_dir / 'line_item_products.parquet', index=False)
        
        # Order-merchant mappings (CSV and Parquet)
        split_map = len(df_mappings) // 2
        df_mappings.iloc[:split_map].to_csv(self.output_dir / 'order_merchant_mapping_part1.csv', index=False)
        df_mappings.iloc[split_map:].to_parquet(self.output_dir / 'order_merchant_mapping_part2.parquet', index=False)
        
        print(f"✓ Generated {num_orders} orders with {len(line_items)} line items (CSV, JSON, Excel, Parquet, HTML)")
        
    def generate_campaign_transactions(self, num_orders, num_campaigns):
        """Generate campaign transaction data"""
        
        # Random subset of orders have campaign usage
        num_campaign_transactions = int(num_orders * 0.3)  # 30% of orders use campaigns
        
        campaign_transactions = pd.DataFrame({
            'order_id': [f'ORD{random.randint(0, num_orders - 1):08d}' for _ in range(num_campaign_transactions)],
            'campaign_id': [f'CAMP{random.randint(0, num_campaigns - 1):04d}' for _ in range(num_campaign_transactions)],
            'discount_availed': np.random.uniform(5, 50, num_campaign_transactions).round(2)
        })
        
        campaign_transactions.to_csv(self.output_dir / 'campaign_transactions.csv', index=False)
        print(f"✓ Generated {num_campaign_transactions} campaign transactions (CSV)")
        
    def generate_order_delays(self, num_orders):
        """Generate order delay data (HTML)"""
        
        # Random subset of orders have delays
        num_delays = int(num_orders * 0.15)  # 15% of orders have delays
        
        delays = pd.DataFrame({
            'order_id': random.sample([f'ORD{i:08d}' for i in range(num_orders)], num_delays),
            'delay_in_days': [random.randint(1, 30) for _ in range(num_delays)],
            'reason': [random.choice(['Weather', 'Traffic', 'Warehouse Issue', 'Carrier Delay']) for _ in range(num_delays)]
        })
        
        html_content = f"""
        <html>
        <head><title>Order Delays</title></head>
        <body>
            <h1>Delayed Orders</h1>
            {delays.to_html(index=False)}
        </body>
        </html>
        """
        
        with open(self.output_dir / 'order_delays.html', 'w') as f:
            f.write(html_content)
            
        print(f"✓ Generated {num_delays} order delays (HTML)")
        
    def generate_all(self):
        """Generate complete test dataset"""
        print(f"\nGenerating test dataset in: {self.output_dir}")
        print(f"Configuration: {self.config}\n")
        
        self.generate_customers(self.config['customers'])
        self.generate_products(self.config['products'])
        self.generate_merchants(self.config['merchants'])
        self.generate_staff(self.config['staff'])
        self.generate_campaigns(self.config['campaigns'])
        self.generate_orders(
            self.config['orders'],
            self.config['customers'],
            self.config['products'],
            self.config['merchants'],
            self.config['staff'],
            self.config['days']
        )
        self.generate_campaign_transactions(self.config['orders'], self.config['campaigns'])
        self.generate_order_delays(self.config['orders'])
        
        print(f"\n✅ Test dataset generation complete!")
        print(f"📁 Output directory: {self.output_dir.absolute()}")
        

def main():
    parser = argparse.ArgumentParser(description='Generate test data for ShopZada DWH')
    parser.add_argument('--size', choices=['minimal', 'standard', 'large'], default='minimal',
                        help='Size of test dataset to generate')
    parser.add_argument('--output', default='data/test_cases/generated/',
                        help='Output directory for test data files')
    
    args = parser.parse_args()
    
    generator = TestDataGenerator(size=args.size, output_dir=args.output)
    generator.generate_all()


if __name__ == '__main__':
    main()
