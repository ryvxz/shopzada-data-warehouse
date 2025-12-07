import pandas as pd
from datetime import datetime
import os

# --- Configuration ---
# Use a default report directory, but it should be configurable via environment variable
REPORT_DIR = os.getenv("REPORT_DIR") 

# --- Helper Function for Formatting ---

def create_markdown_table(df: pd.DataFrame, title: str) -> str:
    """Converts a small summary DataFrame into a Markdown table string."""
    markdown_str = f"### {title}\n\n"
    # Ensure the index is included in the table output
    table_lines = df.to_markdown(index=True)
    markdown_str += table_lines + "\n\n"
    return markdown_str

# --- Main Report Generation Function ---

def generate_quality_report(staging_tables: dict, validation_results: dict):
    """
    Generates a Markdown report summarizing data ingestion and quality checks.

    Parameters:
        staging_tables: Dictionary of {table_name: DataFrame} *after* validation.
        validation_results: Dictionary containing metrics from the validation step.
    """
    REPORT_DIR = os.getenv("REPORT_DIR", "/opt/airflow/plugins/data/reports")
    os.makedirs(REPORT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file_name = f"data_quality_report_{timestamp}.md"
    report_path = os.path.join(REPORT_DIR, report_file_name)
    
    print(f"\n--- Generating Data Quality Report at: {report_path} ---")

    report_content = []
    
    # 1. Report Header
    report_content.append(f"# Data Ingestion and Quality Report\n")
    report_content.append(f"**Generated On:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    report_content.append(f"**Total Tables Processed:** {len(staging_tables)}\n\n")
    report_content.append("---\n")
    
    # 2. Section for Each Table
    for table_name, df in staging_tables.items():
        report_content.append(f"\n## 📊 Table: `{table_name}`\n")
        report_content.append(f"**Final Row Count:** {len(df):,}\n")
        report_content.append(f"**Column Count:** {len(df.columns)}\n")

        # Get relevant metrics from the validation_results (if implemented)
        # Since we haven't tightly coupled the metrics, we'll calculate them here for simplicity
        
        # A. Null Value Summary
        null_counts = df.isnull().sum()
        null_summary = pd.DataFrame({
            'Nulls': null_counts,
            'Percentage': (null_counts / len(df) * 100).round(2).astype(str) + '%'
        })
        null_summary = null_summary[null_summary['Nulls'] > 0]
        
        if not null_summary.empty:
            report_content.append(create_markdown_table(
                null_summary.sort_values(by='Nulls', ascending=False),
                "Columns with Remaining Null Values"
            ))
        else:
            report_content.append("### Columns with Remaining Null Values\n\nNo null values detected (or they were handled by the reader/standardization).\n\n")
            
        # B. Data Type Summary (Helps verify standardization worked)
        dtype_summary = pd.DataFrame(df.dtypes, columns=['Data Type'])
        report_content.append(create_markdown_table(
            dtype_summary,
            "Final Column Data Types"
        ))
        
        # C. Sample Data (Optional: show the first 5 rows)
        report_content.append("### Sample Data (First 5 Rows)\n\n")
        report_content.append(df.head(5).to_markdown(index=False))
        report_content.append("\n\n---\n")

    # Write the report file
    with open(report_path, 'w') as f:
        f.write("\n".join(report_content))

    print(f"--- Data Quality Report Successfully Saved to {report_path} ---\n")