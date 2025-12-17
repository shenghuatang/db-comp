"""
Example usage of db_compare.py
Demonstrates different comparison scenarios
"""

from db_compare import DBCompare, DataSource


def example1_basic_mysql_comparison():
    """
    Example 1: Basic MySQL database comparison
    """
    print("\n" + "="*80)
    print("Example 1: Basic MySQL Comparison")
    print("="*80)

    # Configure first data source (Production)
    source1 = DataSource(
        name="production",
        db_type="mysql",
        host="localhost",
        port=3306,
        database="prod_db",
        username="root",
        password="password",
        sql_query="""
            SELECT 
                customer_id,
                customer_name,
                total_orders,
                total_amount
            FROM customer_summary
            WHERE date = '2023-12-01'
        """
    )

    # Configure second data source (Staging)
    source2 = DataSource(
        name="staging",
        db_type="mysql",
        host="localhost",
        port=3306,
        database="stage_db",
        username="root",
        password="password",
        sql_query="""
            SELECT 
                customer_id,
                customer_name,
                total_orders,
                total_amount
            FROM customer_summary
            WHERE date = '2023-12-01'
        """
    )

    # Create comparator
    comparator = DBCompare(
        data_source1=source1,
        data_source2=source2,
        join_columns="customer_id",  # Single join column
        comparing_columns=["customer_name", "total_orders", "total_amount"],
        output_dir="output/example1"
    )

    # Run comparison
    results = comparator.run_comparison()

    print(f"\nResults: {results}")


def example2_postgresql_with_tolerance():
    """
    Example 2: PostgreSQL comparison with numeric tolerance
    """
    print("\n" + "="*80)
    print("Example 2: PostgreSQL with Tolerance")
    print("="*80)

    source1 = DataSource(
        name="old_system",
        db_type="postgresql",
        host="localhost",
        port=5432,
        database="finance_old",
        username="postgres",
        password="password",
        sql_query="""
            SELECT 
                account_id,
                account_name,
                balance,
                interest_earned
            FROM accounts
        """
    )

    source2 = DataSource(
        name="new_system",
        db_type="postgresql",
        host="localhost",
        port=5432,
        database="finance_new",
        username="postgres",
        password="password",
        sql_query="""
            SELECT 
                account_id,
                account_name,
                balance,
                interest_earned
            FROM accounts
        """
    )

    comparator = DBCompare(
        data_source1=source1,
        data_source2=source2,
        join_columns="account_id",
        comparing_columns=["account_name", "balance", "interest_earned"],
        tolerance={
            "balance": 0.01,        # Allow $0.01 difference
            "interest_earned": 0.001 # Allow $0.001 difference
        },
        output_dir="output/example2"
    )

    results = comparator.run_comparison()
    print(f"\nResults: {results}")


def example3_composite_key():
    """
    Example 3: Comparison using composite keys
    """
    print("\n" + "="*80)
    print("Example 3: Composite Key Comparison")
    print("="*80)

    source1 = DataSource(
        name="source_system",
        db_type="mysql",
        host="localhost",
        port=3306,
        database="sales_db",
        username="root",
        password="password",
        sql_query="""
            SELECT 
                store_id,
                product_id,
                transaction_date,
                quantity,
                revenue
            FROM daily_sales
            WHERE transaction_date = '2023-12-01'
        """
    )

    source2 = DataSource(
        name="warehouse",
        db_type="mysql",
        host="192.168.1.100",
        port=3306,
        database="warehouse_db",
        username="root",
        password="password",
        sql_query="""
            SELECT 
                store_id,
                product_id,
                transaction_date,
                quantity,
                revenue
            FROM sales_summary
            WHERE transaction_date = '2023-12-01'
        """
    )

    comparator = DBCompare(
        data_source1=source1,
        data_source2=source2,
        join_columns=["store_id", "product_id", "transaction_date"],  # Composite key
        comparing_columns=["quantity", "revenue"],
        tolerance={"revenue": 0.01},
        output_dir="output/example3"
    )

    results = comparator.run_comparison()
    print(f"\nResults: {results}")


def example4_cross_database():
    """
    Example 4: Compare data across different database types
    """
    print("\n" + "="*80)
    print("Example 4: Cross-Database Comparison (MySQL vs PostgreSQL)")
    print("="*80)

    # MySQL source
    mysql_source = DataSource(
        name="mysql_db",
        db_type="mysql",
        host="localhost",
        port=3306,
        database="reporting",
        username="root",
        password="password",
        sql_query="""
            SELECT 
                employee_id,
                employee_name,
                department,
                salary
            FROM employees
        """
    )

    # PostgreSQL source
    pg_source = DataSource(
        name="postgres_db",
        db_type="postgresql",
        host="localhost",
        port=5432,
        database="hr_system",
        username="postgres",
        password="password",
        sql_query="""
            SELECT 
                employee_id,
                employee_name,
                department,
                salary
            FROM staff
        """
    )

    comparator = DBCompare(
        data_source1=mysql_source,
        data_source2=pg_source,
        join_columns="employee_id",
        output_dir="output/example4"
    )

    results = comparator.run_comparison()
    print(f"\nResults: {results}")


def example5_sqlite_comparison():
    """
    Example 5: SQLite database comparison (useful for testing)
    """
    print("\n" + "="*80)
    print("Example 5: SQLite Comparison")
    print("="*80)

    source1 = DataSource(
        name="db_before",
        db_type="sqlite",
        host="",  # Not used for SQLite
        port=0,   # Not used for SQLite
        database="data/before.db",  # File path
        username="",  # Not used for SQLite
        password="",  # Not used for SQLite
        sql_query="SELECT id, name, value FROM records"
    )

    source2 = DataSource(
        name="db_after",
        db_type="sqlite",
        host="",
        port=0,
        database="data/after.db",
        username="",
        password="",
        sql_query="SELECT id, name, value FROM records"
    )

    comparator = DBCompare(
        data_source1=source1,
        data_source2=source2,
        join_columns="id",
        output_dir="output/example5"
    )

    results = comparator.run_comparison()
    print(f"\nResults: {results}")


def example6_selective_reports():
    """
    Example 6: Generate only specific reports
    """
    print("\n" + "="*80)
    print("Example 6: Selective Report Generation")
    print("="*80)

    source1 = DataSource(
        name="current",
        db_type="mysql",
        host="localhost",
        port=3306,
        database="inventory",
        username="root",
        password="password",
        sql_query="SELECT sku, product_name, quantity FROM inventory"
    )

    source2 = DataSource(
        name="expected",
        db_type="mysql",
        host="localhost",
        port=3306,
        database="inventory_expected",
        username="root",
        password="password",
        sql_query="SELECT sku, product_name, quantity FROM inventory"
    )

    comparator = DBCompare(
        data_source1=source1,
        data_source2=source2,
        join_columns="sku",
        tolerance={"quantity": 1},  # Allow difference of 1 unit
        output_dir="output/example6"
    )

    # Run comparison with selective reports
    results = comparator.run_comparison(
        generate_full_csv=False,      # Don't generate full CSV
        generate_diff_csv=True,       # Only generate differences
        generate_summary=True,        # Generate summary
        validate_dups=True           # Validate duplicates
    )

    print(f"\nResults: {results}")


if __name__ == "__main__":
    print("Database Comparison Examples")
    print("=" * 80)
    print("\nChoose an example to run:")
    print("1. Basic MySQL Comparison")
    print("2. PostgreSQL with Tolerance")
    print("3. Composite Key Comparison")
    print("4. Cross-Database Comparison")
    print("5. SQLite Comparison")
    print("6. Selective Report Generation")
    print("\nNote: Update connection details and SQL queries before running!")

    # Uncomment the example you want to run:
    # example1_basic_mysql_comparison()
    # example2_postgresql_with_tolerance()
    # example3_composite_key()
    # example4_cross_database()
    # example5_sqlite_comparison()
    # example6_selective_reports()

