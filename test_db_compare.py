"""
Test script for db_compare.py
Creates sample SQLite databases and runs comparison tests
"""

import sqlite3
import os
import pandas as pd
from db_compare import DBCompare, DataSource


def create_sample_databases():
    """Create sample SQLite databases for testing"""

    print("Creating sample databases...")

    # Create output directory
    os.makedirs("test_data", exist_ok=True)

    # Database 1: "before" database
    conn1 = sqlite3.connect("test_data/db_before.db")
    cursor1 = conn1.cursor()

    # Create table
    cursor1.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            customer_name TEXT,
            email TEXT,
            balance REAL,
            status TEXT
        )
    """)

    # Insert test data
    data1 = [
        (1, "Alice Smith", "alice@example.com", 1000.00, "active"),
        (2, "Bob Jones", "bob@example.com", 2500.50, "active"),
        (3, "Charlie Brown", "charlie@example.com", 750.25, "active"),
        (4, "Diana Prince", "diana@example.com", 3000.00, "active"),
        (5, "Eve Adams", "eve@example.com", 500.00, "inactive"),
    ]

    cursor1.executemany(
        "INSERT OR REPLACE INTO customers VALUES (?, ?, ?, ?, ?)",
        data1
    )

    conn1.commit()
    conn1.close()

    print("Created db_before.db")

    # Database 2: "after" database (with some changes)
    conn2 = sqlite3.connect("test_data/db_after.db")
    cursor2 = conn2.cursor()

    # Create table
    cursor2.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            customer_name TEXT,
            email TEXT,
            balance REAL,
            status TEXT
        )
    """)

    # Insert test data with changes
    data2 = [
        (1, "Alice Smith", "alice@example.com", 1000.00, "active"),      # Same
        (2, "Bob Jones", "bob_new@example.com", 2500.50, "active"),     # Email changed
        (3, "Charlie Brown", "charlie@example.com", 755.25, "active"),  # Balance changed
        (4, "Diana Prince", "diana@example.com", 3000.00, "inactive"),  # Status changed
        # (5 is missing - deleted customer)
        (6, "Frank Miller", "frank@example.com", 1200.00, "active"),    # New customer
    ]

    cursor2.executemany(
        "INSERT OR REPLACE INTO customers VALUES (?, ?, ?, ?, ?)",
        data2
    )

    conn2.commit()
    conn2.close()

    print("Created db_after.db")
    print("Sample databases created successfully!\n")


def test_basic_comparison():
    """Test basic database comparison"""

    print("=" * 80)
    print("Test 1: Basic Comparison")
    print("=" * 80)

    source1 = DataSource(
        name="before",
        db_type="sqlite",
        database="test_data/db_before.db",
        host="",
        port=0,
        username="",
        password="",
        sql_query="SELECT * FROM customers WHERE status = 'active'"
    )

    source2 = DataSource(
        name="after",
        db_type="sqlite",
        database="test_data/db_after.db",
        host="",
        port=0,
        username="",
        password="",
        sql_query="SELECT * FROM customers WHERE status = 'active'"
    )

    comparator = DBCompare(
        data_source1=source1,
        data_source2=source2,
        join_columns="customer_id",
        output_dir="test_output/test1",
        log_file="test_output/test1.log"
    )

    results = comparator.run_comparison()

    print(f"\nResults: {results}")
    print("✓ Test 1 completed\n")

    return results


def test_with_tolerance():
    """Test comparison with numeric tolerance"""

    print("=" * 80)
    print("Test 2: Comparison with Tolerance")
    print("=" * 80)

    source1 = DataSource(
        name="before",
        db_type="sqlite",
        database="test_data/db_before.db",
        host="",
        port=0,
        username="",
        password="",
        sql_query="SELECT customer_id, customer_name, balance FROM customers"
    )

    source2 = DataSource(
        name="after",
        db_type="sqlite",
        database="test_data/db_after.db",
        host="",
        port=0,
        username="",
        password="",
        sql_query="SELECT customer_id, customer_name, balance FROM customers"
    )

    comparator = DBCompare(
        data_source1=source1,
        data_source2=source2,
        join_columns="customer_id",
        tolerance={
            "balance": 10.0  # Allow $10 difference
        },
        output_dir="test_output/test2",
        log_file="test_output/test2.log"
    )

    results = comparator.run_comparison()

    print(f"\nResults: {results}")
    print("✓ Test 2 completed\n")

    return results


def test_selective_columns():
    """Test comparison with specific columns only"""

    print("=" * 80)
    print("Test 3: Selective Column Comparison")
    print("=" * 80)

    source1 = DataSource(
        name="before",
        db_type="sqlite",
        database="test_data/db_before.db",
        host="",
        port=0,
        username="",
        password="",
        sql_query="SELECT * FROM customers"
    )

    source2 = DataSource(
        name="after",
        db_type="sqlite",
        database="test_data/db_after.db",
        host="",
        port=0,
        username="",
        password="",
        sql_query="SELECT * FROM customers"
    )

    comparator = DBCompare(
        data_source1=source1,
        data_source2=source2,
        join_columns="customer_id",
        comparing_columns=["customer_name", "balance"],  # Only compare these columns
        output_dir="test_output/test3",
        log_file="test_output/test3.log"
    )

    results = comparator.run_comparison()

    print(f"\nResults: {results}")
    print("✓ Test 3 completed\n")

    return results


def test_differences_only():
    """Test generating only differences report"""

    print("=" * 80)
    print("Test 4: Differences Only Report")
    print("=" * 80)

    source1 = DataSource(
        name="before",
        db_type="sqlite",
        database="test_data/db_before.db",
        host="",
        port=0,
        username="",
        password="",
        sql_query="SELECT * FROM customers"
    )

    source2 = DataSource(
        name="after",
        db_type="sqlite",
        database="test_data/db_after.db",
        host="",
        port=0,
        username="",
        password="",
        sql_query="SELECT * FROM customers"
    )

    comparator = DBCompare(
        data_source1=source1,
        data_source2=source2,
        join_columns="customer_id",
        output_dir="test_output/test4",
        log_file="test_output/test4.log"
    )

    results = comparator.run_comparison(
        generate_full_csv=False,
        generate_diff_csv=True,
        generate_summary=True,
        validate_dups=True
    )

    print(f"\nResults: {results}")
    print("✓ Test 4 completed\n")

    return results


def view_results():
    """Display some results for verification"""

    print("=" * 80)
    print("Sample Results from Test 1")
    print("=" * 80)

    # Read the differences file
    try:
        df = pd.read_csv("test_output/test1/differences_only.csv")
        print(f"\nDifferences found: {len(df)} rows\n")
        print(df.to_string(index=False))
    except FileNotFoundError:
        print("No differences file found")

    print("\n")

    # Read summary report
    try:
        with open("test_output/test1/summary_report.txt", "r") as f:
            print(f.read())
    except FileNotFoundError:
        print("No summary file found")


if __name__ == "__main__":
    print("Database Comparison Test Suite")
    print("=" * 80)
    print()

    # Create test databases
    create_sample_databases()

    # Run tests
    try:
        test1_results = test_basic_comparison()
        test2_results = test_with_tolerance()
        test3_results = test_selective_columns()
        test4_results = test_differences_only()

        # View results
        view_results()

        print("\n" + "=" * 80)
        print("All tests completed successfully!")
        print("=" * 80)
        print("\nTest outputs are available in the test_output/ directory:")
        print("  - test_output/test1/ - Basic comparison")
        print("  - test_output/test2/ - With tolerance")
        print("  - test_output/test3/ - Selective columns")
        print("  - test_output/test4/ - Differences only")
        print("\nCheck the CSV and summary files in each directory.")

    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()

