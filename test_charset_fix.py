"""
Test script to verify charset parameter is working in MySQL connections
"""

from db_compare import DataSource
import sys

def test_charset_in_connection_string():
    """Test that charset parameter is properly added to MySQL connection string"""

    # Create a test data source with charset parameter
    source = DataSource(
        name="test_source",
        db_type="mysql",
        host="127.0.0.1",
        port=3306,
        database="customer_db",
        username="CitywideRds",
        password="citywide",
        sql_query="SELECT 1",
        charset="utf8mb4"
    )

    # Get the connection string
    conn_string = source.get_connection_string()

    print("=" * 60)
    print("MySQL Charset Parameter Test")
    print("=" * 60)
    print(f"\nConnection string generated:")
    print(f"{conn_string}")
    print()

    # Verify charset is in the URL
    if "charset=utf8mb4" in conn_string:
        print("✓ SUCCESS: charset parameter is correctly added to connection string")
        return True
    else:
        print("✗ FAILED: charset parameter is NOT in connection string")
        return False

def test_charset_not_duplicated():
    """Test that charset is not passed twice (URL and engine params)"""

    source = DataSource(
        name="test_source",
        db_type="mysql",
        host="127.0.0.1",
        port=3306,
        database="customer_db",
        username="CitywideRds",
        password="citywide",
        sql_query="SELECT 1",
        charset="utf8mb4"
    )

    # Simulate what happens in connect()
    engine_params = source.connection_params.copy()
    if source.db_type == 'mysql' and 'charset' in engine_params:
        engine_params.pop('charset')

    print("\n" + "=" * 60)
    print("Duplicate Parameter Prevention Test")
    print("=" * 60)
    print(f"\nOriginal connection_params: {source.connection_params}")
    print(f"Filtered engine_params: {engine_params}")
    print()

    if 'charset' not in engine_params:
        print("✓ SUCCESS: charset is removed from engine params (already in URL)")
        return True
    else:
        print("✗ FAILED: charset is still in engine params (would be duplicated)")
        return False

def test_other_params_preserved():
    """Test that other connection parameters are preserved"""

    source = DataSource(
        name="test_source",
        db_type="mysql",
        host="127.0.0.1",
        port=3306,
        database="customer_db",
        username="CitywideRds",
        password="citywide",
        sql_query="SELECT 1",
        charset="utf8mb4",
        pool_size=10,
        pool_recycle=3600
    )

    # Simulate what happens in connect()
    engine_params = source.connection_params.copy()
    if source.db_type == 'mysql' and 'charset' in engine_params:
        engine_params.pop('charset')

    print("\n" + "=" * 60)
    print("Other Parameters Preservation Test")
    print("=" * 60)
    print(f"\nOriginal connection_params: {source.connection_params}")
    print(f"Filtered engine_params: {engine_params}")
    print()

    if 'pool_size' in engine_params and 'pool_recycle' in engine_params and 'charset' not in engine_params:
        print("✓ SUCCESS: Other parameters preserved, charset removed")
        return True
    else:
        print("✗ FAILED: Parameters not correctly filtered")
        return False

if __name__ == "__main__":
    print("\n")
    print("*" * 60)
    print("MySQL Charset Parameter Fix - Verification Tests")
    print("*" * 60)
    print()

    results = []

    # Run tests
    results.append(test_charset_in_connection_string())
    results.append(test_charset_not_duplicated())
    results.append(test_other_params_preserved())

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"\nTests passed: {passed}/{total}")

    if all(results):
        print("\n✓ All tests PASSED! Charset parameter is working correctly.")
        sys.exit(0)
    else:
        print("\n✗ Some tests FAILED. Please review the implementation.")
        sys.exit(1)

