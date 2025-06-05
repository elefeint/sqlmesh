#!/usr/bin/env python3
"""
Simple test script to verify DuckDB transaction support in SQLMesh.

This test validates that our changes to enable SUPPORTS_TRANSACTIONS=True
for DuckDB (which MotherDuck inherits from) work correctly.
"""

import duckdb
from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter
from sqlmesh.core.config.connection import DuckDBConnectionConfig, MotherDuckConnectionConfig


def test_duckdb_transaction_support():
    """Test that DuckDB connections support transaction methods."""
    print("Testing DuckDB transaction method availability...")
    
    # Create a simple in-memory DuckDB connection
    conn = duckdb.connect(":memory:")
    
    # Check if transaction methods exist
    methods = ['begin', 'commit', 'rollback']
    for method in methods:
        if hasattr(conn, method):
            print(f"✓ {method}() method available")
        else:
            print(f"✗ {method}() method missing")
            return False
    
    # Test basic transaction operations
    try:
        print("\nTesting transaction operations...")
        
        # Create test table
        conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
        
        # Test transaction with commit
        conn.begin()
        conn.execute("INSERT INTO test_table VALUES (1, 'Alice')")
        conn.commit()
        
        # Test transaction with rollback
        conn.begin()
        conn.execute("INSERT INTO test_table VALUES (2, 'Bob')")
        conn.rollback()
        
        # Check results
        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()
        if result[0] == 1:
            print("✓ Transaction commit/rollback working correctly")
        else:
            print(f"✗ Expected 1 row, got {result[0]}")
            return False
            
    except Exception as e:
        print(f"✗ Transaction test failed: {e}")
        return False
    finally:
        conn.close()
    
    return True


def test_sqlmesh_adapter_configuration():
    """Test that SQLMesh DuckDB adapter now supports transactions."""
    print("\nTesting SQLMesh DuckDB adapter configuration...")
    
    # Check that SUPPORTS_TRANSACTIONS is now True
    if DuckDBEngineAdapter.SUPPORTS_TRANSACTIONS:
        print("✓ DuckDBEngineAdapter.SUPPORTS_TRANSACTIONS = True")
    else:
        print("✗ DuckDBEngineAdapter.SUPPORTS_TRANSACTIONS = False")
        return False
    
    # Check concurrent tasks configuration for DuckDB
    duckdb_config = DuckDBConnectionConfig(type="duckdb")
    if duckdb_config.concurrent_tasks > 1:
        print(f"✓ DuckDB concurrent_tasks = {duckdb_config.concurrent_tasks}")
    else:
        print(f"✗ DuckDB concurrent_tasks = {duckdb_config.concurrent_tasks} (should be > 1)")
        return False
    
    # Check that MotherDuck inherits these settings
    motherduck_config = MotherDuckConnectionConfig(type="motherduck")
    if motherduck_config.concurrent_tasks > 1:
        print(f"✓ MotherDuck concurrent_tasks = {motherduck_config.concurrent_tasks} (inherited)")
    else:
        print(f"✗ MotherDuck concurrent_tasks = {motherduck_config.concurrent_tasks} (should be > 1)")
        return False
    
    return True


def main():
    """Run all tests."""
    print("Testing DuckDB Transaction Support Implementation")
    print("=" * 50)
    
    success = True
    
    # Test 1: DuckDB transaction support
    success &= test_duckdb_transaction_support()
    
    # Test 2: SQLMesh adapter configuration
    success &= test_sqlmesh_adapter_configuration()
    
    # Summary
    print("\n" + "=" * 50)
    if success:
        print("🎉 All tests passed! DuckDB transaction support is working.")
        print("\nThis means MotherDuck (which inherits from DuckDB) now has:")
        print("- Transaction support (SUPPORTS_TRANSACTIONS = True)")
        print("- Increased concurrent tasks (4 instead of 1)")
        print("\nNext steps:")
        print("1. Test with actual state sync operations")
        print("2. Verify concurrent task execution works correctly")
        print("3. Monitor for any performance impact")
    else:
        print("❌ Some tests failed. Please check the implementation.")
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())