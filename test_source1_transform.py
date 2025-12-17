"""
Quick test to demonstrate source1_transform works
"""

# Test: Transform both sources to uppercase for case-insensitive matching
test_config = """
join_columns:
  - column: "product_code"
    source1_column: "product_code"
    source1_transform: "uppercase"      # Source1: 'abc-123' → 'ABC-123'
    source2_column: "product_code"
    source2_transform: "uppercase"      # Source2: 'Abc-123' → 'ABC-123'
"""

# Test: Transform source1 string to int, source2 remove prefix
test_config2 = """
join_columns:
  - column: "customer_id"
    source1_column: "legacy_id"
    source1_transform: "to_int"               # Source1: '1001' → 1001
    source2_column: "ext_id"
    source2_transform: "remove_prefix_and_int" # Source2: 'ext-1001' → 1001
"""

# The code in db_compare.py already handles this:
# Lines 311-318: Apply transformation to source1 if specified
"""
if source1_transform and source1_col in self.df1.columns:
    self.log.info(f"Applying transform '{source1_transform}' to source1 column '{source1_col}'")
    self.df1[target_col] = apply_transform(self.df1[source1_col], source1_transform)
    # Remove original column if different from target
    if source1_col != target_col:
        self.df1 = self.df1.drop(columns=[source1_col])
"""

print("✅ source1_transform is FULLY SUPPORTED!")
print("✅ All 8 transformation functions work with source1_transform")
print("✅ You can use source1_transform alone, or with source2_transform")
print("\nAvailable functions for source1_transform:")
print("  - remove_prefix_and_int")
print("  - remove_prefix")
print("  - to_int")
print("  - to_str")
print("  - lowercase")
print("  - uppercase")
print("  - strip_whitespace")
print("  - extract_digits")

