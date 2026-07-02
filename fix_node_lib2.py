import re

with open("bindings/node/src/lib.rs", "r") as f:
    content = f.read()

# remove duplicate blocks
content = re.sub(r'        if let Some\(v\) = config\.memtable_size_mb \{\n            cfg\.memtable_size_mb = v as usize;\n        \}\n        if let Some\(v\) = config\.block_cache_capacity_mb \{\n            cfg\.block_cache_capacity_mb = v as usize;\n        \}\n        if let Some\(v\) = config\.bloom_bits_per_key \{\n            cfg\.bloom_bits_per_key = v as usize;\n        \}\n', '', content)

with open("bindings/node/src/lib.rs", "w") as f:
    f.write(content)

