import re

with open("bindings/node/src/lib.rs", "r") as f:
    content = f.read()

config_new = """        if let Some(v) = config.default_ttl_secs {
            if v < 0 { return Err(napi::Error::from_reason("default_ttl_secs cannot be negative")); }
            cfg.default_ttl_secs = Some(v as u64);
        }
        if let Some(v) = config.memtable_size_mb {
            if v <= 0 { return Err(napi::Error::from_reason("memtable_size_mb must be > 0")); }
            cfg.memtable_size_mb = v as usize;
        }
        if let Some(v) = config.block_cache_capacity_mb {
            if v < 0 { return Err(napi::Error::from_reason("block_cache_capacity_mb cannot be negative")); }
            cfg.block_cache_capacity_mb = v as usize;
        }
        if let Some(v) = config.bloom_bits_per_key {
            if v <= 0 { return Err(napi::Error::from_reason("bloom_bits_per_key must be > 0")); }
            cfg.bloom_bits_per_key = v as usize;
        }"""

content = re.sub(
    r'        if let Some\(v\) = config\.default_ttl_secs \{.*?\n        \}',
    config_new,
    content,
    flags=re.DOTALL
)

with open("bindings/node/src/lib.rs", "w") as f:
    f.write(content)

