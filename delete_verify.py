import re

with open("src/wal.rs", "r") as f:
    content = f.read()

content = re.sub(r'/// Verifies the trailing checksum on a decoded record buffer\.\n/// `record_bytes` is everything BEFORE the checksum trailer\.\n/// `file_tail` is the remaining file data starting at the checksum position\.\n/// Returns `true` if the checksum matches \(or is too short to read → treated as\n/// corruption: returns `false`\)\.\nfn verify_checksum\(record_bytes: &\[u8\], file_tail: &\[u8\]\) -> bool \{\n    if file_tail\.len\(\) < CHECKSUM_LEN \{\n        return false;\n    \}\n    let expected = compute_checksum\(record_bytes\);\n    expected == file_tail\[\.\.CHECKSUM_LEN\]\n\}\n', '', content, flags=re.DOTALL)

with open("src/wal.rs", "w") as f:
    f.write(content)

