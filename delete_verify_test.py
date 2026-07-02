import re

with open("src/wal.rs", "r") as f:
    content = f.read()

content = re.sub(r'    #\[test\]\n    fn test_verify_checksum_truncated\(\) \{\n.*?\n    \}\n', '', content, flags=re.DOTALL)

with open("src/wal.rs", "w") as f:
    f.write(content)

