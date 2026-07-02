import re

files = [
    ("Cargo.toml", r'version = "0.7.5"', r'version = "0.7.6"'),
    ("Cargo.toml", r'flowdb-derive = \{ version = "0.7.5"', r'flowdb-derive = { version = "0.7.6"'),
    ("flowdb-derive/Cargo.toml", r'version = "0.7.5"', r'version = "0.7.6"'),
    ("bindings/node/Cargo.toml", r'version = "0.7.5"', r'version = "0.7.6"'),
    ("bindings/node/package.json", r'"version": "0.7.5"', r'"version": "0.7.6"'),
    ("bindings/node/package.json", r'"@restsend/flowdb-darwin-arm64": "0.7.5"', r'"@restsend/flowdb-darwin-arm64": "0.7.6"'),
    ("bindings/node/package.json", r'"@restsend/flowdb-darwin-x64": "0.7.5"', r'"@restsend/flowdb-darwin-x64": "0.7.6"'),
    ("bindings/node/package.json", r'"@restsend/flowdb-linux-x64-gnu": "0.7.5"', r'"@restsend/flowdb-linux-x64-gnu": "0.7.6"'),
    ("bindings/node/package.json", r'"@restsend/flowdb-linux-arm64-gnu": "0.7.5"', r'"@restsend/flowdb-linux-arm64-gnu": "0.7.6"'),
    ("bindings/node/package.json", r'"@restsend/flowdb-win32-x64-msvc": "0.7.5"', r'"@restsend/flowdb-win32-x64-msvc": "0.7.6"'),
]

for filename, old, new in files:
    with open(filename, "r") as f:
        content = f.read()
    content = re.sub(old, new, content)
    with open(filename, "w") as f:
        f.write(content)

