import re

with open("src/manifest.rs", "r") as f:
    content = f.read()

open_manifest_new = """            let lines: Vec<&str> = content.lines().map(|l| l.trim()).filter(|l| !l.is_empty()).collect();
            for (i, line) in lines.iter().enumerate() {
                match serde_json::from_str::<ManifestEntry>(line) {
                    Ok(entry) => apply_entry(&mut state, &entry),
                    Err(e) => {
                        if i == lines.len() - 1 {
                            tracing::warn!("Ignoring torn final manifest entry: {}", e);
                        } else {
                            return Err(crate::error::FlowError::Corruption {
                                file: path.display().to_string(),
                                msg: format!("Corrupted manifest entry at line {}: {}", i + 1, e),
                            }
                            .into());
                        }
                    }
                }
            }"""

content = re.sub(r'            for line in content\.lines\(\) \{.*?\n            \}', open_manifest_new, content, flags=re.DOTALL)

with open("src/manifest.rs", "w") as f:
    f.write(content)

