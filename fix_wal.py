import re

with open("src/wal.rs", "r") as f:
    content = f.read()

# Fix skip_record
skip_record_new = """    fn skip_record(&self, data: &[u8], start: usize) -> Result<Option<usize>> {
        let mut pos = start;
        pos += 8;
        pos += 1;

        if pos + 2 > data.len() {
            return Ok(None);
        }
        let key_len = read_u16(&data[pos..pos + 2]) as usize;
        pos += 2 + key_len;

        if pos + 16 > data.len() {
            return Ok(None);
        }
        pos += 16;

        if pos + 4 > data.len() {
            return Ok(None);
        }
        let range_end_len = read_u32(&data[pos..pos + 4]) as usize;
        pos += 4 + range_end_len;

        if pos + 4 > data.len() {
            return Ok(None);
        }
        let val_len = read_u32(&data[pos..pos + 4]) as usize;
        pos += 4 + val_len;

        if pos + CHECKSUM_LEN > data.len() {
            return Ok(None);
        }

        let record_bytes = &data[start..pos];
        let tail = &data[pos..pos + CHECKSUM_LEN];
        let expected = compute_checksum(record_bytes);
        if expected != tail {
            return Err(crate::error::FlowError::Corruption {
                file: "WAL".into(),
                msg: "Checksum mismatch in WAL record".into(),
            }
            .into());
        }

        pos += CHECKSUM_LEN;

        Ok(Some(pos - start))
    }"""

content = re.sub(r'    fn skip_record.*?Ok\(Some\(pos - start\)\)\n    }', skip_record_new, content, flags=re.DOTALL)

# Fix decode_record
decode_record_new = """    // Verify per-record checksum.
    if pos + CHECKSUM_LEN > data.len() {
        return Ok(None);
    }
    let record_bytes = &data[..pos];
    let tail = &data[pos..pos + CHECKSUM_LEN];
    let expected = compute_checksum(record_bytes);
    if expected != tail {
        return Err(crate::error::FlowError::Corruption {
            file: "WAL".into(),
            msg: "Checksum mismatch in WAL record".into(),
        }
        .into());
    }
    pos += CHECKSUM_LEN;"""

content = re.sub(r'    // Verify per-record checksum\..*?pos \+= CHECKSUM_LEN;', decode_record_new, content, flags=re.DOTALL)

# Fix replay_from err matching
replay_from_new = """                match decode_record(&data[pos..]) {
                    Ok(Some((rec, advance))) => {
                        if rec.seq > after_seq {
                            records.push(rec);
                        }
                        pos += advance;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::error!("Corruption in WAL {}: {}", segment.path.display(), e);
                        return Err(e);
                    }
                }"""

content = re.sub(r'                match decode_record\(&data\[pos\.\.\]\) \{.*?Ok\(None\) => break,\n                    Err\(_\) => break,\n                \}', replay_from_new, content, flags=re.DOTALL)

# Fix find_max_seq_in_segment err matching
find_max_seq_new = """            if let Some(len) = self.skip_record(&data, pos)? {
                max_seq = max_seq.max(seq);
                pos += len;
            } else {
                break;
            }"""
content = re.sub(r'            if let Some\(len\) = self\.skip_record\(&data, pos\)\? \{.*?\n            \} else \{\n                break;\n            \}', find_max_seq_new, content, flags=re.DOTALL)


with open("src/wal.rs", "w") as f:
    f.write(content)

