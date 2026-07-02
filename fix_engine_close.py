import re

with open("src/engine.rs", "r") as f:
    content = f.read()

# Replace maintenance field in Engine struct
content = re.sub(
    r'    storage: Arc<dyn StorageBackend>,\n    maintenance: Option<MaintenanceHandle>,',
    r'    storage: Arc<dyn StorageBackend>,\n    maintenance: parking_lot::Mutex<Option<MaintenanceHandle>>,\n    is_closed: std::sync::atomic::AtomicBool,',
    content
)

# Replace in Engine::open
content = re.sub(
    r'            storage,\n            maintenance: None,\n            patch_lock: std::sync::Mutex::new\(\(\)\),\n        \};',
    r'            storage,\n            maintenance: parking_lot::Mutex::new(None),\n            is_closed: std::sync::atomic::AtomicBool::new(false),\n            patch_lock: std::sync::Mutex::new(()),\n        };',
    content
)

# Replace in Engine::spawn_background_maintenance
content = re.sub(
    r'    pub fn spawn_background_maintenance\(&mut self\) \{',
    r'    pub fn spawn_background_maintenance(&self) {',
    content
)

content = re.sub(
    r'        self\.maintenance = Some\(MaintenanceHandle \{',
    r'        *self.maintenance.lock() = Some(MaintenanceHandle {',
    content
)

# Replace in Engine::shutdown
content = re.sub(
    r'    pub fn shutdown\(mut self\) -> Result<\(\)> \{.*?        self\.maintenance\.take\(\);\n        let mut worker = self\.worker\.worker\.lock\(\);\n        worker\.do_flush\(\)\?;\n        worker\.flush_wal\(\)\?\;\n        Ok\(\(\)\)\n    \}',
    r'    pub fn shutdown(self) -> Result<()> {\n        self.close()\n    }',
    content,
    flags=re.DOTALL
)

# Replace in Engine::close
close_new = """    pub fn close(&self) -> Result<()> {
        if self.is_closed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }

        // Stop background thread
        *self.maintenance.lock() = None;

        let mut w = self.worker.worker.lock();
        w.do_flush()?;
        w.flush_wal()
    }"""
content = re.sub(
    r'    pub fn close\(&self\) -> Result<\(\)> \{.*?        w\.do_flush\(\)\?;\n        w\.flush_wal\(\)\n    \}',
    close_new,
    content,
    flags=re.DOTALL
)

# Check is_closed in methods
check_closed = """
        if self.is_closed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(FlowError::Closed);
        }
"""
methods_to_patch = [
    r'    pub fn write_batch_sync\(&self, records: &\[Record\]\) -> Result<\(\)> \{',
    r'    pub fn write_batch_ttl_sync\(&self, records: &\[Record\], ttl_secs: Option<u64>\) -> Result<\(\)> \{',
    r'    pub fn write_batch\(&self, records: &\[Record\]\) -> Result<\(\)> \{',
    r'    pub fn write_batch_ttl\(&self, records: &\[Record\], ttl_secs: Option<u64>\) -> Result<\(\)> \{',
    r'    pub fn write_batch_owned\(&self, records: Vec<Record>\) -> Result<\(\)> \{',
    r'    pub fn write_batch_owned_ttl\(&self, records: Vec<Record>, ttl_secs: Option<u64>\) -> Result<\(\)> \{',
    r'    pub fn delete_range\(&self, start: &str, end: &str\) -> Result<\(\)> \{',
    r'    pub fn patch_record_sync\(&self, key: &\[u8\], patch: serde_json::Value\) -> Result<\(\)> \{',
    r'    pub fn patch_record\(&self, key: &\[u8\], patch: serde_json::Value\) -> Result<\(\)> \{',
    r'    pub fn get_latest\(&self, key: &\[u8\]\) -> Result<Option<Record>> \{',
    r'    pub fn query\(&self, query: Query\) -> Result<ScanIterator> \{',
]

for method in methods_to_patch:
    # insert check_closed immediately after the opening brace
    content = re.sub(
        method,
        method.replace(' {', ' {\n' + check_closed),
        content
    )

with open("src/engine.rs", "w") as f:
    f.write(content)

