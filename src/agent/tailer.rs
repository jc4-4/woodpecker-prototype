use crate::error::Result;
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Continuously tails a log file from previous offset.
/// Return a buffer of log events aligned by regex.
/// Detect and chase to new file upon end-of-file.
// See the kinesis agent for checkpoints etc. https://github.com/awslabs/amazon-kinesis-agent
pub struct Tailer<'a> {
    path: &'a Path,
    file: File,
    buffer: Vec<u8>,
}

impl Tailer<'_> {
    fn try_new(path: &str, buffer_size: usize) -> Result<Tailer> {
        let path = Path::new(path);
        let file = File::open(path)?;
        Ok(Tailer {
            path,
            file,
            buffer: vec![0; buffer_size],
        })
    }

    fn read(&mut self) -> Result<Option<&[u8]>> {
        let bytes = self.file.read(&mut *self.buffer)?;
        if bytes == 0 {
            Ok(None)
        } else {
            Ok(Some(&self.buffer[0..bytes]))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::agent::tailer::Tailer;
    use log::{debug, info};
    use std::env::current_dir;
    use std::fs::{create_dir_all, remove_file, File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;
    use std::str::from_utf8;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn read_write() {
        init();
        let content = b"Mary has a little lamb\nLittle lamb,\nlittle lamb";
        let mut file = File::create("foo.txt").unwrap();
        file.write_all(content).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let mut tailer = Tailer::try_new("foo.txt", 10).unwrap();
        let mut bytes = 0;
        while let Some(v) = tailer.read().unwrap() {
            debug!("length: {} content: {}", v.len(), from_utf8(v).unwrap());
            bytes += v.len();
        }
        remove_file("foo.txt");
        assert_eq!(bytes, content.len());
    }
}
