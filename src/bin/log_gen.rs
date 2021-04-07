use log::debug;
use prototype::error::Result;
use std::env::current_dir;
use std::fs::File;
use std::io::BufRead;

fn main() -> Result<()> {
    env_logger::init();

    let path = current_dir()?;
    debug!("The current directory is {}", path.display());

    let file = File::open("./src/bin/mary.txt")?;

    let reader = std::io::BufReader::new(file);
    for line in reader.lines() {
        debug!("{}", line.unwrap());
    }

    Ok(())
}
