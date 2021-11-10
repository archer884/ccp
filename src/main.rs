use std::{
    collections::HashMap,
    ffi::OsString,
    fs::{self, File},
    io::{self, BufReader, BufWriter},
    path::{Path, PathBuf},
    thread,
};

use clap::Parser;
use rayon::prelude::*;

#[derive(Clone, Debug, Parser)]
struct Opts {
    #[clap(required = true)]
    from: Vec<String>,
    to: String,
    // I'm much too lazy to implement this at the moment.
    // #[clap(short, long)]
    // recurse: bool,
}

impl Opts {
    fn paths(&'_ self) -> impl Iterator<Item = PathBuf> + '_ {
        self.from
            .iter()
            .filter_map(|candidate| {
                let path = Path::new(candidate);

                if path.is_dir() {
                    return fs::read_dir(path).ok().map(PathSource::Dir);
                }

                if path.is_file() {
                    return Some(PathSource::Literal(Some(path.into())));
                }

                glob::glob(candidate).ok().map(PathSource::Glob)
            })
            .flatten()
    }
}

enum PathSource {
    Dir(fs::ReadDir),
    Glob(glob::Paths),
    Literal(Option<PathBuf>),
}

impl Iterator for PathSource {
    type Item = PathBuf;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            PathSource::Dir(paths) => paths
                .next()
                .transpose()
                .ok()
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.is_file()),
            PathSource::Glob(paths) => paths.next().transpose().ok().flatten(),
            PathSource::Literal(path) => path.take(),
        }
    }
}

fn main() {
    let opts = Opts::parse();
    if let Err(e) = run(&opts) {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}

fn run(opts: &Opts) -> anyhow::Result<()> {
    let from: Vec<_> = opts.paths().collect();
    let from2 = from.clone(); // fuck me running
                              // let to = Path::new(&opts.to).canonicalize()?;
    let to = Path::new(&opts.to);
    let to_dir = to.is_dir();

    if from.len() > 1 && !to_dir {
        anyhow::bail!("many to one paths - probable data loss");
    }

    // I'm doing this in a weird way in an effort to hash these files concurrent with
    // the copy operation.
    let hashes_task = thread::spawn(move || build_hashes(from2));

    let mut destination_paths = Vec::new();
    for from in from.iter() {
        let mut source = File::open(&from).map(BufReader::new)?;
        if to_dir {
            let to = to.join(from.file_name().unwrap());
            let mut destination = File::create(&to).map(BufWriter::new)?;
            io::copy(&mut source, &mut destination)?;
            destination_paths.push(to);
        } else {
            let mut destination = File::create(&to).map(BufWriter::new)?;
            io::copy(&mut source, &mut destination)?;
            destination_paths.push(to.into());
        }
    }

    let source_hashes = hashes_task.join().unwrap()?;
    let destination_hashes = build_hashes(destination_paths)?;

    for (path, hash) in source_hashes {
        if hash != destination_hashes[&path] {
            eprintln!("{}", path.to_string_lossy());
        }
    }

    Ok(())
}

fn build_hashes(paths: Vec<PathBuf>) -> io::Result<HashMap<OsString, blake3::Hash>> {
    paths
        .into_par_iter()
        .map(|path| {
            fs::read(&path)
                .map(|content| (path.file_name().unwrap().to_owned(), blake3::hash(&content)))
        })
        .collect()
}
