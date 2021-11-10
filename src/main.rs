use std::{
    collections::HashMap,
    ffi::OsString,
    fs::{self, File},
    io::{self, BufReader, BufWriter},
    path::{Path, PathBuf},
};

mod error;

use clap::Parser;
use error::Error;
use rayon::prelude::*;
use stopwatch::Stopwatch;

#[derive(Clone, Debug, Parser)]
struct Opts {
    #[clap(required = true)]
    from: Vec<String>,
    to: String,
    #[clap(short, long)]
    verbose: bool,
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

fn run(opts: &Opts) -> Result<(), error::Error> {
    let mut stopwatch = Stopwatch::start_new();

    let from: Vec<_> = opts.paths().collect();
    let to = Path::new(&opts.to);
    let to_dir = to.is_dir();

    if from.len() > 1 && !to_dir {
        return Err(Error::Data);
    }

    if opts.verbose {
        println!("init: {}", stopwatch);
    }

    // We enclose all of this work within a crossbeam scope for more efficient resource sharing.
    let (source_hashes, destination_paths) = crossbeam::scope(|cx| {
        // I'm doing this in a weird way in an effort to hash these files concurrent with
        // the copy operation.
        let hashes_task = cx.spawn(|_| {
            let result = build_hashes(&from);
            if opts.verbose {
                println!("finish hash: {}", stopwatch);
            }
            result
        });

        if opts.verbose {
            println!("begin hash/begin copy: {}", stopwatch);
        }

        let mut destination_paths = Vec::new();
        for from in from.iter() {
            // Note: fs::copy would seem to be a more convenient implementation of the following,
            // but it actually just introduces lossage around either network file systems or
            // symlinks (don't know which and don't care), so we're not doing that.

            if to_dir {
                let to = to.join(from.file_name().unwrap());
                copy(from, &to)?;
                destination_paths.push(to);
            } else {
                copy(from, to)?;
                destination_paths.push(to.into());
            }
        }

        if opts.verbose {
            println!("finish copy: {}", stopwatch);
        }

        let hashes = hashes_task.join().unwrap();
        hashes.map(|hashes| (hashes, destination_paths))
    })
    .map_err(|_| Error::Threading)??;

    if opts.verbose {
        println!("begin destination hash: {}", stopwatch);
    }

    let destination_hashes = build_hashes(&destination_paths)?;

    if opts.verbose {
        println!("finish destination hash: {}", stopwatch);
    }

    for (path, hash) in source_hashes {
        if hash != destination_hashes[&path] {
            eprintln!("{}", path.to_string_lossy());
        }
    }

    if opts.verbose {
        stopwatch.stop();
        println!("complete: {}", stopwatch);
    }

    Ok(())
}

fn build_hashes(paths: &[PathBuf]) -> io::Result<HashMap<OsString, blake3::Hash>> {
    paths
        .into_par_iter()
        .map(|path| {
            fs::read(&path)
                .map(|content| (path.file_name().unwrap().to_owned(), blake3::hash(&content)))
        })
        .collect()
}

fn copy(from: &Path, to: &Path) -> io::Result<()> {
    let mut from = File::open(&from).map(BufReader::new)?;
    let mut to = File::open(&to).map(BufWriter::new)?;
    io::copy(&mut from, &mut to)?;
    Ok(())
}
