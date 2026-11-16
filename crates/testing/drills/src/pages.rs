//! Which pages of a program's code it touched, for the code-used account: `orion-drill pages
//! mark <pid> <file>` marks every page of `<file>`'s executable mappings in `<pid>` idle, and
//! `orion-drill pages read <pid> <file>` prints, for each, its offset in the file and whether it
//! was touched since. Root only: it reads `/proc/<pid>/pagemap` for the frames and uses the
//! kernel's idle page tracking (`/sys/kernel/mm/page_idle/bitmap`).

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;

use crate::Failed;

const BITMAP: &str = "/sys/kernel/mm/page_idle/bitmap";
/// The frame number in a pagemap entry: bits 0 to 54.
const PFN_MASK: u64 = (1 << 55) - 1;
/// A pagemap entry's page is present.
const PRESENT: u64 = 1 << 63;

/// One page of code: where it is in the file, and its frame if it is in memory.
struct Page {
    offset: u64,
    frame: Option<u64>,
}

/// Runs `pages mark|read <pid> <file>`.
pub(crate) fn pages(args: &[String]) -> Result<(), Failed> {
    let (verb, pid, file) = match args {
        [_, verb, pid, file, ..] => (verb.as_str(), pid.as_str(), file.as_str()),
        _ => return Err("usage: pages mark|read <pid> <file>".into()),
    };
    let found = code_pages(pid, file)?;
    match verb {
        "mark" => {
            let bitmap = OpenOptions::new().write(true).open(BITMAP)?;
            for frame in found.iter().filter_map(|page| page.frame) {
                let bit = 1_u64 << (frame % 64);
                bitmap.write_at(&bit.to_ne_bytes(), frame / 64 * 8)?;
            }
            println!("marked {} pages", found.len());
            Ok(())
        }
        "read" => {
            let bitmap = File::open(BITMAP)?;
            let (mut touched, mut present) = (0, 0);
            for page in &found {
                let used = match page.frame {
                    Some(frame) => {
                        present += 1;
                        let mut word = [0_u8; 8];
                        bitmap.read_exact_at(&mut word, frame / 64 * 8)?;
                        u64::from_ne_bytes(word) & (1 << (frame % 64)) == 0
                    }
                    None => false,
                };
                touched += usize::from(used);
                println!("{:#x} {}", page.offset, u8::from(used));
            }
            println!("pages {} present {present} touched {touched}", found.len());
            Ok(())
        }
        _ => Err("usage: pages mark|read <pid> <file>".into()),
    }
}

/// Every page of `file`'s executable mappings in `pid`, with its frame.
fn code_pages(pid: &str, file: &str) -> Result<Vec<Page>, Failed> {
    let size = page_size()?;
    let maps = std::fs::read_to_string(format!("/proc/{pid}/maps"))?;
    let pagemap = File::open(format!("/proc/{pid}/pagemap"))?;
    let mut pages = Vec::new();
    for line in maps.lines() {
        // start-end perms offset dev inode path
        let fields: Vec<&str> = line.split_whitespace().collect();
        let [range, perms, offset, _, _, path, ..] = fields.as_slice() else {
            continue;
        };
        if !perms.contains('x') || !path.ends_with(file) {
            continue;
        }
        let (start, end) = range.split_once('-').ok_or("a maps line")?;
        let (start, end) = (
            u64::from_str_radix(start, 16)?,
            u64::from_str_radix(end, 16)?,
        );
        let offset = u64::from_str_radix(offset, 16)?;
        let mut at = start;
        while at < end {
            let mut entry = [0_u8; 8];
            pagemap.read_exact_at(&mut entry, at / size * 8)?;
            let entry = u64::from_ne_bytes(entry);
            let frame = (entry & PRESENT != 0 && entry & PFN_MASK != 0).then_some(entry & PFN_MASK);
            pages.push(Page {
                offset: offset + (at - start),
                frame,
            });
            at += size;
        }
    }
    if pages.is_empty() {
        return Err(format!("no executable mapping of {file} in {pid}").into());
    }
    Ok(pages)
}

/// The page size, from what the kernel says of this process.
fn page_size() -> Result<u64, Failed> {
    let status = std::fs::read_to_string("/proc/self/smaps")?;
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix("KernelPageSize:"))
        .and_then(|rest| rest.split_whitespace().next())
        .ok_or("no page size")?;
    Ok(kib.parse::<u64>()? * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn this_programs_own_code_pages_are_found_with_their_offsets() {
        let exe = std::env::current_exe().expect("this program");
        let name = exe
            .file_name()
            .and_then(|name| name.to_str())
            .expect("a name");
        let pid = std::process::id().to_string();
        let found = code_pages(&pid, name).expect("its code");
        let size = page_size().expect("page size");
        assert!(found.len() > 1);
        // Offsets go up a page at a time within a mapping, and every one is page aligned.
        assert!(found.iter().all(|page| page.offset % size == 0));
        assert!(
            found
                .windows(2)
                .any(|two| two[1].offset == two[0].offset + size)
        );
        // The first page is at the offset in the file the kernel says its mapping starts.
        let maps = std::fs::read_to_string("/proc/self/maps").expect("maps");
        let first = maps
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>())
            .filter(|fields| fields.len() >= 6 && fields[1].contains('x'))
            .filter(|fields| fields[5].ends_with(name))
            .map(|fields| u64::from_str_radix(fields[2], 16).expect("an offset"))
            .min()
            .expect("an executable mapping");
        assert_eq!(found.iter().map(|page| page.offset).min(), Some(first));
        assert!(code_pages(&pid, "no-such-file").is_err());
    }
}
