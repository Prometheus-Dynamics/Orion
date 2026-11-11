//! A slow link, for a page watching a board from far away: what it accepts on one address it
//! carries to another, towards the one who connected at a set number of bytes a second, and
//! with small buffers, so the board feels the link as slow and not as deep.
//!
//! On the board, in front of anything that adds a deep buffer of its own (an SSH tunnel).

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::time::{Duration, Instant};

use super::Failed;

/// Bytes the socket towards the board holds.
const SHALLOW: usize = 8 * 1024;

/// Carries each connection to `listen` on to `target`, at `rate` bytes a second back.
pub(crate) fn throttle(listen: &str, target: &str, rate: usize) -> Result<(), Failed> {
    let listener = TcpListener::bind(listen)?;
    println!("carrying {listen} to {target} at {rate} bytes a second");
    for page in listener.incoming().flatten() {
        let Ok(board) = TcpStream::connect(target) else {
            println!("no {target}");
            continue;
        };
        let _ = rustix::net::sockopt::set_socket_recv_buffer_size(&board, SHALLOW);
        let _ = rustix::net::sockopt::set_socket_send_buffer_size(&page, SHALLOW);
        let (mut from_page, mut to_board) = (page.try_clone()?, board.try_clone()?);
        std::thread::spawn(move || {
            let _ = std::io::copy(&mut from_page, &mut to_board);
            let _ = to_board.shutdown(Shutdown::Both);
        });
        let (mut from_board, mut to_page) = (board, page);
        std::thread::spawn(move || {
            let mut bytes = [0_u8; 1024];
            let began = Instant::now();
            let mut carried = 0_usize;
            while let Ok(count @ 1..) = from_board.read(&mut bytes) {
                if to_page.write_all(&bytes[..count]).is_err() {
                    break;
                }
                carried += count;
                let due = Duration::from_secs_f64(carried as f64 / rate as f64);
                std::thread::sleep(due.saturating_sub(began.elapsed()));
            }
            let _ = to_page.shutdown(Shutdown::Both);
        });
    }
    Ok(())
}
