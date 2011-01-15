use std::{
    io::{self, Write},
    net::TcpListener,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use yyds_gateway_redis::{connection::serve_connection, resp::RequestLimits, DEFAULT_PORT};

fn main() -> io::Result<()> {
    let mut arguments = std::env::args().skip(1);
    let port = match arguments.next().as_deref() {
        None => DEFAULT_PORT,
        Some("--port") => arguments
            .next()
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "expected --port <0..65535>"))?,
        Some(_) => return Err(io::Error::new(io::ErrorKind::InvalidInput, "usage: yyds-redis-server [--port <port>]")),
    };
    if arguments.next().is_some() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "unexpected argument"));
    }
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))?;
    eprintln!("YYDS RESP2 probe listening on {} (PING/ECHO/QUIT only)", listener.local_addr()?);
    let active = Arc::new(AtomicUsize::new(0));
    for connection in listener.incoming() {
        let mut stream = connection?;
        if active.fetch_add(1, Ordering::AcqRel) >= 64 {
            active.fetch_sub(1, Ordering::AcqRel);
            stream.set_write_timeout(Some(std::time::Duration::from_secs(1)))?;
            let _ = stream.write_all(b"-ERR maximum client count reached\r\n");
            continue;
        }
        let permit = ConnectionPermit(Arc::clone(&active));
        std::thread::Builder::new().spawn(move || {
            let _permit = permit;
            if let Err(error) = serve_connection(stream, RequestLimits::default()) {
                eprintln!("RESP2 connection closed: {error}");
            }
        })?;
    }
    Ok(())
}

struct ConnectionPermit(Arc<AtomicUsize>);

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
