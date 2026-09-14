#[macro_use]
extern crate log;

use std::{fs::File, io::BufReader, net::IpAddr, time::Duration};

use color_eyre::eyre::{self, eyre};
use futures_util::StreamExt as _;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::{
    handshake::server::{Request, Response},
    protocol::WebSocketConfig,
};

use crate::{
    blaster::{Blaster, BlasterOperation, Config},
    session::Session,
};

mod blaster;
mod id;
mod protocol;
mod session;
mod tokens;

pub const MAX_PLAYERS: usize = 16;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let _ = color_eyre::install();

    let config: Config = serde_json::from_reader(BufReader::new(
        File::open("nutblaster.json").map_err(|x| eyre!("nutblaster.json: {x}"))?,
    ))?;

    let addr = if let Some(addr) = std::env::args().nth(1) {
        addr
    } else {
        String::from("127.0.0.1:36900")
    };

    let listener = TcpListener::bind(&addr).await?;

    info!("listening on: ws://{}", addr);

    let blaster0 = Blaster::new(config);
    let blaster = blaster0.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;
            blaster.execute(BlasterOperation::PruneStaleSessions);
        }
    });

    let blaster = blaster0.clone();

    while let Ok((stream, local_address)) = listener.accept().await {
        let mut real_ip = local_address.ip();

        let max = 32 * 1024;
        let config = WebSocketConfig::default()
            .max_frame_size(Some(max))
            .max_message_size(Some(max));

        let blaster = blaster.clone();

        tokio::spawn(async move {
            let hdr = |req: &Request, response: Response| {
                if let Some(xff) = req.headers().get("x-forwarded-for")
                    && let Ok(xff_str) = xff.to_str()
                    && let Some(client_ip) = xff_str.split(',').next()
                    // X-Forwarded-For can be a comma-separated list: "client, proxy1, proxy2". The first entry is the original client IP.
                    && let Ok(real) = client_ip.trim().parse::<IpAddr>()
                {
                    real_ip = real;
                }

                Ok(response)
            };

            let accept = tokio_tungstenite::accept_hdr_async_with_config(stream, hdr, Some(config));

            let (sender, receiver) = match accept.await {
                Ok(ws) => {
                    info!("hi {}", real_ip);
                    ws.split()
                }
                Err(e) => {
                    error!("{}: {}", real_ip, e);
                    blaster.close_session(real_ip).await;
                    return;
                }
            };

            if !blaster.introduce_session(real_ip).await {
                error!("{}: too many handles", real_ip);
                return; // no clean shutdown for you pesky beggars!!!
            }

            let session = Session::new(blaster.clone(), real_ip, sender, receiver);
            session.mainloop().await;
            blaster.close_session(real_ip).await;
        });
    }

    Ok(())
}
