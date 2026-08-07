#[macro_use]
extern crate log;

use std::{fs::File, io::BufReader};

use color_eyre::eyre::{self, eyre};
use futures_util::StreamExt as _;
use tokio::net::TcpListener;

use crate::{
    blaster::{Blaster, Config},
    connection::Connection,
};

mod blaster;
mod connection;
mod id;
mod protocol;

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

    let blaster = Blaster::new(config);

    while let Ok((stream, addr)) = listener.accept().await {
        let blaster = blaster.clone();

        tokio::spawn(async move {
            info!("conn: {}", addr);

            let (sender, receiver) = match tokio_tungstenite::accept_async(stream).await {
                Ok(ws) => {
                    info!("hi {}", addr);
                    ws.split()
                }
                Err(e) => {
                    error!("{}: {}", addr, e);
                    return;
                }
            };

            let conn = Connection::new(blaster, addr, sender, receiver);
            conn.mainloop().await;
        });
    }

    Ok(())
}
