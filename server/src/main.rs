#[macro_use]
extern crate log;

use std::{fs::File, io::BufReader};

use color_eyre::eyre::{self, eyre};
use futures_util::StreamExt as _;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use crate::{
    blaster::{Blaster, Config},
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

    let blaster = Blaster::new(config);

    while let Ok((stream, addr)) = listener.accept().await {
        if !blaster.introduce_session(&addr).await {
            error!("{}: too many handles", addr);
            // no clean shutdown for you pesky beggars!!!
            continue;
        }

        let max = 32 * 1024;
        let config = WebSocketConfig::default()
            .max_frame_size(Some(max))
            .max_message_size(Some(max));

        let blaster = blaster.clone();

        tokio::spawn(async move {
            info!("join: {}", addr);

            let (sender, receiver) =
                match tokio_tungstenite::accept_async_with_config(stream, Some(config)).await {
                    Ok(ws) => {
                        info!("hi {}", addr);
                        ws.split()
                    }
                    Err(e) => {
                        error!("{}: {}", addr, e);
                        blaster.close_session(&addr).await;
                        return;
                    }
                };

            let session = Session::new(blaster.clone(), addr, sender, receiver);
            session.mainloop().await;

            blaster.close_session(&addr).await;
        });
    }

    Ok(())
}
