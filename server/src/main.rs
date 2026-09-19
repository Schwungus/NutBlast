#[macro_use]
extern crate log;

use std::{fs::File, io::BufReader, net::IpAddr, time::Duration};

use color_eyre::eyre::{self, eyre};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::{
    handshake::server::{Request, Response},
    http::StatusCode,
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

#[tokio::main]
async fn main() -> eyre::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let _ = color_eyre::install();

    let config = File::open("nutblaster.json").map_err(|x| eyre!("nutblaster.json: {x}"))?;
    let config: Config = serde_json::from_reader(BufReader::new(config))?;

    let addr = std::env::args().nth(1);
    let addr = addr.unwrap_or(String::from("127.0.0.1:36900"));

    let trust_xff = config.trust_reverse_proxy_xff.unwrap_or(false);

    if trust_xff {
        warn!("blindly trusting the X-Forwarded-For header from reverse-proxy!!!");
    } else {
        warn!("ignoring the X-Forwarded-For HTTP header");
        warn!(
            "if NutBlaster is behind a reverse-proxy such as nginx, it will see every client's IP as the proxy's LAN address!"
        );
    }

    let listener = TcpListener::bind(&addr).await?;
    info!("listening on: ws://{}", addr);

    let blaster = Blaster::new(config);
    let pruning_blaster = blaster.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;
            pruning_blaster.execute(BlasterOperation::Prune);
        }
    });

    while let Ok((stream, local_address)) = listener.accept().await {
        let blaster = blaster.clone();

        tokio::spawn(async move {
            let mut real_ip = local_address.ip();
            let mut session_handle = None;

            let hdr = |req: &Request, response: Response| {
                if trust_xff
                    && let Some(xff) = req.headers().get("x-forwarded-for")
                    && let Ok(xff) = xff.to_str()
                    && let Some(client_ip) = xff.split(',').next()
                    // X-Forwarded-For can be a comma-separated list: "client, proxy1, proxy2". The first entry is the original client IP.
                    && let Ok(real) = client_ip.trim().parse::<IpAddr>()
                {
                    real_ip = real;
                }

                session_handle = blaster.introduce_session(real_ip);

                if session_handle.is_none() {
                    error!("{}: too many handles", real_ip);

                    let err_response = Response::builder()
                        .status(StatusCode::TOO_MANY_REQUESTS)
                        .body(None)
                        .unwrap();

                    return Err(err_response);
                }

                Ok(response)
            };

            const MAX: Option<usize> = Some(32 * 1024);

            let config = WebSocketConfig::default()
                .max_frame_size(MAX)
                .max_message_size(MAX);

            let accept = tokio_tungstenite::accept_hdr_async_with_config(stream, hdr, Some(config));

            match accept.await {
                Ok(ws) => Session::new(blaster.clone(), real_ip, ws).serve().await,
                Err(e) => error!("{}: {}", real_ip, e),
            }
        });
    }

    Ok(())
}
